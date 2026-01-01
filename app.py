# -*- coding: utf-8 -*-
"""
タスク管理ボード（完全版 / 複数人運用向け / タイムゾーン安全化）

機能:
- CSV 永続化 + GitHub 連携（SHA による楽観的ロック / 成否でUI分岐 / committer情報）
- 起票日は自動・編集不可、更新日は編集/クローズ時に自動更新（JST）
- 簡易ログイン（Secrets の USERS によるトークン方式）
- 監査ログ（audit.csv）: 作成 / 更新 / 削除 / 一括削除 / クローズ を記録
- フィルタ、一覧、クローズ候補抽出（対応中 + 返信待ち系 + 7日前より前の更新）
- 手動リフレッシュボタン（最新反映）

注意:
- Secrets の SAVE_WITH_TIME は文字列でも正しく解釈されます（true/false/1/0/yes/no/on/off）。
- GITHUB_* の設定が必要です。監査ログを GitHub に保存する場合は GITHUB_PATH_AUDIT も設定します。
"""

import streamlit as st
import pandas as pd
from datetime import datetime, date
import uuid
from zoneinfo import ZoneInfo

# --- GitHub API 用 ---
import base64
import requests

# ===== 安全なブールパーサー =====
def get_bool_secret(key: str, default: bool = True) -> bool:
    v = st.secrets.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "on"}
    return bool(v)

# ===== 監査ログパス =====
AUDIT_PATH = st.secrets.get("AUDIT_PATH", "audit.csv")

# ===== タイムゾーン・ヘルパー =====
JST = ZoneInfo("Asia/Tokyo")
SAVE_WITH_TIME = get_bool_secret("SAVE_WITH_TIME", True)  # True: YYYY-MM-DD HH:MM:SS / False: YYYY-MM-DD


def now_jst() -> datetime:
    return datetime.now(JST)


def now_jst_str() -> str:
    fmt = "%Y-%m-%d %H:%M:%S" if SAVE_WITH_TIME else "%Y-%m-%d"
    return now_jst().strftime(fmt)


def today_jst() -> date:
    return now_jst().date()


# ===== ページ設定 =====
st.set_page_config(page_title="タスク管理ボード（完全版）", layout="wide")
st.title("タスク管理ボード（完全版 / 起票日は自動・編集不可、更新者はプルダウン）")

CSV_PATH = st.secrets.get("CSV_PATH", "tasks.csv")
LOCK_PATH = st.secrets.get("LOCK_PATH", "locks.csv")  # 予約（将来用）
LOCK_TTL_MIN = int(st.secrets.get("LOCK_TTL_MIN", 10))
MANDATORY_COLS = [
    "ID", "起票日", "更新日", "タスク", "対応状況", "更新者", "次アクション", "備考", "ソース",
]

# ===== ユーティリティ =====
MISSING_SET = {"", "none", "null", "nan", "na", "n/a", "-", "—"}


def _ensure_str(x) -> str:
    return "" if x is None else str(x)


def _is_missing(x) -> bool:
    s = _ensure_str(x).strip().lower()
    return s in MISSING_SET


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # 列名の単純正規化（全角スペース→半角、前後空白除去）
    df.columns = [c.replace("\u3000", " ").strip() for c in df.columns]
    # よくある別名の統一
    rename_map = {
        "更新": "更新日", "最終更新": "更新日", "起票": "起票日", "作成日": "起票日",
        "担当": "更新者", "担当者": "更新者"
    }
    df.columns = [rename_map.get(c, c) for c in df.columns]

    # 必須列の追加
    for col in MANDATORY_COLS:
        if col not in df.columns:
            df[col] = ""

    # ID 正規化（空/重複を必ず解消）
    df["ID"] = df["ID"].astype(str).replace({"nan": "", "None": ""})
    mask_empty = df["ID"].str.strip().eq("")
    if mask_empty.any():
        df.loc[mask_empty, "ID"] = [str(uuid.uuid4()) for _ in range(mask_empty.sum())]
    dup_mask = df["ID"].duplicated(keep="first")
    if dup_mask.any():
        df.loc[dup_mask, "ID"] = [str(uuid.uuid4()) for _ in range(dup_mask.sum())]

    # 文字列列の正規化（None/null/nanなどを空へ）
    str_cols = ["タスク", "対応状況", "更新者", "次アクション", "備考", "ソース"]
    for col in str_cols:
        df[col] = df[col].apply(lambda x: "" if _is_missing(x) else _ensure_str(x))

    # 日付列（NaTを許容）
    for col in ["起票日", "更新日"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    return df.reset_index(drop=True)


@st.cache_data(ttl=10)
def load_tasks() -> pd.DataFrame:
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    except FileNotFoundError:
        df = pd.DataFrame(columns=MANDATORY_COLS)
    df = _normalize_df(df)
    # 読み込み直後に安全弁（欠損日付は“いま”で補完）
    df = safety_autofill_all(df)
    return df


def format_ts(dt) -> str:
    """CSV 保存時の日付フォーマット統一。NaT は“いま”で補完。"""
    if pd.isna(dt):
        dt = pd.Timestamp(now_jst())
    else:
        dt = pd.to_datetime(dt, errors="coerce")
        if pd.isna(dt):
            dt = pd.Timestamp(now_jst())
    return dt.strftime("%Y-%m-%d %H:%M:%S" if SAVE_WITH_TIME else "%Y-%m-%d")


def save_tasks(df: pd.DataFrame):
    """保存前に安全弁をかけ、CSVへ書き出し"""
    df_out = safety_autofill_all(df.copy())
    for col in ["起票日", "更新日"]:
        df_out[col] = pd.to_datetime(df_out[col], errors="coerce").apply(format_ts)
    df_out.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")


# ===== 日付の安全弁（全行） =====

def safety_autofill_all(df: pd.DataFrame) -> pd.DataFrame:
    now_ts = pd.Timestamp(now_jst())
    # 起票日は欠損のみ補完（既存起票日は維持）
    df["起票日"] = df["起票日"].apply(lambda x: now_ts if pd.isna(pd.to_datetime(x, errors="coerce")) else pd.to_datetime(x, errors="coerce"))
    # 更新日は欠損なら補完（編集/クローズ時は別途上書き）
    df["更新日"] = df["更新日"].apply(lambda x: now_ts if pd.isna(pd.to_datetime(x, errors="coerce")) else pd.to_datetime(x, errors="coerce"))
    return df


# ===== GitHubへコミット保存（SHA 楽観的ロック / 成否返却） =====

def save_to_github_file(local_path: str, remote_path: str, commit_message: str, debug: bool = False) -> bool:
    required_keys = ["GITHUB_TOKEN", "GITHUB_OWNER", "GITHUB_REPO"]
    missing = [k for k in required_keys if k not in st.secrets]
    branch = st.secrets.get("GITHUB_BRANCH", "main")
    if missing:
        st.error(f"Secrets が不足しています: {missing}（Manage app → Settings → Secrets を確認）")
        return False

    token = st.secrets["GITHUB_TOKEN"]
    owner = st.secrets["GITHUB_OWNER"]
    repo = st.secrets["GITHUB_REPO"]
    path = remote_path

    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "streamlit-app",
    }
    try:
        # 最新 sha を取得
        r = requests.get(url, headers=headers, params={"ref": branch}, timeout=20)
        if debug:
            st.write({"GET_status": r.status_code, "GET_text": r.text[:300]})
        latest_sha = r.json().get("sha") if r.status_code == 200 else None

        with open(local_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode("utf-8")

        ts = now_jst().strftime("%Y-%m-%d %H:%M:%S %Z")
        payload = {
            "message": f"{commit_message} ({ts})",
            "content": content_b64,
            "branch": branch,
            "committer": {"name": "Streamlit App", "email": "noreply@example.com"},
        }
        if latest_sha:
            payload["sha"] = latest_sha

        put = requests.put(url, headers=headers, json=payload, timeout=20)
        if debug:
            st.write({"PUT_status": put.status_code, "PUT_text": put.text[:500]})

        if put.status_code in (200, 201):
            st.toast("GitHubへ保存完了", icon="✅")
            return True
        elif put.status_code == 422:
            st.warning("他の更新と競合しました。最新を読み直してから再保存してください。")
            return False
        elif put.status_code == 401:
            st.error("401 Unauthorized: トークン無効。新しいPATをSecretsへ。")
        elif put.status_code == 403:
            st.error("403 Forbidden: 権限不足/保護ルール。PAT権限『Contents: Read and write』やブランチ保護を確認。")
        elif put.status_code == 404:
            st.error("404 Not Found: OWNER/REPO/PATH/BRANCH を再確認。")
        elif put.status_code == 429:
            st.error("429 Too Many Requests: レート制限。しばらく待って再試行してください。")
        else:
            st.error(f"GitHub保存失敗: {put.status_code} {put.text[:300]}")
        return False
    except Exception as e:
        st.error(f"GitHub保存中に例外: {e}")
        return False


def save_to_github_csv(local_path: str = CSV_PATH, debug: bool = False) -> bool:
    remote = st.secrets.get("GITHUB_PATH")
    if not remote:
        st.error("Secrets に GITHUB_PATH がありません。")
        return False
    return save_to_github_file(local_path, remote, "Update tasks.csv from Streamlit app", debug=debug)


# 監査ログを GitHub にも保存（任意設定）
def save_audit_to_github(debug: bool = False) -> bool:
    remote_audit = st.secrets.get("GITHUB_PATH_AUDIT")
    if not remote_audit:
        return True  # 設定がなければ成功扱い
    return save_to_github_file(AUDIT_PATH, remote_audit, "Update audit.csv from Streamlit app", debug=debug)


# ===== 監査ログ =====
def write_audit(action: str, task_id: str, before: dict, after: dict):
    rec = {
        "ts": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
        "user": st.session_state.get("current_user", "unknown"),
        "action": action,              # "create" | "update" | "delete" | "delete_bulk" | "close"
        "task_id": task_id,
        "before": str(before) if before else "",
        "after": str(after) if after else "",
    }
    try:
        df_a = pd.read_csv(AUDIT_PATH, encoding="utf-8-sig")
    except FileNotFoundError:
        df_a = pd.DataFrame(columns=rec.keys())
    df_a = pd.concat([df_a, pd.DataFrame([rec])], ignore_index=True)
    df_a.to_csv(AUDIT_PATH, index=False, encoding="utf-8-sig")
    save_audit_to_github(debug=False)


# ===== データ読み込み =====
df = load_tasks()
df_by_id = df.set_index("ID")

# ===== 簡易ログイン（トークン方式） =====
st.sidebar.header("ログイン")
USERS = st.secrets.get("USERS", {})  # 例: {"都筑":"tokenA","二上":"tokenB"}
if USERS:
    token_input = st.sidebar.text_input("ログイントークン", type="password")
    user_sel = st.sidebar.selectbox("ユーザー", list(USERS.keys()))
    if st.sidebar.button("ログイン"):
        if USERS.get(user_sel) == token_input:
            st.session_state["current_user"] = user_sel
            st.sidebar.success(f"{user_sel} としてログインしました")
        else:
            st.sidebar.error("トークンが不正です")
else:
    st.session_state.setdefault("current_user", "anonymous")

# ===== 手動リフレッシュ =====

def _do_refresh():
    st.cache_data.clear()
    st.rerun()

st.sidebar.button("最新を読み込む", on_click=_do_refresh)

# ===== サイドバー・フィルター =====
st.sidebar.header("フィルター")
status_options = ["すべて"] + sorted(df["対応状況"].dropna().unique().tolist())
status_sel = st.sidebar.selectbox("対応状況", status_options)
assignees = sorted([a for a in df["更新者"].dropna().unique().tolist() if a.strip() != ""])
assignee_sel = st.sidebar.multiselect("担当者", assignees)
kw = st.sidebar.text_input("キーワード（タスク/備考/次アクション）")

view_df = df.copy()
if status_sel != "すべて":
    view_df = view_df[view_df["対応状況"] == status_sel]
if assignee_sel:
    view_df = view_df[view_df["更新者"].isin(assignee_sel)]
if kw:
    mask = (
        view_df["タスク"].str.contains(kw, na=False, regex=False)
        | view_df["備考"].str.contains(kw, na=False, regex=False)
        | view_df["次アクション"].str.contains(kw, na=False, regex=False)
    )
    view_df = view_df[mask]

# ===== サマリー =====
total = len(df)
status_counts = df["対応状況"].value_counts()

reply_mask = pd.Series(False, index=df.index)
for k in ["返信待ち", "返信無し", "返信なし", "返信ない", "催促"]:
    reply_mask = (
        reply_mask
        | df["次アクション"].str.contains(k, na=False)
        | df["備考"].str.contains(k, na=False)
    )
reply_count = int(df[reply_mask].shape[0])

col1, col2, col3, col4 = st.columns(4)
col1.metric("総タスク数", total)
col2.metric("対応中", int(status_counts.get("対応中", 0)))
col3.metric("クローズ", int(status_counts.get("クローズ", 0)))
col4.metric("返信待ち系", reply_count)

# ===== 一覧 =====
st.subheader("一覧")


def _fmt_display(dt: pd.Timestamp) -> str:
    if pd.isna(dt):
        return "-"
    # tz付きでも安全に表示（JST想定なら tz情報は捨ててOK）
    try:
        ts = pd.Timestamp(dt)
        if getattr(ts, "tzinfo", None) is not None:
            ts = ts.tz_localize(None)
        dt = ts
    except Exception:
        pass
    return dt.strftime("%Y-%m-%d %H:%M:%S" if SAVE_WITH_TIME else "%Y-%m-%d")


disp = view_df.copy()
disp["起票日"] = disp["起票日"].apply(_fmt_display)
disp["更新日"] = disp["更新日"].apply(_fmt_display)
st.dataframe(disp.sort_values("更新日", ascending=False), use_container_width=True)

# ===== クローズ候補 =====
st.subheader("クローズ候補（ルール: 対応中かつ返信待ち系、更新が7日以上前）")
# tz-aware を tz-naive に統一してから 7日前閾値を計算
now_ts = pd.Timestamp(now_jst()).tz_localize(None)
threshold_dt = now_ts - pd.Timedelta(days=7)

in_progress = df[df["対応状況"].eq("対応中")]
reply_df = df[reply_mask]
closing_candidates = in_progress[in_progress.index.isin(reply_df.index)]

closing_candidates = closing_candidates.copy()
closing_candidates["更新日"] = pd.to_datetime(closing_candidates["更新日"], errors="coerce")
# 混在対策: tz付きだけ tz を外す
try:
    if getattr(closing_candidates["更新日"].dt, "tz", None) is not None:
        closing_candidates["更新日"] = closing_candidates["更新日"].dt.tz_localize(None)
except Exception:
    pass

closing_candidates = closing_candidates[
    closing_candidates["更新日"].notna() & (closing_candidates["更新日"] < threshold_dt)
]

if closing_candidates.empty:
    st.info("該当なし")
else:
    show = closing_candidates.copy()
    show["起票日"] = show["起票日"].apply(_fmt_display)
    show["更新日"] = show["更新日"].apply(_fmt_display)
    st.dataframe(show.sort_values("更新日"), use_container_width=True)

    to_close_ids = st.multiselect(
        "クローズするタスク（複数選択可）",
        closing_candidates["ID"].tolist(),
        format_func=lambda _id: f'{df_by_id.loc[_id,"タスク"]} / {df_by_id.loc[_id,"更新者"]} / {_fmt_display(df_by_id.loc[_id,"更新日"])}'
    )
    if st.button("選択したタスクをクローズに更新", type="primary", disabled=(len(to_close_ids) == 0)):
        befores = {tid: df_by_id.loc[tid, ["対応状況", "更新日"]].to_dict() for tid in to_close_ids}
        df.loc[df["ID"].isin(to_close_ids), "対応状況"] = "クローズ"
        df.loc[df["ID"].isin(to_close_ids), "更新日"] = pd.Timestamp(now_jst())
        save_tasks(df)
        ok = save_to_github_csv(debug=False)
        if ok:
            for tid in to_close_ids:
                after = {"対応状況": "クローズ", "更新日": _fmt_display(pd.Timestamp(now_jst()))}
                write_audit("close", tid, befores.get(tid), after)
            st.success(f"{len(to_close_ids)}件をクローズに更新しました。")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("GitHub保存に失敗しました。最新を読み直して再試行してください。")

# ===== 新規追加 =====
st.subheader("新規タスク追加（起票日/更新日は自動でJSTの“いま”）")
with st.form("add"):
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"起票日: **{now_jst_str()}**")
    c2.markdown(f"更新日: **{now_jst_str()}**")
    status = c3.selectbox("対応状況", ["未対応", "対応中", "クローズ"], index=1)

    task = st.text_input("タスク（件名）")
    fixed_assignees = st.secrets.get("FIXED_OWNERS", ["都筑", "二上", "三平", "成瀬", "柿野", "花田", "武藤", "島浦"])  # 任意固定
    ass_choices = sorted(set([a for a in df["更新者"].tolist() if a.strip() != ""] + list(fixed_assignees)))
    assignee = st.selectbox("更新者（担当）", options=ass_choices)

    next_action = st.text_area("次アクション")
    notes = st.text_area("備考")
    source = st.text_input("ソース（ID/リンクなど）")

    submitted = st.form_submit_button("追加", type="primary")
    if submitted:
        now_ts2 = pd.Timestamp(now_jst())
        new_row = {
            "ID": str(uuid.uuid4()),
            "起票日": now_ts2,
            "更新日": now_ts2,
            "タスク": task,
            "対応状況": status,
            "更新者": assignee,
            "次アクション": next_action,
            "備考": notes,
            "ソース": source,
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        save_tasks(df)
        ok = save_to_github_csv(debug=False)
        if ok:
            write_audit("create", new_row["ID"], None, {
                k: (new_row[k] if k not in ["起票日", "更新日"] else _fmt_display(new_row[k]))
                for k in new_row.keys()
            })
            st.success("追加しました（起票・更新はJSTの“いま”）。")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("GitHub保存に失敗しました。競合の可能性があります。")

# ===== 編集・削除 =====
st.subheader("タスク編集・削除（1件を選んで安全に更新／削除）")

if len(df) == 0:
    st.info("編集対象のタスクがありません。まずは追加してください。")
else:
    choice_id = st.selectbox(
        "編集対象",
        options=df_by_id.index.tolist(),
        format_func=lambda _id: f'[{df_by_id.loc[_id,"対応状況"]}] {df_by_id.loc[_id,"タスク"]} / {df_by_id.loc[_id,"更新者"]} / {_fmt_display(df_by_id.loc[_id,"更新日"])}',
        key="selected_id",
    )

    if choice_id not in df_by_id.index:
        st.warning("選択したIDが見つかりません。再読み込みします。")
        st.cache_data.clear()
        st.rerun()

    with st.form(f"edit_task_{choice_id}"):
        c1, c2, c3 = st.columns(3)
        task_e = c1.text_input("タスク（件名）", df_by_id.loc[choice_id, "タスク"], key=f"task_{choice_id}")
        status_e = c2.selectbox(
            "対応状況", ["未対応", "対応中", "クローズ"],
            index=( ["未対応","対応中","クローズ"].index(df_by_id.loc[choice_id,"対応状況"]) if df_by_id.loc[choice_id,"対応状況"] in ["未対応","対応中","クローズ"] else 1 ),
            key=f"status_{choice_id}"
        )

        fixed_assignees_e = st.secrets.get("FIXED_OWNERS", ["都筑", "二上", "三平", "成瀬", "柿野", "花田", "武藤", "島浦"])  # 任意固定
        ass_choices_e = sorted(set([a for a in df["更新者"].tolist() if a.strip() != ""] + list(fixed_assignees_e)))
        default_assignee = df_by_id.loc[choice_id, "更新者"]
        ass_index = ass_choices_e.index(default_assignee) if default_assignee in ass_choices_e else 0
        assignee_e = c3.selectbox("更新者（担当）", options=ass_choices_e, index=ass_index, key=f"assignee_{choice_id}")

        next_action_e = st.text_area("次アクション", df_by_id.loc[choice_id, "次アクション"], key=f"next_{choice_id}")
        notes_e = st.text_area("備考", df_by_id.loc[choice_id, "備考"], key=f"notes_{choice_id}")
        source_e = st.text_input("ソース（ID/リンクなど）", df_by_id.loc[choice_id, "ソース"], key=f"source_{choice_id}")

        st.caption(
            f"起票日: {_fmt_display(df_by_id.loc[choice_id, '起票日'])} / 最終更新: {_fmt_display(df_by_id.loc[choice_id, '更新日'])}"
        )

        col_ok, col_spacer, col_del = st.columns([1, 1, 1])
        submit_edit = col_ok.form_submit_button("更新する", type="primary")

        st.markdown("##### 削除（危険）")
        st.warning("この操作は元に戻せません。削除する場合、確認ワードに `DELETE` と入力してください。")
        confirm_word = st.text_input("確認ワード（DELETE と入力）", value="", key=f"confirm_{choice_id}")
        delete_btn = col_del.form_submit_button("このタスクを削除", type="secondary")

    if submit_edit:
        before = df_by_id.loc[choice_id, ["タスク","対応状況","更新者","次アクション","備考","ソース"]].to_dict()
        df.loc[df["ID"] == choice_id, ["タスク","対応状況","更新者","次アクション","備考","ソース"]] = [
            task_e, status_e, assignee_e, next_action_e, notes_e, source_e
        ]
        df.loc[df["ID"] == choice_id, "更新日"] = pd.Timestamp(now_jst())
        save_tasks(df)
        ok = save_to_github_csv(debug=False)
        if ok:
            write_audit("update", choice_id, before, {
                "タスク": task_e, "対応状況": status_e, "更新者": assignee_e,
                "次アクション": next_action_e, "備考": notes_e, "ソース": source_e
            })
            st.success("タスクを更新しました（更新日はJSTの“いま”）。")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("GitHub保存に失敗しました。競合の可能性があります。最新を読み直して再試行してください。")

    elif delete_btn:
        if confirm_word.strip().upper() == "DELETE":
            before = df_by_id.loc[choice_id, ["タスク","対応状況","更新者","次アクション","備考","ソース"]].to_dict()
            df = df[~df["ID"].eq(choice_id)].copy()
            save_tasks(df)
            ok = save_to_github_csv(debug=False)
            st.session_state.pop("selected_id", None)
            if ok:
                write_audit("delete", choice_id, before, None)
                st.success("タスクを削除しました。")
                st.cache_data.clear()
                st.rerun()
            else:
                st.error("GitHub保存に失敗しました。競合の可能性があります。")
        else:
            st.error("確認ワードが正しくありません。`DELETE` と入力してください。")

# ===== 一括削除 =====
st.subheader("一括削除（複数選択）")
del_targets = st.multiselect(
    "削除したいタスク（複数選択）",
    options=view_df["ID"].tolist(),
    format_func=lambda _id: f'{df_by_id.loc[_id,"タスク"]} / {df_by_id.loc[_id,"更新者"]} / {_fmt_display(df_by_id.loc[_id,"更新日"])}'
)
confirm_word_bulk = st.text_input("確認ワード（DELETE と入力）", value="", key="confirm_bulk")
if st.button("選択タスクを削除", disabled=(len(del_targets) == 0)):
    if confirm_word_bulk.strip().upper() == "DELETE":
        before_map = {tid: df_by_id.loc[tid, ["タスク","対応状況","更新者","次アクション","備考","ソース"]].to_dict() for tid in del_targets}
        df = df[~df["ID"].isin(del_targets)].copy()
        save_tasks(df)
        ok = save_to_github_csv(debug=False)
        if ok:
            for tid in del_targets:
                write_audit("delete_bulk", tid, before_map.get(tid), None)
            st.success(f"{len(del_targets)}件のタスクを削除しました。")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("GitHub保存に失敗しました。競合の可能性があります。")
    else:
        st.error("確認ワードが正しくありません。`DELETE` と入力してください。")

# ===== サイドバー：手動保存＆診断 =====
colA, colB = st.sidebar.columns(2)
if colA.button("GitHubへ手動保存"):
    ok = save_to_github_csv(debug=False)
    if ok:
        st.sidebar.success("GitHubへ保存完了")
    else:
        st.sidebar.error("GitHub保存失敗")
if colB.button("GitHub保存の診断"):
    save_to_github_csv(debug=True)

st.sidebar.caption(f"Secrets keys: {list(st.secrets.keys())}")

# ===== フッター =====
st.caption("※ 起票日は新規作成時のみ自動セットし、以後は編集不可（既存値維持）。更新日は編集/クローズ操作でJSTの“いま”に自動更新。GitHub連携はGET→PUTで保存します。")
