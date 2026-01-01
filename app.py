# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from datetime import datetime, date
import uuid
from zoneinfo import ZoneInfo

# --- GitHub API 用 ---
import base64
import requests

# --- AgGrid（行クリックで選択） ---
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
    AGGRID_AVAILABLE = True
except Exception as e:
    AGGRID_AVAILABLE = False

# ===== タイムゾーン・ヘルパー =====
JST = ZoneInfo("Asia/Tokyo")

SAVE_WITH_TIME = bool(st.secrets.get("SAVE_WITH_TIME", True))  # True: YYYY-MM-DD HH:MM:SS / False: YYYY-MM-DD

def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    fmt = "%Y-%m-%d %H:%M:%S" if SAVE_WITH_TIME else "%Y-%m-%d"
    return now_jst().strftime(fmt)

def today_jst() -> date:
    return now_jst().date()

# ===== ページ設定 =====
st.set_page_config(page_title="タスク管理ボード（AgGrid版 完全）", layout="wide")
st.title("タスク管理ボード — AgGrid版（行クリックで編集）\n起票日は自動・編集不可／更新者はプルダウン／欠損補完／GitHub保存")

CSV_PATH = st.secrets.get("CSV_PATH", "tasks.csv")
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

    # 文字列列の正規化
    str_cols = ["タスク", "対応状況", "更新者", "次アクション", "備考", "ソース"]
    for col in str_cols:
        df[col] = df[col].apply(lambda x: "" if _is_missing(x) else _ensure_str(x))

    # 日付列（NaTを許容）
    for col in ["起票日", "更新日"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    return df.reset_index(drop=True)

@st.cache_data(ttl=30)
def load_tasks() -> pd.DataFrame:
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    except FileNotFoundError:
        df = pd.DataFrame(columns=MANDATORY_COLS)
    df = _normalize_df(df)
    # 読み込み直後に安全弁（欠損日付は“いま”で補完）
    df = safety_autofill_all(df)
    return df

def _format_date_for_save(dt: pd.Timestamp) -> str:
    if pd.isna(dt):
        return now_jst_str()  # 欠損は“いま”
    if SAVE_WITH_TIME:
        return pd.to_datetime(dt).strftime("%Y-%m-%d %H:%M:%S")
    else:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")


def save_tasks(df: pd.DataFrame):
    """保存前に安全弁をかけ、CSVへ書き出し"""
    df_out = safety_autofill_all(df.copy())
    for col in ["起票日", "更新日"]:
        df_out[col] = df_out[col].apply(lambda x: _format_date_for_save(pd.to_datetime(x, errors="coerce")))
    df_out.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

# ===== 日付の安全弁（全行） =====

def safety_autofill_all(df: pd.DataFrame) -> pd.DataFrame:
    now_ts = pd.Timestamp(now_jst())
    # 起票日は欠損のみ補完（既存起票日は維持）
    df["起票日"] = df["起票日"].apply(lambda x: now_ts if pd.isna(pd.to_datetime(x, errors="coerce")) else pd.to_datetime(x, errors="coerce"))
    # 更新日は欠損なら補完（編集/クローズ時は別途上書き）
    df["更新日"] = df["更新日"].apply(lambda x: now_ts if pd.isna(pd.to_datetime(x, errors="coerce")) else pd.to_datetime(x, errors="coerce"))
    return df

# ===== GitHubへコミット保存（診断付き） =====

def save_to_github_csv(local_path: str = CSV_PATH, debug: bool = False):
    required_keys = ["GITHUB_TOKEN", "GITHUB_OWNER", "GITHUB_REPO", "GITHUB_PATH"]
    missing = [k for k in required_keys if k not in st.secrets]
    branch = st.secrets.get("GITHUB_BRANCH", "main")
    if missing:
        st.error(f"Secrets が不足しています: {missing}（Manage app → Settings → Secrets を確認）")
        return

    token = st.secrets["GITHUB_TOKEN"]
    owner = st.secrets["GITHUB_OWNER"]
    repo  = st.secrets["GITHUB_REPO"]
    path  = st.secrets["GITHUB_PATH"]

    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "streamlit-app",
    }

    try:
        r = requests.get(url, headers=headers, params={"ref": branch}, timeout=20)
        if debug:
            st.write({"GET_status": r.status_code, "GET_text": r.text[:300]})
        sha = r.json().get("sha") if r.status_code == 200 else None

        with open(local_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode("utf-8")

        ts = now_jst().strftime("%Y-%m-%d %H:%M:%S %Z")
        payload = {
            "message": f"Update tasks.csv from Streamlit app ({ts})",
            "content": content_b64,
            "branch": branch,
        }
        if sha:
            payload["sha"] = sha

        put = requests.put(url, headers=headers, json=payload, timeout=20)
        if debug:
            st.write({"PUT_status": put.status_code, "PUT_text": put.text[:500]})

        if put.status_code in (200, 201):
            st.toast("GitHubへ保存完了", icon="✅")
        elif put.status_code == 401:
            st.error("401 Unauthorized: トークン無効。新しいPATをSecretsへ。")
        elif put.status_code == 403:
            st.error("403 Forbidden: 権限不足/保護ルール。PAT権限『Contents: Read and write』やブランチ保護を確認。")
        elif put.status_code == 404:
            st.error("404 Not Found: OWNER/REPO/PATH/BRANCH を再確認。")
        elif put.status_code == 422:
            st.error("422 Unprocessable: SHA不正 or ブランチ保護。最新を取得して再保存してください。")
        else:
            st.error(f"GitHub保存失敗: {put.status_code} {put.text[:300]}")
    except Exception as e:
        st.error(f"GitHub保存中に例外: {e}")

# ===== データ読み込み =====
df = load_tasks()
df_by_id = df.set_index("ID")

# ===== サイドバー・フィルター =====
st.sidebar.header("フィルター")
status_options = ["すべて"] + sorted(df["対応状況"].dropna().unique().tolist())
status_sel = st.sidebar.selectbox("対応状況", status_options)
assignees = sorted(df["更新者"].dropna().unique().tolist())
assignee_sel = st.sidebar.multiselect("担当者", assignees)
kw = st.sidebar.text_input("キーワード（タスク/備考/次アクション）")

view_df = df.copy()
if status_sel != "すべて":
    view_df = view_df[view_df["対応状況"] == status_sel]
if assignee_sel:
    view_df = view_df[view_df["更新者"].isin(assignee_sel)]
if kw:
    mask = (
        view_df["タスク"].str.contains(kw, na=False)
        | view_df["備考"].str.contains(kw, na=False)
        | view_df["次アクション"].str.contains(kw, na=False)
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

# ===== 一覧（AgGrid：行クリックで選択→編集へ反映） =====
st.subheader("一覧（行クリックで選択）")

def _fmt_display(dt: pd.Timestamp) -> str:
    if pd.isna(dt):
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M:%S" if SAVE_WITH_TIME else "%Y-%m-%d")

# 表示用のコピー（日時は文字列に）
disp = view_df.copy()
disp["起票日"] = disp["起票日"].apply(_fmt_display)
disp["更新日"] = disp["更新日"].apply(_fmt_display)

selected_id = None
if AGGRID_AVAILABLE:
    gb = GridOptionsBuilder.from_dataframe(disp)
    gb.configure_selection(selection_mode="single", use_checkbox=False)
    gb.configure_grid_options(enableSorting=True, enableFilter=True, rowSelection="single")
    # 列幅やテキスト折り返しなどの好み設定（任意）
    gb.configure_default_column(flex=1, wrapText=True, autoHeight=True)
    grid_options = gb.build()

    grid = AgGrid(
        disp.sort_values("更新日", ascending=False),
        gridOptions=grid_options,
        height=520,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True,
        theme="balham",
    )
    rows = grid.get("selected_rows", [])
    selected_id = rows[0]["ID"] if rows else None
else:
    st.warning("AgGrid が未導入です。ターミナルで `pip install streamlit-aggrid` を実行してください。\n一時的に標準の表を表示します。")
    st.dataframe(disp.sort_values("更新日", ascending=False), use_container_width=True)

# ===== クローズ候補（.dtエラー対策版） =====
st.subheader("クローズ候補（ルール: 対応中かつ返信待ち系、更新が7日以上前）")
threshold_date = today_jst() - pd.Timedelta(days=7)
threshold_dt = pd.Timestamp(threshold_date)

in_progress = df[df["対応状況"].str.contains("対応中", na=False)]
reply_df = df[reply_mask]
closing_candidates = in_progress[in_progress.index.isin(reply_df.index)]
closing_candidates = closing_candidates.copy()
closing_candidates["更新日"] = pd.to_datetime(closing_candidates["更新日"], errors="coerce")
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
        df.loc[df["ID"].isin(to_close_ids), "対応状況"] = "クローズ"
        df.loc[df["ID"].isin(to_close_ids), "更新日"] = pd.Timestamp(now_jst())
        save_tasks(df)
        save_to_github_csv(debug=False)
        st.success(f"{len(to_close_ids)}件をクローズに更新しました。")
        st.cache_data.clear()
        st.rerun()

# ===== 新規追加 =====
st.subheader("新規タスク追加（起票日/更新日は自動でJSTの“いま”）")
with st.form("add"):
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"起票日: **{now_jst_str()}**")
    c2.markdown(f"更新日: **{now_jst_str()}**")
    status = c3.selectbox("対応状況", ["未対応", "対応中", "クローズ"], index=1)

    task = st.text_input("タスク（件名）")
    fixed_assignees = st.secrets.get("FIXED_OWNERS", ["都筑", "二上", "三平", "成瀬", "柿野", "花田", "武藤", "島浦"])  # 任意固定
    ass_choices = sorted(set(df["更新者"].tolist() + list(fixed_assignees)))
    assignee = st.selectbox("更新者（担当）", options=ass_choices)

    next_action = st.text_area("次アクション")
    notes = st.text_area("備考")
    source = st.text_input("ソース（ID/リンクなど）")

    submitted = st.form_submit_button("追加", type="primary")
    if submitted:
        now_ts = pd.Timestamp(now_jst())
        new_row = {
            "ID": str(uuid.uuid4()),
            "起票日": now_ts,
            "更新日": now_ts,
            "タスク": task,
            "対応状況": status,
            "更新者": assignee,
            "次アクション": next_action,
            "備考": notes,
            "ソース": source,
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        save_tasks(df)
        save_to_github_csv(debug=False)
        st.success("追加しました（起票・更新はJSTの“いま”）。")
        st.cache_data.clear()
        st.rerun()

# ===== 編集・削除（行クリックの選択をフォームに反映） =====
st.subheader("タスク編集・削除（行クリックで選んだ1件を更新／削除）")

if len(df) == 0:
    st.info("編集対象のタスクがありません。まずは追加してください。")
else:
    ids = df_by_id.index.tolist()

    # 選択優先順位：AgGridの選択 -> URLクエリ id -> セッション -> 先頭
    qid = None
    try:
        q = getattr(st, "query_params", None) or st.experimental_get_query_params()
        if q and "id" in q:
            v = q["id"]
            qid = v if isinstance(v, str) else (v[0] if isinstance(v, list) and v else None)
    except Exception:
        qid = None

    sel = selected_id or qid or st.session_state.get("selected_id")
    initial_index = ids.index(sel) if (sel in ids) else 0

    choice_id = st.selectbox(
        "編集対象",
        options=ids,
        index=initial_index,
        format_func=lambda _id: f'[{df_by_id.loc[_id,"対応状況"]}] {df_by_id.loc[_id,"タスク"]} / {df_by_id.loc[_id,"更新者"]} / {_fmt_display(df_by_id.loc[_id,"更新日"])}',
        key="selected_id",
    )

    # URLとセッションに維持（共有や再訪がしやすい）
    try:
        if hasattr(st, "query_params"):
            st.query_params["id"] = choice_id
        else:
            st.experimental_set_query_params(id=choice_id)
    except Exception:
        pass
    st.session_state["selected_id"] = choice_id

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
        ass_choices_e = sorted(set(df["更新者"].tolist() + list(fixed_assignees_e)))
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
        df.loc[df["ID"] == choice_id, ["タスク","対応状況","更新者","次アクション","備考","ソース"]] = [
            task_e, status_e, assignee_e, next_action_e, notes_e, source_e
        ]
        df.loc[df["ID"] == choice_id, "更新日"] = pd.Timestamp(now_jst())  # “いま”
        save_tasks(df)
        save_to_github_csv(debug=False)
        st.success("タスクを更新しました（更新日はJSTの“いま”）。")
        st.cache_data.clear()
        st.rerun()

    elif delete_btn:
        if confirm_word.strip().upper() == "DELETE":
            df = df[~df["ID"].eq(choice_id)].copy()
            save_tasks(df)
            save_to_github_csv(debug=False)
            st.session_state.pop("selected_id", None)
            st.success("タスクを削除しました。")
            st.cache_data.clear()
            st.rerun()
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
        df = df[~df["ID"].isin(del_targets)].copy()
        save_tasks(df)
        save_to_github_csv(debug=False)
        st.success(f"{len(del_targets)}件のタスクを削除しました。")
        st.cache_data.clear()
        st.rerun()
    else:
        st.error("確認ワードが正しくありません。`DELETE` と入力してください。")

# ===== サイドバー：手動保存＆診断 =====
colA, colB = st.sidebar.columns(2)
if colA.button("GitHubへ手動保存"):
    save_to_github_csv(debug=False)
if colB.button("GitHub保存の診断"):
    save_to_github_csv(debug=True)

st.sidebar.caption(f"Secrets keys: {list(st.secrets.keys())}")

# ===== フッター =====
st.caption("※ AgGridの行クリックで編集対象を選択できます。起票日は新規作成時のみ自動セット、以後は編集不可（既存値維持）。更新日は編集/クローズ操作でJSTの“いま”に自動更新。GitHub連携はGET→PUTで保存します。")
