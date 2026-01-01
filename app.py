
# app.py（完全版：
#   直保存方式／日付の自動起票／サイドバー・フィルター常時表示／診断エクスパンダ／コミット名入り）
# --------------------------------------------------------------------------------------
# 変更操作（追加／編集／削除／クローズ）後に
#   1) ローカル保存 → 2) GitHubコミット → 3) キャッシュクリア → 4) rerun
# をその場で実行し、更新漏れを防止。
# 「更新する」を押すと更新日は“いま（JST）”を自動起票。
# 既存行の起票日が欠損なら編集/クローズ時に“いま（JST）”を自動起票。
# サイドバーは initial_sidebar_state="expanded" で常時展開。
# 診断はメイン側エクスパンダ（必要時のみ）。
# コミットメッセージに [user:◯◯] を付与（サイドバー入力 or Secrets）。
# --------------------------------------------------------------------------------------

import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
import base64
import requests

# ===== ページ設定（最初に呼ぶ：重要） =====
st.set_page_config(
    page_title="タスク管理ボード（自動保存）",
    layout="wide",
    initial_sidebar_state="expanded"  # サイドバーを常時展開
)
st.title("タスク管理ボード（自動保存）")

# ===== タイムゾーン（JST） =====
JST = ZoneInfo("Asia/Tokyo")
def now_jst() -> datetime:
    return datetime.now(JST)

def today_jst():
    return now_jst().date()

# JST の「いま」を Pandas Timestamp で返す（起票/更新日に使用）
def jst_now_ts() -> pd.Timestamp:
    return pd.Timestamp(now_jst())

# 欠損（None/""/"None"/"NaT"/NaN）を一括判定
def is_missing_date(v) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    if s in ("", "None", "NaT"):
        return True
    try:
        return pd.isna(v)
    except Exception:
        return False

# ===== CSV設定 =====
CSV_PATH = "tasks.csv"
MANDATORY_COLS = [
    "ID", "起票日", "更新日", "タスク", "対応状況", "更新者", "次アクション", "備考", "ソース",
]

# ===== ユーティリティ =====
def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # 必須列が無ければ追加
    for col in MANDATORY_COLS:
        if col not in df.columns:
            df[col] = ""

    # ID 正規化（空/重複を必ず解消）
    df["ID"] = df["ID"].astype(str).replace({"nan": ""})
    mask_empty = df["ID"].str.strip().eq("")
    if mask_empty.any():
        df.loc[mask_empty, "ID"] = [str(uuid.uuid4()) for _ in range(mask_empty.sum())]
    dup_mask = df["ID"].duplicated(keep="first")
    if dup_mask.any():
        df.loc[dup_mask, "ID"] = [str(uuid.uuid4()) for _ in range(dup_mask.sum())]

    # 日付型（NaT許容）
    for col in ["起票日", "更新日"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # 文字列列
    for col in ["タスク", "対応状況", "更新者", "次アクション", "備考", "ソース"]:
        df[col] = df[col].astype(str)

    return df.reset_index(drop=True)

@st.cache_data(ttl=30)
def load_tasks() -> pd.DataFrame:
    """CSV 読み込み（存在しなければ空表）"""
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    except FileNotFoundError:
        df = pd.DataFrame(columns=MANDATORY_COLS)
    return _normalize_df(df)

def save_tasks_locally(df: pd.DataFrame):
    """CSV へ保存（通常は日付のみ。時刻も残したい場合はフォーマット変更）"""
    df_out = df.copy()
    for col in ["起票日", "更新日"]:
        # 日付のみ：下の1行（デフォルト）
        df_out[col] = pd.to_datetime(df_out[col], errors="coerce").dt.strftime("%Y-%m-%d")
        # 時刻まで保存したい場合は上をコメントアウトして、こちらに切替：
        # df_out[col] = pd.to_datetime(df_out[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    df_out.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

# ===== サイドバー：コミットメッセージに入れる名前 =====
st.sidebar.subheader("コミット設定")
commit_user_in = st.sidebar.text_input(
    "コミットに入れる名前（例：都筑）",
    value=st.session_state.get("commit_user", st.secrets.get("COMMIT_USER", "")),
)
st.session_state["commit_user"] = commit_user_in.strip()

def get_commit_user() -> str:
    """優先順：サイドバー入力 > Secrets(COMMIT_USER) > 空文字"""
    return (st.session_state.get("commit_user") or st.secrets.get("COMMIT_USER", "")).strip()

# ===== GitHubへコミット保存（診断＋競合検知付き、名前入り） =====
def save_to_github_csv(local_path: str = CSV_PATH, debug: bool = False, commit_user: str = "", action: str = "Update") -> int:
    """
    ローカルCSVを GitHub の指定パスへコミット保存。
    - 直前 GET の sha をPUT payloadに含めて競合検知（他ユーザー更新 → 422）
    - コミットメッセージ先頭に [user:◯◯] を付与（commit_user）
    - action は "Add"/"Edit"/"Delete"/"Close"/"Diagnose" など
    - 成功: 200/201 を返す。失敗はステータスコード（-1 はSecrets不足）
    """
    # Secrets チェック
    required_keys = ["GITHUB_TOKEN", "GITHUB_OWNER", "GITHUB_REPO", "GITHUB_PATH"]
    missing = [k for k in required_keys if k not in st.secrets]
    branch = st.secrets.get("GITHUB_BRANCH", "main")
    if missing:
        st.error(f"Secrets が不足しています: {missing}（Manage app → Settings → Secrets を確認）")
        return -1

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
        # 既存ファイルの SHA 取得（更新時に必要）
        r = requests.get(url, headers=headers, params={"ref": branch}, timeout=20)
        if debug:
            st.write({"GET_status": r.status_code, "GET_text": r.text[:300]})
        sha = r.json().get("sha") if r.status_code == 200 else None

        # CSV を base64 化
        with open(local_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode("utf-8")
        ts = now_jst().strftime("%Y-%m-%d %H:%M:%S %Z")

        # メッセージ（名前＋操作種類）
        prefix = f"[user:{commit_user}] " if commit_user else ""
        message = f"{prefix}{action} tasks.csv from Streamlit app ({ts})"

        payload = {
            "message": message,
            "content": content_b64,
            "branch": branch,
            # author/committer を指定したい場合は有効化（環境により無視されることあり）：
            # "author":   {"name": commit_user or "TaskBoard", "email": "noreply@example.com"},
            # "committer":{"name": commit_user or "TaskBoard", "email": "noreply@example.com"},
        }
        if sha:
            payload["sha"] = sha  # 既存更新時は必須（競合検知にも使う）

        put = requests.put(url, headers=headers, json=payload, timeout=20)
        if debug:
            st.write({"PUT_status": put.status_code, "PUT_text": put.text[:500]})

        # ステータス別の分岐（見える化）
        if put.status_code in (200, 201):
            pass  # 成功
        elif put.status_code == 401:
            st.error("401 Unauthorized: トークンが無効（期限切れ／Revoke）。→ 新しいPATをSecretsへ。")
        elif put.status_code == 403:
            st.error("403 Forbidden: 権限不足 / 組織側のPAT承認未完了 / ブランチ保護で拒否。"
                     "→ PAT権限『Contents: Read and write』、Org承認、保存用ブランチの利用を確認。")
        elif put.status_code == 404:
            st.error("404 Not Found: OWNER/REPO/PATH/BRANCH の不一致。→ Secretsの値と実URL/パスを再確認。")
        elif put.status_code == 422:
            st.error("422 Unprocessable: 他のユーザーが先に更新（sha不一致など）。最新データを読み込んで再操作してください。")
        else:
            st.error(f"GitHub保存失敗: {put.status_code} {put.text[:300]}")

        return put.status_code

    except Exception as e:
        st.error(f"GitHub保存中に例外: {e}")
        return -1

# ===== 保存＆確認を 1ヶ所に統一（直保存ヘルパー） =====
def save_then_confirm_commit(df: pd.DataFrame, *, show_toast: bool = True, action: str = "Update") -> bool:
    """
    1) ローカルCSVへ保存
    2) GitHubへコミット（コミット名・操作種類付き）
    3) 成功(200/201)ならキャッシュクリア
    """
    try:
        save_tasks_locally(df)  # ローカルへ

        commit_user = get_commit_user()
        status = save_to_github_csv(debug=False, commit_user=commit_user, action=action)  # GitHubへ

        if status in (200, 201):
            if show_toast:
                st.toast("GitHubへ保存完了", icon="✅")
            st.cache_data.clear()
            return True
        else:
            st.error("GitHubへの保存に失敗しました。診断エクスパンダで GET/PUT の内容をご確認ください。")
            return False
    except Exception as e:
        st.error(f"保存処理中に例外: {e}")
        return False

# ===== データ読み込み =====
df = load_tasks()
df_by_id = df.set_index("ID")

# ===== サイドバー・フィルター（常時表示） =====
st.sidebar.header("フィルター")

status_options = ["すべて"] + sorted(df["対応状況"].dropna().unique().tolist())
status_sel = st.sidebar.selectbox("対応状況", status_options)

assignees = sorted(df["更新者"].dropna().unique().tolist())
assignee_sel = st.sidebar.multiselect("担当者", assignees)

kw = st.sidebar.text_input("キーワード（更新日/タスク/備考/次アクション）")

# ===== フィルター適用（view_df を作る） =====
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
        | view_df["更新日"].astype(str).str.contains(kw, na=False)
    )
    view_df = view_df[mask]

# ===== サマリー =====
st.subheader("サマリー")
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

# ===== 一覧（view_df を表示） =====
st.subheader("一覧")
st.dataframe(view_df.sort_values("更新日", ascending=False), use_container_width=True)

# ===== クローズ候補 =====
st.subheader("クローズ候補（ルール: 対応中かつ返信待ち系、更新が7日以上前）")
threshold_date = today_jst() - pd.Timedelta(days=7)
in_progress = df[df["対応状況"].str.contains("対応中", na=False)]
reply_df = df[reply_mask]
closing_candidates = in_progress[in_progress.index.isin(reply_df.index)]
closing_candidates = closing_candidates[
    closing_candidates["更新日"].notna()
    & (closing_candidates["更新日"].dt.date < threshold_date)
]
if closing_candidates.empty:
    st.info("該当なし")
else:
    st.dataframe(closing_candidates.sort_values("更新日"), use_container_width=True)
    to_close_ids = st.multiselect(
        "クローズするタスク（複数選択可）",
        closing_candidates["ID"].tolist(),
        format_func=lambda _id: f'{df_by_id.loc[_id,"タスク"]} / {df_by_id.loc[_id,"更新者"]} / '
                                f'{df_by_id.loc[_id,"更新日"].strftime("%Y-%m-%d") if pd.notnull(df_by_id.loc[_id,"更新日"]) else "-"}'
    )
    if st.button("選択したタスクをクローズに更新", type="primary", disabled=(len(to_close_ids) == 0)):
        # 起票日が空の行は「いま」で埋める
        mask_missing_created = df["ID"].isin(to_close_ids) & df["起票日"].apply(is_missing_date)
        df.loc[mask_missing_created, "起票日"] = jst_now_ts()

        df.loc[df["ID"].isin(to_close_ids), "対応状況"] = "クローズ"
        df.loc[df["ID"].isin(to_close_ids), "更新日"] = jst_now_ts()

        ok = save_then_confirm_commit(df, action=f"Close {len(to_close_ids)} items")
        if ok:
            st.success(f"{len(to_close_ids)}件をクローズに更新しました。（起票/更新 自動／GitHubへ保存完了）")
        st.rerun()

# ===== 新規追加（直保存方式） =====
st.subheader("新規タスク追加（保存は自動）")
with st.form("add"):
    c1, c2, c3 = st.columns(3)
    created = c1.date_input("起票日", today_jst())  # ユーザー選択。未選択なら自動で「いま」
    status_sel_add = c3.selectbox("対応状況", ["未対応", "対応中", "クローズ"], index=1)
    task = st.text_input("タスク（件名）")
    ass_choices = sorted(set(df["更新者"].tolist() + ["都筑", "二上", "三平", "成瀬", "柿野", "花田", "武藤", "島浦"]))
    assignee = st.selectbox("更新者（担当）", options=ass_choices)
    next_action = st.text_area("次アクション")
    notes = st.text_area("備考")
    source = st.text_input("ソース（ID/リンクなど）")
    submitted = st.form_submit_button("追加", type="primary")
    if submitted:
        # 起票日はフォームの値／欠損なら「いま」
        created_ts = pd.Timestamp(created) if not is_missing_date(created) else jst_now_ts()

        new_row = {
            "ID": str(uuid.uuid4()),
            "起票日": created_ts,
            "更新日": jst_now_ts(),  # 追加時は「いま」を自動起票
            "タスク": task,
            "対応状況": status_sel_add,
            "更新者": assignee,
            "次アクション": next_action,
            "備考": notes,
            "ソース": source,
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        short_title = (task or "").strip()[:60]
        ok = save_then_confirm_commit(df, action=f"Add: {short_title}")
        if ok:
            st.success("追加しました。（起票/更新 自動／GitHubへ保存完了）")
        st.rerun()

# ===== 編集・削除（直保存方式） =====
st.subheader("タスク編集・削除（1件を選んで安全に更新／削除）")
if len(df) == 0:
    st.info("編集対象のタスクがありません。まずは追加してください。")
else:
    choice_id = st.selectbox(
        "編集対象",
        options=df_by_id.index.tolist(),
        format_func=lambda _id: f'[{df_by_id.loc[_id,"対応状況"]}] {df_by_id.loc[_id,"タスク"]} / {df_by_id.loc[_id,"更新者"]} / '
                                f'{df_by_id.loc[_id,"更新日"].strftime("%Y-%m-%d") if pd.notnull(df_by_id.loc[_id,"更新日"]) else "-"}',
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
            index=(["未対応","対応中","クローズ"].index(df_by_id.loc[choice_id,"対応状況"])
                   if df_by_id.loc[choice_id,"対応状況"] in ["未対応","対応中","クローズ"] else 1),
            key=f"status_{choice_id}"
        )
        ass_choices_e = sorted(set(df["更新者"].tolist() + ["都筑", "二上", "三平", "成瀬", "柿野", "花田", "武藤", "島浦"]))
        default_assignee = df_by_id.loc[choice_id, "更新者"]
        ass_index = ass_choices_e.index(default_assignee) if default_assignee in ass_choices_e else 0
        assignee_e = c3.selectbox("更新者（担当）", options=ass_choices_e, index=ass_index, key=f"assignee_{choice_id}")
        next_action_e = st.text_area("次アクション", df_by_id.loc[choice_id, "次アクション"], key=f"next_{choice_id}")
        notes_e = st.text_area("備考", df_by_id.loc[choice_id, "備考"], key=f"notes_{choice_id}")
        source_e = st.text_input("ソース（ID/リンクなど）", df_by_id.loc[choice_id, "ソース"], key=f"source_{choice_id}")

        st.caption(
            f"※『更新する』を押すと、更新日は自動で現在（JST）に設定されます。起票日が空の場合も自動で現在日時が入ります。"
        )

        col_ok, col_spacer, col_del = st.columns([1, 1, 1])
        submit_edit = col_ok.form_submit_button("更新する", type="primary")

        st.markdown("##### 削除（危険）")
        st.warning("この操作は元に戻せません。削除する場合、確認ワードに `DELETE` と入力してください。")
        confirm_word = st.text_input("確認ワード（DELETE と入力）", value="", key=f"confirm_{choice_id}")
        delete_btn = col_del.form_submit_button("このタスクを削除", type="secondary")

    if submit_edit:
        # 入力内容を反映
        df.loc[df["ID"] == choice_id, ["タスク","対応状況","更新者","次アクション","備考","ソース"]] = [
            task_e, status_e, assignee_e, next_action_e, notes_e, source_e
        ]

        # 起票日が空なら「いま（JST）」で自動起票
        current_created = df_by_id.loc[choice_id, "起票日"] if choice_id in df_by_id.index else None
        if is_missing_date(current_created):
            df.loc[df["ID"] == choice_id, "起票日"] = jst_now_ts()

        # 更新日は「いま（JST）」へ自動起票
        df.loc[df["ID"] == choice_id, "更新日"] = jst_now_ts()

        short_title = str(task_e).strip()[:60]
        ok = save_then_confirm_commit(df, action=f"Edit: {short_title}")
        if ok:
            st.success("タスクを更新しました。（起票/更新 自動／GitHubへ保存完了）")
        st.rerun()

    elif delete_btn:
        if confirm_word.strip().upper() == "DELETE":
            df = df[~df["ID"].eq(choice_id)].copy()
            short_title = str(df_by_id.loc[choice_id, "タスク"]).strip()[:60] if choice_id in df_by_id.index else ""
            ok = save_then_confirm_commit(df, action=f"Delete: {short_title}")
            if ok:
                st.success("タスクを削除しました。（GitHubへ保存完了）")
            st.session_state.pop("selected_id", None)
            st.rerun()
        else:
            st.error("確認ワードが正しくありません。`DELETE` と入力してください。")

# ===== 一括削除（直保存方式） =====
st.subheader("一括削除（複数選択）")
del_targets = st.multiselect(
    "削除したいタスク（複数選択）",
    options=view_df["ID"].tolist(),
    format_func=lambda _id: f'{df_by_id.loc[_id,"タスク"]} / {df_by_id.loc[_id,"更新者"]} / '
                            f'{df_by_id.loc[_id,"更新日"].strftime("%Y-%m-%d") if pd.notnull(df_by_id.loc[_id,"更新日"]) else "-"}')
confirm_word_bulk = st.text_input("確認ワード（DELETE と入力）", value="", key="confirm_bulk")
if st.button("選択タスクを削除", disabled=(len(del_targets) == 0)):
    if confirm_word_bulk.strip().upper() == "DELETE":
        df = df[~df["ID"].isin(del_targets)].copy()
        ok = save_then_confirm_commit(df, action=f"Delete {len(del_targets)} items")
        if ok:
            st.success(f"{len(del_targets)}件のタスクを削除しました。（GitHubへ保存完了）")
        st.rerun()
    else:
        st.error("確認ワードが正しくありません。`DELETE` と入力してください。")

# ===== 診断（必要時のみ開く：メインエリアに配置） =====
with st.expander("GitHub保存の診断（必要なときだけ開く）", expanded=False):
    st.write("GitHub API へのアクセス状況（GET/PUT）と短い応答本文を表示します。問題の切り分けに使います。")

    c1, c2 = st.columns(2)
    run_getput = c1.button("診断を実行（GET/PUT）")
    manual_backup = c2.button("手動バックアップ（最新CSVをコミット）")

    if run_getput:
        _ = save_to_github_csv(
            debug=True,
            commit_user=get_commit_user(),
            action="Diagnose"
        )

    if manual_backup:
        ok = save_then_confirm_commit(df, show_toast=False, action="Manual backup")
        if ok:
            st.success("バックアップ完了（GitHubへ保存しました）。")

    st.caption(f"Secrets keys（値は表示しません）: {list(st.secrets.keys())}")

# ===== フッター =====
st.caption("※ この試作はローカルCSV保存です。複数人での同時編集には SharePoint/Dataverse/Database を推奨。GitHub連携でCSVの永続化が可能です。")
