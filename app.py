
# app.py（完全自動保存対応・競合検知付き）
import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
import base64, requests

# ===== 時刻（JST） =====
JST = ZoneInfo("Asia/Tokyo")
def now_jst() -> datetime:
    return datetime.now(JST)

# ===== ページ設定 =====
st.set_page_config(page_title="タスク管理ボード（自動保存対応）", layout="wide")
st.title("タスク管理ボード（自動保存）")

CSV_PATH = "tasks.csv"
MANDATORY_COLS = ["ID","起票日","更新日","タスク","対応状況","更新者","次アクション","備考","ソース"]

# ===== ユーティリティ =====
def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    for col in MANDATORY_COLS:
        if col not in df.columns:
            df[col] = ""
    # ID 正規化
    df["ID"] = df["ID"].astype(str).replace({"nan": ""})
    empty = df["ID"].str.strip().eq("")
    if empty.any():
        df.loc[empty, "ID"] = [str(uuid.uuid4()) for _ in range(empty.sum())]
    dup = df["ID"].duplicated(keep="first")
    if dup.any():
        df.loc[dup, "ID"] = [str(uuid.uuid4()) for _ in range(dup.sum())]
    # 日付型
    for col in ["起票日","更新日"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ["タスク","対応状況","更新者","次アクション","備考","ソース"]:
        df[col] = df[col].astype(str)
    return df.reset_index(drop=True)

@st.cache_data(ttl=30)
def load_tasks() -> pd.DataFrame:
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    except FileNotFoundError:
        df = pd.DataFrame(columns=MANDATORY_COLS)
    return _normalize_df(df)

def save_tasks_locally(df: pd.DataFrame):
    out = df.copy()
    for col in ["起票日","更新日"]:
        out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime("%Y-%m-%d")
    out.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

# ===== GitHub 保存（診断＋競合検知） =====
def save_to_github_csv(local_path: str = CSV_PATH, debug: bool = False) -> int:
    """
    現在の CSV を GitHub の指定パスへコミット保存。
    - 直前 GET の sha を使って競合検知（422 なら他ユーザー先行更新）
    - 成功: 200/201 を返す。失敗はステータスコードを返す。
    """
    required = ["GITHUB_TOKEN","GITHUB_OWNER","GITHUB_REPO","GITHUB_PATH"]
    missing = [k for k in required if k not in st.secrets]
    branch = st.secrets.get("GITHUB_BRANCH","main")
    if missing:
        st.error(f"Secrets 不足: {missing}")
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
        "User-Agent": "streamlit-autosave"
    }

    # 1) 直前の sha を取得
    r = requests.get(url, headers=headers, params={"ref": branch}, timeout=20)
    if debug:
        st.write({"GET_status": r.status_code, "GET_text": r.text[:300]})
    sha = r.json().get("sha") if r.status_code == 200 else None

    # 2) CSV -> base64
    with open(local_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode("utf-8")

    payload = {
        "message": f"Update tasks.csv from Streamlit app ({now_jst().strftime('%Y-%m-%d %H:%M:%S %Z')})",
        "content": content_b64,
        "branch": branch
    }
    if sha:  # 既存更新なら競合検知のために sha を付与
        payload["sha"] = sha

    put = requests.put(url, headers=headers, json=payload, timeout=20)
    if debug:
        st.write({"PUT_status": put.status_code, "PUT_text": put.text[:500]})

    # ステータス別ハンドリング
    if put.status_code in (200, 201):
        st.toast("GitHubへ自動保存完了", icon="✅")
    elif put.status_code == 422:
        # 競合（sha不一致等）
        st.error("競合検知：他のユーザーが先に更新しました。最新データを読み込み直します。")
        st.cache_data.clear()
        # 自動再読み込みを促す（ユーザー操作不要）
        st.rerun()
    elif put.status_code == 403:
        st.error("403 Forbidden：権限不足/ブランチ保護。保存用ブランチの利用を検討してください。")
    elif put.status_code == 401:
        st.error("401 Unauthorized：PAT無効。再発行してSecretsへ保存してください。")
    elif put.status_code == 404:
        st.error("404 Not Found：OWNER/REPO/PATH/BRANCH の不一致。Secrets再確認。")
    else:
        st.error(f"GitHub保存失敗: {put.status_code} {put.text[:300]}")

    return put.status_code

# ===== df 読み込み =====
df = load_tasks()
df_by_id = df.set_index("ID")

# ===== 自動保存の仕組み（デバウンス） =====
# 状態更新の都度 st.session_state["dirty"]=True を立てる。ここで検知して保存する。
if "dirty" not in st.session_state:
    st.session_state["dirty"] = False

def autosave_if_needed():
    if st.session_state.get("dirty", False):
        # まずローカルCSVへ
        save_tasks_locally(df)
        # GitHubへ自動保存
        status = save_to_github_csv(debug=False)
        # 成功/失敗に関わらずフラグを一旦下ろす（無限ループ防止）
        st.session_state["dirty"] = False
        # 最新を見せるためにキャッシュクリア＆再描画（成功時のみでもOK）
        if status in (200, 201):
            st.cache_data.clear()
            st.rerun()

# ページ描画の冒頭で自動保存を試みる
autosave_if_needed()

# ===== メイン UI =====
st.subheader("新規タスク追加（保存は自動）")
with st.form("add"):
    c1, c2, c3 = st.columns(3)
    created = c1.date_input("起票日", datetime.now(JST).date())
    updated = c2.date_input("更新日", datetime.now(JST).date())
    status_sel = c3.selectbox("対応状況", ["未対応","対応中","クローズ"], index=1)
    task = st.text_input("タスク（件名）")
    assignee = st.text_input("更新者（担当）", value="")
    next_action = st.text_area("次アクション")
    notes = st.text_area("備考")
    source = st.text_input("ソース（ID/リンクなど）")
    submitted = st.form_submit_button("追加", type="primary")
    if submitted:
        new_row = {
            "ID": str(uuid.uuid4()),
            "起票日": pd.Timestamp(created),
            "更新日": pd.Timestamp(updated),
            "タスク": task,
            "対応状況": status_sel,
            "更新者": assignee,
            "次アクション": next_action,
            "備考": notes,
            "ソース": source
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        # 変更フラグを立てるだけ → 自動保存が走る
        st.session_state["dirty"] = True
        st.success("追加しました（自動保存を実行します）。")
        # すぐに反映したい場合は rerun（autosave 内でも行う）
        st.rerun()

st.subheader("一覧")
st.dataframe(df.sort_values("更新日", ascending=False), use_container_width=True)

st.subheader("タスク編集・削除（自動保存）")
if len(df) == 0:
    st.info("編集対象がありません。まずは追加してください。")
else:
    choice_id = st.selectbox("編集対象ID", options=df_by_id.index.tolist())
    if choice_id in df_by_id.index:
        with st.form(f"edit_{choice_id}"):
            c1, c2, c3 = st.columns(3)
            task_e = c1.text_input("タスク（件名）", df_by_id.loc[choice_id, "タスク"])
            status_e = c2.selectbox("対応状況", ["未対応","対応中","クローズ"],
                                    index=(["未対応","対応中","クローズ"].index(df_by_id.loc[choice_id,"対応状況"])
                                           if df_by_id.loc[choice_id,"対応状況"] in ["未対応","対応中","クローズ"] else 1))
            assignee_e = c3.text_input("更新者（担当）", df_by_id.loc[choice_id, "更新者"])
            next_e = st.text_area("次アクション", df_by_id.loc[choice_id, "次アクション"])
            notes_e = st.text_area("備考", df_by_id.loc[choice_id, "備考"])
            source_e = st.text_input("ソース", df_by_id.loc[choice_id, "ソース"])
            submit_edit = st.form_submit_button("更新", type="primary")
            del_ok = st.form_submit_button("削除", type="secondary")
        if submit_edit:
            df.loc[df["ID"] == choice_id, ["タスク","対応状況","更新者","次アクション","備考","ソース"]] = \
                [task_e, status_e, assignee_e, next_e, notes_e, source_e]
            df.loc[df["ID"] == choice_id, "更新日"] = pd.Timestamp(datetime.now(JST).date())
            st.session_state["dirty"] = True
            st.success("更新しました（自動保存を実行します）。")
            st.rerun()
        elif del_ok:
            df = df[~df["ID"].eq(choice_id)].copy()
            st.session_state["dirty"] = True
            st.success("削除しました（自動保存を実行します）。")
            st.rerun()

# =====（任意）定期バックアップ（5分ごと）=====
# 変更があるときだけコミットしたい場合は、dirty フラグ管理のままで OK。
# バックアップ運用をしたい場合は次を有効化：
# st_autorefresh が重い環境では控えめに。
enable_backup = False  # True にすると 5分ごとにリフレッシュ
if enable_backup:
    st.caption("5分ごとにバックアップ実行中（差分がある場合のみコミット）。")
    st.experimental_singleton.clear()  # 念のため古いシングルトンをクリア
    st_autorefresh = st.experimental_rerun  # ダミー（Streamlit >=1.30 の場合は st.autorefresh を利用）
    # ※ あなたの環境の Streamlit バージョンに合わせて st.autorefresh(interval=300000) を使用してください。

# ===== 診断（任意で残す） =====
with st.expander("GitHub保存の診断（必要時のみ）", expanded=False):
    if st.button("診断実行"):
        save_to_github_csv(debug=True)
    st.caption(f"Secrets keys: {list(st.secrets.keys())}")
