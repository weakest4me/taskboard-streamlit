# -*- coding: utf-8 -*-
# app.py — 補完ロジック強化版（None/null/nan/空文字すべて補完対象）
# 注意：このファイルは Streamlit アプリ用です。実行には `streamlit run app.py` を使用してください。

import base64
import io
import json
import uuid
from dataclasses import dataclass
from typing import Tuple, Optional, Dict, Any

import pandas as pd
import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo
import requests

###############################################################################
# 設定 / ユーティリティ
###############################################################################
JST = ZoneInfo("Asia/Tokyo")


def jst_now_str() -> str:
    """SAVE_WITH_TIME に応じて JST の現在を文字列化"""
    with_time = bool(st.secrets.get("SAVE_WITH_TIME", True))
    now = datetime.now(JST)
    return now.strftime("%Y-%m-%d %H:%M:%S") if with_time else now.strftime("%Y-%m-%d")


def ensure_str(x) -> str:
    return "" if x is None else str(x)


def is_missing(x) -> bool:
    s = ensure_str(x).strip().lower()
    return s == "" or s in {"nan", "none", "null", "na", "n/a", "-", "—"}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # 全角/半角スペースの統一
    df.columns = [c.replace("　", " ").strip() for c in df.columns]
    # よくある別名を正規化
    rename_map = {
        "更新": "更新日", "最終更新": "更新日", "更新日": "更新日",
        "起票": "起票日", "作成日": "起票日", "起票日": "起票日",
        "ID": "ID"
    }
    df.columns = [rename_map.get(c, c) for c in df.columns]
    # 必須列を用意
    for required in ["起票日", "更新日", "ID"]:
        if required not in df.columns:
            df[required] = ""
    return df


def strict_read_csv(content_bytes: bytes) -> pd.DataFrame:
    csv_text = content_bytes.decode("utf-8")
    df = pd.read_csv(io.StringIO(csv_text), dtype=str, keep_default_na=False, na_filter=False)
    df = normalize_columns(df)
    # 読み込み直後に None/NULL 系を空文字へ正規化
    df = df.applymap(lambda x: "" if is_missing(x) else ensure_str(x))
    return df


def safety_autofill_all(df: pd.DataFrame) -> pd.DataFrame:
    now = jst_now_str()
    df["起票日"] = df["起票日"].apply(lambda x: now if is_missing(x) else ensure_str(x))
    df["更新日"] = df["更新日"].apply(lambda x: now if is_missing(x) else ensure_str(x))
    return df


def generate_uuid() -> str:
    return str(uuid.uuid4())


@dataclass
class GitHubConfig:
    token: str
    owner: str
    repo: str
    path: str
    branch: str


class GitHubClient:
    def __init__(self, config: GitHubConfig):
        self.config = config
        self.base = f"https://api.github.com/repos/{config.owner}/{config.repo}/contents/{config.path}"
        self.headers = {
            "Authorization": f"Bearer {config.token}",
            "Accept": "application/vnd.github+json"
        }

    def get(self) -> Tuple[int, Dict[str, Any]]:
        params = {"ref": self.config.branch}
        r = requests.get(self.base, headers=self.headers, params=params)
        try:
            body = r.json()
        except Exception:
            body = {"text": r.text}
        return r.status_code, body

    def put(self, sha: str, content_bytes: bytes, message: str, author: Optional[Dict[str, str]] = None,
            committer: Optional[Dict[str, str]] = None) -> Tuple[int, Dict[str, Any]]:
        payload = {
            "message": message,
            "content": base64.b64encode(content_bytes).decode("utf-8"),
            "sha": sha,
            "branch": self.config.branch,
        }
        if author:
            payload["author"] = author
        if committer:
            payload["committer"] = committer
        r = requests.put(self.base, headers=self.headers, data=json.dumps(payload))
        try:
            body = r.json()
        except Exception:
            body = {"text": r.text}
        return r.status_code, body


###############################################################################
# 差分検出＆補完ロジック（ここが “壊さずに直す” 要の部分）
###############################################################################

def apply_autofill_on_operations(original: pd.DataFrame, edited: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    - 新規行（ID欠損/新規ID）: 起票日=now, 更新日=now をセット
    - 既存行の変更（任意カラム変更 or 対応状況が"クローズ"へ）: 更新日=now を上書き、起票日が欠損なら補完
    - 削除は edited に存在しない ID を落とす
    """
    now = jst_now_str()
    original = normalize_columns(original.copy()).astype(str)
    edited   = normalize_columns(edited.copy()).astype(str)

    # 編集DFの None/null を正規化
    edited = edited.applymap(lambda x: "" if is_missing(x) else ensure_str(x))

    key = "ID"
    original_ids = set(original[key].tolist())
    edited_ids   = set(edited[key].tolist())

    ops = {"add": 0, "edit": 0, "close": 0, "delete": 0}

    def is_empty_id(x: str) -> bool:
        return is_missing(x)

    for i, row in edited.iterrows():
        if is_empty_id(row[key]) or row[key] not in original_ids:
            # 新規
            new_id = generate_uuid() if is_empty_id(row[key]) else row[key]
            edited.at[i, key] = new_id
            # 起票/更新 日付セット
            edited.at[i, "起票日"] = now if is_missing(row.get("起票日", "")) else ensure_str(row.get("起票日", ""))
            edited.at[i, "更新日"] = now
            ops["add"] += 1
        else:
            # 既存 → 差分判定
            o = original.loc[original[key] == row[key]]
            if o.empty:
                continue
            o_row = o.iloc[0]

            # 起票日は欠損なら補完
            if is_missing(row.get("起票日", "")):
                edited.at[i, "起票日"] = now

            # 差分（起票日/更新日/ID以外で比較）
            changed = any(
                ensure_str(o_row.get(c, "")) != ensure_str(row.get(c, ""))
                for c in edited.columns if c not in ["起票日", "更新日", "ID"]
            )

            # クローズ判定
            closed = False
            if "対応状況" in edited.columns:
                prev = ensure_str(o_row.get("対応状況", "")).strip()
                cur  = ensure_str(row.get("対応状況", "")).strip()
                if prev != cur and cur == "クローズ":
                    closed = True

            if changed or closed:
                edited.at[i, "更新日"] = now
                ops["edit"] += 1
                if closed:
                    ops["close"] += 1
            else:
                # 変更なし＆更新日が欠損なら安全弁
                if is_missing(row.get("更新日", "")):
                    edited.at[i, "更新日"] = now

    # 削除（original にあって edited にない ID）
    ops["delete"] = sum(1 for i in original_ids if i not in edited_ids)

    # 最終安全弁（全行）
    edited = safety_autofill_all(edited).astype(str)
    return edited, ops


###############################################################################
# Streamlit UI
###############################################################################
st.set_page_config(page_title="Taskboard (補完ロジック版)", layout="wide")

st.title("Taskboard — 起票日・更新日の自動補完（JST）")

# --- Secrets / Sidebar ---
st.sidebar.header("設定")
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
REPO_OWNER = st.secrets.get("REPO_OWNER", st.secrets.get("GITHUB_OWNER", ""))
REPO_NAME = st.secrets.get("REPO_NAME", st.secrets.get("GITHUB_REPO", ""))
CSV_PATH = st.secrets.get("CSV_PATH", st.secrets.get("GITHUB_PATH", "tasks.csv"))
DEFAULT_BRANCH = st.secrets.get("DEFAULT_BRANCH", st.secrets.get("GITHUB_BRANCH", "main"))

commit_user = st.sidebar.text_input("コミットユーザー名", value=st.secrets.get("COMMIT_USER", "Unknown User"))
commit_email = st.sidebar.text_input("コミットメール", value=st.secrets.get("COMMIT_EMAIL", "unknown@example.com"))
save_with_time = st.sidebar.checkbox("日時まで保存（SAVE_WITH_TIME）", value=bool(st.secrets.get("SAVE_WITH_TIME", True)))
if save_with_time != bool(st.secrets.get("SAVE_WITH_TIME", True)):
    st.session_state["SAVE_WITH_TIME"] = save_with_time

st.sidebar.markdown("---")
save_branch = st.sidebar.text_input("保存先ブランチ", value=DEFAULT_BRANCH)

if not all([GITHUB_TOKEN, REPO_OWNER, REPO_NAME, CSV_PATH]):
    st.error("Secrets が不足しています（GITHUB_TOKEN/REPO_OWNER/REPO_NAME/CSV_PATH）")
    st.stop()

gh = GitHubClient(GitHubConfig(GITHUB_TOKEN, REPO_OWNER, REPO_NAME, CSV_PATH, save_branch))

# --- GET ---
with st.spinner("GitHub から CSV を取得中..."):
    get_status, get_body = gh.get()

if get_status != 200:
    st.error(f"GET 失敗: status={get_status}\n{get_body}")
    st.stop()

# GitHub body から content/sha を取得
current_sha = ensure_str(get_body.get("sha", ""))
encoded = ensure_str(get_body.get("content", ""))
if not encoded:
    st.error("GitHub GET の応答に content がありません。パスやブランチを確認してください。")
    st.stop()

content_bytes = base64.b64decode(encoded)
df_original = strict_read_csv(content_bytes)

st.subheader("編集（右上の『保存』でコミット）")
# 列型定義（あれば）
column_config = {
    "起票日": st.column_config.TextColumn("起票日"),
    "更新日": st.column_config.TextColumn("更新日"),
    "タスク": st.column_config.TextColumn("タスク", width="large"),
    "対応状況": st.column_config.SelectboxColumn("対応状況", options=["未対応", "対応中", "クローズ", "未対応"], default="未対応"),
    "更新者": st.column_config.TextColumn("更新者"),
    "次アクション": st.column_config.TextColumn("次アクション"),
    "備考": st.column_config.TextColumn("備考", width="large"),
    "ソース": st.column_config.TextColumn("ソース"),
    "ID": st.column_config.TextColumn("ID"),
}

edited = st.data_editor(
    df_original,
    num_rows="dynamic",
    use_container_width=True,
    column_config=column_config,
    hide_index=True,
)

st.markdown("---")
col_a, col_b = st.columns([1, 1])

with col_a:
    do_save = st.button("保存（補完＆GitHubへコミット）", type="primary")
with col_b:
    st.write("※ 保存時に『起票日/更新日』の欠損を自動補完し、JSTの“いま”で更新します。")

# --- 保存直前チェック（見える化） ---
with st.expander("保存直前チェック（補完後サンプル10行）"):
    preview = safety_autofill_all(edited.copy()).astype(str)
    st.write(preview[["ID","起票日","更新日"]].tail(10))
    st.write({
        "missing_起票日": int(preview["起票日"].apply(is_missing).sum()),
        "missing_更新日": int(preview["更新日"].apply(is_missing).sum())
    })

# --- 診断 ---
with st.expander("診断 / GitHub I/O"):
    st.write({"GET_status": get_status, "GET_sha": current_sha, "branch": save_branch})
    st.code(json.dumps({k: v for k, v in get_body.items() if k in ["name", "path", "sha", "size", "html_url", "download_url"]}, ensure_ascii=False, indent=2))

# --- 保存処理 ---
if do_save:
    # 差分検出＋補完
    df_edited, ops = apply_autofill_on_operations(df_original, edited)

    # CSV バイト化（文字列化の上で）
    df_edited = df_edited.astype(str)
    csv_bytes = df_edited.to_csv(index=False).encode("utf-8")

    commit_message = f"Save by {commit_user} — add:{ops['add']} edit:{ops['edit']} close:{ops['close']} delete:{ops['delete']}"
    author = {"name": commit_user, "email": commit_email}
    committer = {"name": commit_user, "email": commit_email}

    with st.spinner("GitHub へ PUT 中..."):
        put_status, put_body = gh.put(current_sha, csv_bytes, commit_message, author=author, committer=committer)

    with st.expander("保存結果 / PUT 応答"):
        st.write({"PUT_status": put_status})
        st.code(json.dumps(put_body, ensure_ascii=False, indent=2))

    if put_status == 200 or put_status == 201:
        st.success("保存に成功しました。キャッシュをクリアして再描画します。")
        try:
            st.cache_data.clear()
        except Exception:
            pass
        st.experimental_rerun()
    elif put_status == 422:
        st.error("競合（422）が発生しました。最新の SHA を取得して再保存を試してください。必要なら差分マージ機能を追加します。")
    else:
        st.error("保存に失敗しました。診断情報を確認してください。")

# --- フッター補助 ---
st.markdown(
    """
**補足**
- 読み込み時に `dtype=str, keep_default_na=False, na_filter=False` を適用し、NaN を作らないようにしています。
- 追加/編集/クローズ時は自動的に `更新日` を JST の“いま”で上書きし、`起票日` が欠損なら補完します。
- 保存直前には全行に対して安全弁（`safety_autofill_all`）を適用します。
- コミットにはユーザー名と操作サマリ（add/edit/close/delete）を含めています。
    """
)
