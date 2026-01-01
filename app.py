# -*- coding: utf-8 -*-
# Taskboard — 完全差し替え版（最初のUI: テーブル中心 / 更新者はプルダウン）
# 実行: streamlit run app_table_ui.py

import base64, io, json, uuid, requests
from dataclasses import dataclass
from typing import Tuple, Optional, Dict, Any
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

###############################################################################
# ユーティリティ
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
    # 全角/半角スペースを統一
    df.columns = [c.replace("\u3000", " ").strip() for c in df.columns]
    # 別名を正規化
    rename_map = {
        "更新": "更新日", "最終更新": "更新日", "更新日": "更新日",
        "起票": "起票日", "作成日": "起票日", "起票日": "起票日",
        "ID": "ID"
    }
    df.columns = [rename_map.get(c, c) for c in df.columns]
    # 必須列が無ければ作成
    for required in ["起票日", "更新日", "ID"]:
        if required not in df.columns:
            df[required] = ""
    return df


def strict_read_csv(content_bytes: bytes) -> pd.DataFrame:
    """CSV 読み込み（NaNを作らない & None/null/nan/空文字を正規化）"""
    csv_text = content_bytes.decode("utf-8")
    df = pd.read_csv(io.StringIO(csv_text), dtype=str, keep_default_na=False, na_filter=False)
    df = normalize_columns(df)
    df = df.applymap(lambda x: "" if is_missing(x) else ensure_str(x))
    return df


def safety_autofill_all(df: pd.DataFrame) -> pd.DataFrame:
    """保存直前の安全弁：起票日/更新日 欠損なら“いま”で補完"""
    now = jst_now_str()
    df["起票日"] = df["起票日"].apply(lambda x: now if is_missing(x) else ensure_str(x))
    df["更新日"] = df["更新日"].apply(lambda x: now if is_missing(x) else ensure_str(x))
    return df


def generate_uuid() -> str:
    return str(uuid.uuid4())

###############################################################################
# GitHub API
###############################################################################
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
        self.headers = {"Authorization": f"Bearer {config.token}", "Accept": "application/vnd.github+json"}

    def get(self) -> Tuple[int, Dict[str, Any]]:
        params = {"ref": self.config.branch}
        r = requests.get(self.base, headers=self.headers, params=params)
        try: body = r.json()
        except Exception: body = {"text": r.text}
        return r.status_code, body

    def put(self, sha: str, content_bytes: bytes, message: str,
            author: Optional[Dict[str, str]] = None, committer: Optional[Dict[str, str]] = None) -> Tuple[int, Dict[str, Any]]:
        payload = {
            "message": message,
            "content": base64.b64encode(content_bytes).decode("utf-8"),
            "sha": sha,
            "branch": self.config.branch,
        }
        if author: payload["author"] = author
        if committer: payload["committer"] = committer
        r = requests.put(self.base, headers=self.headers, data=json.dumps(payload))
        try: body = r.json()
        except Exception: body = {"text": r.text}
        return r.status_code, body

###############################################################################
# 差分検出＆補完ロジック（最初のUI用 / テーブル中心）
###############################################################################

def apply_autofill_on_operations(original: pd.DataFrame, edited: pd.DataFrame):
    """
    - 新規行（ID欠損/未登録）: ID発番、起票/更新=now
    - 既存行の差分 or クローズ化: 更新=now、起票欠損は補完
    - 保存直前に全行安全弁
    """
    now = jst_now_str()
    original = normalize_columns(original.copy()).astype(str)
    edited   = normalize_columns(edited.copy()).astype(str)
    edited   = edited.applymap(lambda x: "" if is_missing(x) else ensure_str(x))

    key = "ID"
    original_ids = set(original[key].tolist())
    ops = {"add": 0, "edit": 0, "close": 0}

    def is_empty_id(x: str) -> bool: return is_missing(x)

    for i, row in edited.iterrows():
        rid = ensure_str(row.get(key, ""))
        if is_empty_id(rid) or rid not in original_ids:
            # 新規
            new_id = generate_uuid() if is_empty_id(rid) else rid
            edited.at[i, key] = new_id
            edited.at[i, "起票日"] = now if is_missing(row.get("起票日", "")) else ensure_str(row.get("起票日", ""))
            edited.at[i, "更新日"] = now
            ops["add"] += 1
        else:
            # 既存 → 差分/クローズ判定
            o_row = original.loc[original[key] == rid].iloc[0]
            if is_missing(row.get("起票日", "")):
                edited.at[i, "起票日"] = now
            changed = any(
                ensure_str(o_row.get(c, "")) != ensure_str(row.get(c, ""))
                for c in edited.columns if c not in ["起票日", "更新日", "ID"]
            )
            closed = False
            if "対応状況" in edited.columns:
                prev = ensure_str(o_row.get("対応状況", "")).strip()
                cur  = ensure_str(row.get("対応状況", "")).strip()
                if prev != cur and cur == "クローズ": closed = True
            if changed or closed:
                edited.at[i, "更新日"] = now
                ops["edit"] += 1
                if closed: ops["close"] += 1
            else:
                # 変更なしでも更新日が欠損なら補完
                if is_missing(row.get("更新日", "")):
                    edited.at[i, "更新日"] = now

    edited = safety_autofill_all(edited).astype(str)
    return edited, ops

###############################################################################
# UI（最初の data_editor 中心 / 更新者はプルダウン）
###############################################################################

def main():
    st.set_page_config(page_title="Taskboard（テーブル中心UI）", layout="wide")
    st.title("Taskboard — 起票日・更新日の自動補完（JST / テーブル中心UI）")

    # サイドバー（Secrets 読み込み）
    st.sidebar.header("設定")
    GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
    REPO_OWNER   = st.secrets.get("REPO_OWNER", st.secrets.get("GITHUB_OWNER", ""))
    REPO_NAME    = st.secrets.get("REPO_NAME", st.secrets.get("GITHUB_REPO", ""))
    CSV_PATH     = st.secrets.get("CSV_PATH", st.secrets.get("GITHUB_PATH", "tasks.csv"))
    DEFAULT_BRANCH = st.secrets.get("DEFAULT_BRANCH", st.secrets.get("GITHUB_BRANCH", "main"))

    commit_user = st.sidebar.text_input("コミットユーザー名", value=st.secrets.get("COMMIT_USER", "Unknown User"))
    commit_email = st.sidebar.text_input("コミットメール", value=st.secrets.get("COMMIT_EMAIL", "unknown@example.com"))
    save_branch = st.sidebar.text_input("保存先ブランチ", value=DEFAULT_BRANCH)

    if not all([GITHUB_TOKEN, REPO_OWNER, REPO_NAME, CSV_PATH]):
        st.error("Secrets が不足しています（GITHUB_TOKEN/REPO_OWNER/REPO_NAME/CSV_PATH）")
        st.stop()

    gh = GitHubClient(GitHubConfig(GITHUB_TOKEN, REPO_OWNER, REPO_NAME, CSV_PATH, save_branch))

    # 取得
    with st.spinner("GitHub から CSV を取得中..."):
        get_status, get_body = gh.get()
    if get_status != 200:
        st.error(f"GET 失敗: status={get_status}\n{get_body}")
        st.stop()

    current_sha = ensure_str(get_body.get("sha", ""))
    encoded     = ensure_str(get_body.get("content", ""))
    if not encoded:
        st.error("GitHub GET の応答に content がありません。パス/ブランチを確認してください。")
        st.stop()

    content_bytes = base64.b64decode(encoded)
    df_original   = strict_read_csv(content_bytes)

    # 更新者の選択肢（CSV既存＋固定）
    existing_owners = sorted(set(
        str(x).strip() for x in df_original.get("更新者", []).tolist()
        if str(x).strip() not in {"", "None", "none", "nan"}
    ))
    fixed_owners = st.secrets.get("FIXED_OWNERS", ["都筑 颯樹", "担当A", "担当B"])  # 必要に応じて secrets.toml で上書き
    owner_options = sorted(set(existing_owners + list(fixed_owners)))

    # 列設定（最初のUIに近い構成）
    column_config = {
        "起票日": st.column_config.TextColumn("起票日"),
        "更新日": st.column_config.TextColumn("更新日"),
        "タスク": st.column_config.TextColumn("タスク", width="large"),
        "対応状況": st.column_config.SelectboxColumn("対応状況", options=["未対応", "対応中", "クローズ"], default="未対応"),
        # ★更新者はプルダウンに戻す
        "更新者": st.column_config.SelectboxColumn("更新者", options=owner_options, allow_none=True),
        "次アクション": st.column_config.TextColumn("次アクション"),
        "備考": st.column_config.TextColumn("備考", width="large"),
        "ソース": st.column_config.TextColumn("ソース"),
        "ID": st.column_config.TextColumn("ID"),
    }

    # 編集テーブル（最初のUI）
    st.subheader("編集（右上の『保存』でコミット）")
    edited = st.data_editor(
        df_original,
        num_rows="dynamic",
        use_container_width=True,
        column_config=column_config,
        hide_index=True,
    )

    st.markdown("---")
    do_save = st.button("保存（補完＆GitHubへコミット）", type="primary")

    # 保存処理
    if do_save:
        df_edited, ops = apply_autofill_on_operations(df_original, edited)
        df_edited = df_edited.astype(str)
        csv_bytes = df_edited.to_csv(index=False).encode("utf-8")

        commit_message = f"Save by {commit_user} — add:{ops['add']} edit:{ops['edit']} close:{ops['close']}"
        author = {"name": commit_user, "email": commit_email}
        committer = {"name": commit_user, "email": commit_email}

        with st.spinner("GitHub へ PUT 中..."):
            put_status, put_body = gh.put(current_sha, csv_bytes, commit_message, author=author, committer=committer)

        with st.expander("保存結果 / PUT 応答"):
            st.write({"PUT_status": put_status})
            st.code(json.dumps(put_body, ensure_ascii=False, indent=2))

        if put_status in (200, 201):
            st.success("保存に成功しました。最新CSVで再表示します。")
            try: st.cache_data.clear()
            except Exception: pass
            st.experimental_rerun()
        elif put_status == 422:
            st.error("競合（422）。最新の内容を取得してから再保存してください。必要なら自動マージ版（app_full.py）をご利用ください。")
        else:
            st.error("保存に失敗しました。診断情報をご確認ください。")

if __name__ == "__main__":
    main()
