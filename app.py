# -*- coding: utf-8 -*-
# Taskboard — 編集しやすさ重視版（行の詳細フォームで編集／起票・更新は自動補完）
# 実行: streamlit run app_focus.py

import base64, io, json, uuid, requests
from dataclasses import dataclass
from typing import Tuple, Optional, Dict, Any, List
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------
# 基本ユーティリティ
# ---------------------------------------------------------------------
JST = ZoneInfo("Asia/Tokyo")

def jst_now_str() -> str:
    with_time = bool(st.secrets.get("SAVE_WITH_TIME", True))
    now = datetime.now(JST)
    return now.strftime("%Y-%m-%d %H:%M:%S") if with_time else now.strftime("%Y-%m-%d")

def ensure_str(x) -> str:
    return "" if x is None else str(x)

def is_missing(x) -> bool:
    s = ensure_str(x).strip().lower()
    return s == "" or s in {"nan", "none", "null", "na", "n/a", "-", "—"}

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.replace("\u3000", " ").strip() for c in df.columns]
    rename_map = {
        "更新": "更新日", "最終更新": "更新日", "更新日": "更新日",
        "起票": "起票日", "作成日": "起票日", "起票日": "起票日",
        "ID": "ID"
    }
    df.columns = [rename_map.get(c, c) for c in df.columns]
    for required in ["起票日", "更新日", "ID"]:
        if required not in df.columns:
            df[required] = ""
    return df

def strict_read_csv(content_bytes: bytes) -> pd.DataFrame:
    csv_text = content_bytes.decode("utf-8")
    df = pd.read_csv(io.StringIO(csv_text), dtype=str, keep_default_na=False, na_filter=False)
    df = normalize_columns(df)
    df = df.applymap(lambda x: "" if is_missing(x) else ensure_str(x))  # 読み込み直後に正規化
    return df

def safety_autofill_all(df: pd.DataFrame) -> pd.DataFrame:
    now = jst_now_str()
    df["起票日"] = df["起票日"].apply(lambda x: now if is_missing(x) else ensure_str(x))
    df["更新日"] = df["更新日"].apply(lambda x: now if is_missing(x) else ensure_str(x))
    return df

def generate_uuid() -> str:
    return str(uuid.uuid4())

# ---------------------------------------------------------------------
# GitHub API
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# 補完ロジック（Add/Edit/Close）
# ---------------------------------------------------------------------

def apply_autofill_on_operations(original: pd.DataFrame, edited: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    now = jst_now_str()
    original = normalize_columns(original.copy()).astype(str)
    edited   = normalize_columns(edited.copy()).astype(str)
    edited   = edited.applymap(lambda x: "" if is_missing(x) else ensure_str(x))

    key = "ID"
    original_ids = set(original[key].tolist())
    edited_ids   = set(edited[key].tolist())
    ops = {"add": 0, "edit": 0, "close": 0, "delete": 0}

    def is_empty_id(x: str) -> bool: return is_missing(x)

    for i, row in edited.iterrows():
        if is_empty_id(row[key]) or row[key] not in original_ids:
            # 新規
            new_id = generate_uuid() if is_empty_id(row[key]) else row[key]
            edited.at[i, key] = new_id
            edited.at[i, "起票日"] = now if is_missing(row.get("起票日", "")) else ensure_str(row.get("起票日", ""))
            edited.at[i, "更新日"] = now
            ops["add"] += 1
        else:
            # 既存
            o = original.loc[original[key] == row[key]]
            if o.empty: continue
            o_row = o.iloc[0]

            if is_missing(row.get("起票日", "")):
                edited.at[i, "起票日"] = now

            changed = any(ensure_str(o_row.get(c, "")) != ensure_str(row.get(c, "")) for c in edited.columns if c not in ["起票日", "更新日", "ID"])

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
                if is_missing(row.get("更新日", "")):
                    edited.at[i, "更新日"] = now

    ops["delete"] = sum(1 for i in original_ids if i not in edited_ids)
    edited = safety_autofill_all(edited).astype(str)
    return edited, ops

# ---------------------------------------------------------------------
# UI（左に表・右に選択行の詳細フォーム／起票/更新は読み取り専用）
# ---------------------------------------------------------------------

def main():
    st.set_page_config(page_title="Taskboard（編集しやすさ重視）", layout="wide")
    st.title("Taskboard — 起票日・更新日の自動補完（JST）")

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

    with st.spinner("GitHub から CSV を取得中..."):
        get_status, get_body = gh.get()
    if get_status != 200:
        st.error(f"GET 失敗: status={get_status}\n{get_body}")
        st.stop()

    current_sha = ensure_str(get_body.get("sha", ""))
    encoded     = ensure_str(get_body.get("content", ""))
    if not encoded:
        st.error("GitHub GET の応答に content がありません。パスやブランチを確認してください。")
        st.stop()

    content_bytes = base64.b64decode(encoded)
    df_original   = strict_read_csv(content_bytes)

    if "df_work" not in st.session_state:
        st.session_state["df_work"] = df_original.copy()
    df_work = st.session_state["df_work"]

    # 左右2ペイン
    left, right = st.columns([1.4, 1])

    # 左: 表（読みやすく）
    with left:
        st.subheader("一覧")
        view_cols = [c for c in ["起票日","更新日","タスク","対応状況","更新者","次アクション","備考","ソース","ID"] if c in df_work.columns]
        st.dataframe(df_work[view_cols], use_container_width=True, height=520)

        # 新規追加クイックフォーム（行追加はここで）
        with st.expander("＋ 新規追加（起票・更新・IDは自動付与）"):
            col1, col2, col3 = st.columns([2,1,1])
            with col1:
                q_task = st.text_input("タスク", "")
            with col2:
                q_status = st.selectbox("対応状況", ["未対応","対応中","クローズ"], index=0)
            with col3:
                q_owner = st.text_input("更新者", commit_user)
            q_next = st.text_input("次アクション", "")
            q_note = st.text_input("備考", "")
            q_src  = st.text_input("ソース", "")
            if st.button("追加する", type="primary"):
                now = jst_now_str()
                new_row = {
                    "起票日": now,
                    "更新日": now,
                    "タスク": q_task,
                    "対応状況": q_status,
                    "更新者": q_owner,
                    "次アクション": q_next,
                    "備考": q_note,
                    "ソース": q_src,
                    "ID": generate_uuid(),
                }
                df_work = pd.concat([df_work, pd.DataFrame([new_row])], ignore_index=True)
                st.session_state["df_work"] = df_work
                st.success("追加しました（起票/更新はJSTの“いま”）")

    # 右: 選択行の詳細編集フォーム
    with right:
        st.subheader("詳細編集")
        # 行選択（IDで）
        options = [
            {
                "label": f"{ensure_str(r.get('タスク',''))} | {ensure_str(r.get('対応状況',''))} | {ensure_str(r.get('更新者',''))} | {ensure_str(r.get('ID',''))}",
                "id": ensure_str(r.get("ID",""))
            }
            for _, r in df_work.iterrows()
        ]
        option_labels = [o["label"] for o in options]
        option_ids    = [o["id"] for o in options]
        if not option_ids:
            st.info("行がありません。左の『＋ 新規追加』から登録してください。")
        else:
            selected_label = st.selectbox("編集対象", options=option_labels, index=0)
            selected_id = option_ids[option_labels.index(selected_label)]

            row = df_work.loc[df_work["ID"] == selected_id].iloc[0].to_dict()
            st.write(f"**ID**: {selected_id}")
            st.write(f"**起票日**: {ensure_str(row.get('起票日',''))}")
            st.write(f"**更新日**: {ensure_str(row.get('更新日',''))}")

            with st.form("detail_form"):
                t_task   = st.text_input("タスク", ensure_str(row.get("タスク","")))
                t_status = st.selectbox("対応状況", ["未対応","対応中","クローズ"], index=["未対応","対応中","クローズ"].index(ensure_str(row.get("対応状況","未対応"))))
                t_owner  = st.text_input("更新者", ensure_str(row.get("更新者","")))
                t_next   = st.text_input("次アクション", ensure_str(row.get("次アクション","")))
                t_note   = st.text_area("備考", ensure_str(row.get("備考","")))
                t_src    = st.text_input("ソース", ensure_str(row.get("ソース","")))

                colu1, colu2, colu3 = st.columns([1,1,1])
                upd_btn   = colu1.form_submit_button("更新", type="primary")
                close_btn = colu2.form_submit_button("クローズにする")
                dup_btn   = colu3.form_submit_button("複製して新規")

                if upd_btn:
                    # 値を適用
                    idx = df_work.index[df_work["ID"] == selected_id][0]
                    df_work.at[idx, "タスク"] = t_task
                    df_work.at[idx, "対応状況"] = t_status
                    df_work.at[idx, "更新者"] = t_owner
                    df_work.at[idx, "次アクション"] = t_next
                    df_work.at[idx, "備考"] = t_note
                    df_work.at[idx, "ソース"] = t_src
                    # 日付補完
                    now = jst_now_str()
                    if is_missing(df_work.at[idx, "起票日"]):
                        df_work.at[idx, "起票日"] = now
                    df_work.at[idx, "更新日"] = now
                    st.session_state["df_work"] = df_work
                    st.success("更新しました（更新日はJSTの“いま”）")

                if close_btn:
                    idx = df_work.index[df_work["ID"] == selected_id][0]
                    df_work.at[idx, "対応状況"] = "クローズ"
                    now = jst_now_str()
                    if is_missing(df_work.at[idx, "起票日"]):
                        df_work.at[idx, "起票日"] = now
                    df_work.at[idx, "更新日"] = now
                    st.session_state["df_work"] = df_work
                    st.success("クローズにしました（更新日はJSTの“いま”）")

                if dup_btn:
                    now = jst_now_str()
                    new_row = {
                        "起票日": now,
                        "更新日": now,
                        "タスク": t_task,
                        "対応状況": "未対応",
                        "更新者": t_owner,
                        "次アクション": t_next,
                        "備考": t_note,
                        "ソース": t_src,
                        "ID": generate_uuid(),
                    }
                    df_work = pd.concat([df_work, pd.DataFrame([new_row])], ignore_index=True)
                    st.session_state["df_work"] = df_work
                    st.success("複製から新規作成しました（起票/更新はJSTの“いま”）")

    st.markdown("---")
    do_save = st.button("保存（補完＆GitHubへコミット）", type="primary")

    if do_save:
        df_edited, ops = apply_autofill_on_operations(df_original, st.session_state["df_work"]) 
        df_edited = df_edited.astype(str)
        csv_bytes = df_edited.to_csv(index=False).encode("utf-8")

        commit_user = st.secrets.get("COMMIT_USER", "Unknown User")
        commit_email = st.secrets.get("COMMIT_EMAIL", "unknown@example.com")
        author = {"name": commit_user, "email": commit_email}
        committer = {"name": commit_user, "email": commit_email}
        commit_message = f"Save by {commit_user} — add:{ops['add']} edit:{ops['edit']} close:{ops['close']} delete:{ops['delete']}"

        with st.spinner("GitHub へ PUT 中..."):
            put_status, put_body = gh.put(current_sha, csv_bytes, commit_message, author=author, committer=committer)

        with st.expander("保存結果 / PUT 応答"):
            st.write({"PUT_status": put_status})
            st.code(json.dumps(put_body, ensure_ascii=False, indent=2))

        if put_status in (200, 201):
            st.success("保存に成功しました。最新CSVで再表示します。")
            try: st.cache_data.clear()
            except Exception: pass
            st.session_state["df_work"] = df_edited.copy()
            st.experimental_rerun()
        elif put_status == 422:
            st.error("競合（422）。この簡易版では自動マージを行いません。完全版(app_full.py)をご利用ください。")
        else:
            st.error("保存に失敗しました。診断情報をご確認ください。")

if __name__ == "__main__":
    main()
