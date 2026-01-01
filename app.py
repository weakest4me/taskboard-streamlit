# -*- coding: utf-8 -*-
# Taskboard — 完全版（起票日/更新日の自動補完・簡易UI・競合解決）
# 実行: streamlit run app.py

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
    """
    - 新規行（ID欠損/新規ID）: 起票日=now, 更新日=now をセット
    - 既存行の変更（任意カラム変更 or 対応状況が"クローズ"へ）: 更新日=now を上書き、起票日が欠損なら補完
    - 削除は edited に存在しない ID を落とす件数を記録（実際の削除は行わない）
    """
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

            # 差分（起票日/更新日/ID以外で比較）
            changed = any(ensure_str(o_row.get(c, "")) != ensure_str(row.get(c, "")) for c in edited.columns if c not in ["起票日", "更新日", "ID"])

            # クローズ判定
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

    # 参考: baseにあるがeditedに無いID数（削除）
    ops["delete"] = sum(1 for i in original_ids if i not in edited_ids)

    # 最終安全弁（全行）
    edited = safety_autofill_all(edited).astype(str)
    return edited, ops

# ---------------------------------------------------------------------
# 競合（422）時の簡易マージ: IDキーで編集側優先＋日付補完
# ---------------------------------------------------------------------

def merge_by_id(base: pd.DataFrame, edited: pd.DataFrame) -> pd.DataFrame:
    base = normalize_columns(base.copy()).astype(str)
    edited = normalize_columns(edited.copy()).astype(str)
    now = jst_now_str()

    # indexをIDにそろえる
    base_idx = base.set_index("ID", drop=False)
    edit_idx = edited.set_index("ID", drop=False)

    # 全IDの和集合
    all_ids: List[str] = sorted(set(base_idx.index).union(set(edit_idx.index)))
    cols = list({*base.columns, *edited.columns})
    out = pd.DataFrame(columns=cols)

    for _id in all_ids:
        if _id in edit_idx.index and _id in base_idx.index:
            # 両方ある → 非日付列は編集優先
            merged = {}
            for c in cols:
                if c in ["起票日", "更新日", "ID"]:
                    continue
                merged[c] = ensure_str(edit_idx.loc[_id].get(c, base_idx.loc[_id].get(c, "")))
            # 起票日: 編集が欠損なら base→欠損なら now
            ep = ensure_str(edit_idx.loc[_id].get("起票日", ""))
            bp = ensure_str(base_idx.loc[_id].get("起票日", ""))
            merged["起票日"] = ep if not is_missing(ep) else (bp if not is_missing(bp) else now)
            # 更新日は now
            merged["更新日"] = now
            merged["ID"] = _id
            out = pd.concat([out, pd.DataFrame([merged])], ignore_index=True)
        elif _id in edit_idx.index and _id not in base_idx.index:
            # 編集側のみ → 新規
            row = edit_idx.loc[_id].to_dict()
            row["起票日"] = ensure_str(row.get("起票日", "")) if not is_missing(row.get("起票日", "")) else now
            row["更新日"] = now
            row["ID"] = _id
            out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)
        else:
            # base側のみ → そのまま（更新日はそのまま維持）
            row = base_idx.loc[_id].to_dict()
            row["起票日"] = ensure_str(row.get("起票日", "")) if not is_missing(row.get("起票日", "")) else now
            row["更新日"] = ensure_str(row.get("更新日", "")) if not is_missing(row.get("更新日", "")) else now
            row["ID"] = _id
            out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)

    out = normalize_columns(out).astype(str)
    out = safety_autofill_all(out)
    return out

# ---------------------------------------------------------------------
# UI（新規フォーム＋編集テーブル／起票/更新は読み取り専用／フィルタ付き）
# ---------------------------------------------------------------------

def main():
    st.set_page_config(page_title="Taskboard（完全版）", layout="wide")
    st.title("Taskboard — 起票日・更新日の自動補完（JST／完全版）")

    st.sidebar.header("設定")
    GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", "")
    REPO_OWNER   = st.secrets.get("REPO_OWNER", st.secrets.get("GITHUB_OWNER", ""))
    REPO_NAME    = st.secrets.get("REPO_NAME", st.secrets.get("GITHUB_REPO", ""))
    CSV_PATH     = st.secrets.get("CSV_PATH", st.secrets.get("GITHUB_PATH", "tasks.csv"))
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

    # セッション作業用DF
    if "df_work" not in st.session_state:
        st.session_state["df_work"] = df_original.copy()
    df_work = st.session_state["df_work"]

    # ----------------------- フィルタ -----------------------
    st.subheader("フィルタ")
    colf1, colf2, colf3 = st.columns([2,2,2])
    with colf1:
        status_filter = st.multiselect("対応状況で絞り込み", options=["未対応","対応中","クローズ"], default=["未対応","対応中","クローズ"])
    with colf2:
        owner_filter = st.text_input("更新者を含むキーワード", "")
    with colf3:
        text_filter = st.text_input("タスク/備考/次アクション/ソースのキーワード", "")

    df_view = df_work.copy()
    if status_filter:
        df_view = df_view[df_view.get("対応状況", "").isin(status_filter)]
    if owner_filter.strip():
        df_view = df_view[df_view.get("更新者", "").str.contains(owner_filter.strip(), na=False)]
    if text_filter.strip():
        kw = text_filter.strip()
        cols = [c for c in ["タスク","備考","次アクション","ソース"] if c in df_view.columns]
        if cols:
            df_view = df_view[df_view[cols].apply(lambda r: any(ensure_str(v).find(kw) >= 0 for v in r), axis=1)]

    # ----------------------- 新規追加フォーム -----------------------
    st.subheader("新規追加（ボタンで起票/更新/ID 自動設定）")
    with st.form("add_form", clear_on_submit=True):
        col1, col2, col3 = st.columns([2,1,1])
        with col1:
            new_task = st.text_input("タスク", "")
        with col2:
            new_status = st.selectbox("対応状況", ["未対応", "対応中", "クローズ"], index=0)
        with col3:
            new_owner = st.text_input("更新者", commit_user)

        new_next = st.text_input("次アクション", "")
        new_note = st.text_input("備考", "")
        new_src  = st.text_input("ソース", "")

        submitted = st.form_submit_button("＋ 追加（起票・更新を自動付与）", type="primary")
        if submitted:
            now = jst_now_str()
            new_row = {
                "起票日": now,
                "更新日": now,
                "タスク": new_task,
                "対応状況": new_status,
                "更新者": new_owner,
                "次アクション": new_next,
                "備考": new_note,
                "ソース": new_src,
                "ID": generate_uuid(),
            }
            df_work = pd.concat([df_work, pd.DataFrame([new_row])], ignore_index=True)
            st.session_state["df_work"] = df_work
            st.success("新規行を追加しました（起票/更新はJSTの“いま”）")

    # ----------------------- 編集テーブル（起票/更新は読み取り専用） -----------------------
    st.subheader("編集（起票日/更新日は自動で扱います）")
    column_config = {
        "起票日": st.column_config.TextColumn("起票日", disabled=True),
        "更新日": st.column_config.TextColumn("更新日", disabled=True),
        "タスク": st.column_config.TextColumn("タスク", width="large"),
        "対応状況": st.column_config.SelectboxColumn("対応状況", options=["未対応", "対応中", "クローズ"], default="未対応"),
        "更新者": st.column_config.TextColumn("更新者"),
        "次アクション": st.column_config.TextColumn("次アクション"),
        "備考": st.column_config.TextColumn("備考", width="large"),
        "ソース": st.column_config.TextColumn("ソース"),
        "ID": st.column_config.TextColumn("ID"),
    }

    edited = st.data_editor(
        df_view,
        key="editor",
        num_rows="dynamic",
        use_container_width=True,
        column_config=column_config,
        hide_index=True,
    )

    # 編集結果を元DFに反映（フィルタしているためIDでマージ）
    edited_idx = edited.set_index("ID", drop=False)
    work_idx = df_work.set_index("ID", drop=False)
    for _id in edited_idx.index:
        if _id in work_idx.index:
            for c in edited.columns:
                if c in ["起票日","更新日","ID"]: continue
                work_idx.at[_id, c] = ensure_str(edited_idx.at[_id, c])
    df_work = work_idx.reset_index(drop=True)
    st.session_state["df_work"] = df_work

    # 保存直前チェック（補完後をプレビュー）
    with st.expander("保存直前チェック（補完後サンプル10行）"):
        preview = safety_autofill_all(df_work.copy()).astype(str)
        st.write(preview[["ID","起票日","更新日"]].tail(10))
        st.write({"missing_起票日": int(preview["起票日"].apply(is_missing).sum()),
                  "missing_更新日": int(preview["更新日"].apply(is_missing).sum())})

    st.markdown("---")
    col_a, col_b = st.columns([1, 2])
    with col_a:
        do_save = st.button("保存（補完＆GitHubへコミット）", type="primary")
    with col_b:
        st.write("※ 起票日/更新日は保存時に欠損があっても自動補完します。クローズや編集時は更新日を“いま”で上書きします。")

    # ----------------------- 保存処理 -----------------------
    if do_save:
        df_edited, ops = apply_autofill_on_operations(df_original, df_work)
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

        if put_status in (200, 201):
            st.success("保存に成功しました。最新CSVで再表示します。")
            try: st.cache_data.clear()
            except Exception: pass
            st.session_state["df_work"] = df_edited.copy()  # 画面DFも最新
            st.experimental_rerun()
        elif put_status == 422:
            # 競合 → 最新GETして簡易マージ→再PUT
            st.warning("競合（422）。最新を取得してマージ保存を試みます……")
            latest_status, latest_body = gh.get()
            if latest_status == 200 and ensure_str(latest_body.get("content", "")):
                latest_bytes = base64.b64decode(latest_body["content"])  # 最新CSV
                df_latest = strict_read_csv(latest_bytes)
                merged = merge_by_id(df_latest, df_edited)  # 最新と編集をIDで統合
                csv2 = merged.astype(str).to_csv(index=False).encode("utf-8")
                new_sha = ensure_str(latest_body.get("sha", ""))
                commit_message2 = commit_message + " (auto-merged)"
                with st.spinner("競合解消後の再PUT 中..."):
                    put2_status, put2_body = gh.put(new_sha, csv2, commit_message2, author=author, committer=committer)
                with st.expander("競合解消 / 再PUT 応答"):
                    st.write({"PUT_status": put2_status})
                    st.code(json.dumps(put2_body, ensure_ascii=False, indent=2))
                if put2_status in (200, 201):
                    st.success("マージ保存に成功しました。最新CSVで再表示します。")
                    try: st.cache_data.clear()
                    except Exception: pass
                    st.session_state["df_work"] = merged.copy()
                    st.experimental_rerun()
                else:
                    st.error("マージ保存に失敗しました。応答をご確認ください。")
            else:
                st.error("最新内容の取得に失敗しました。ネットワークや権限を確認してください。")
        else:
            st.error("保存に失敗しました。診断情報をご確認ください。")

    # --- 診断 ---
    with st.expander("診断 / GitHub I/O"):
        st.write({"GET_status": get_status, "GET_sha": current_sha, "branch": save_branch})
        st.code(json.dumps({k: v for k, v in get_body.items() if k in ["name", "path", "sha", "size", "html_url", "download_url"]}, ensure_ascii=False, indent=2))

# エントリポイント
if __name__ == "__main__":
    main()
