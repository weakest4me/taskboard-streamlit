
# -*- coding: utf-8 -*-
"""
タスク管理ボード（完全版 / 複数人運用向け / タイムゾーン安全化 / Excel風ビュー）

機能:
- CSV 永続化 + GitHub 連携（GET→PUT, sha 楽観的ロック, 429 リトライ, committer=ログインユーザー）
- CSV インジェクション対策（= + - @ 先頭のセルを自動無害化）
- 起票日は自動・編集不可、更新日は編集/クローズ時に自動更新（JST）
- 簡易ログイン（Secrets の USERS によるトークン方式）
- 監査ログ（audit.csv, JSON構造）: 作成 / 更新 / 削除 / 一括削除 / クローズ を記録
- フィルタ、一覧（通常表 / Excel風）、クローズ候補抽出（対応中 + 返信待ち系 + 7日前より前の更新）
- 手動リフレッシュボタン（最新反映）・フィルタ結果のCSVダウンロード

注意:
- Secrets の SAVE_WITH_TIME は文字列でも正しく解釈されます（true/false/1/0/yes/no/on/off）。
- GITHUB_* の設定が必要です。監査ログを GitHub に保存する場合は GITHUB_PATH_AUDIT も設定します。
"""

import base64
import time
import uuid
from datetime import datetime, date
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st

# ==============================
# ページ設定
# ==============================
st.set_page_config(page_title="タスク管理ボード（完全版）", layout="wide")
st.title("タスク管理ボード（完全版 / 起票日は自動・編集不可、更新者はプルダウン）")

# ==============================
# Secrets & 定数
# ==============================
def get_bool_secret(key: str, default: bool = True) -> bool:
    v = st.secrets.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "on"}
    return bool(v)

CSV_PATH = st.secrets.get("CSV_PATH", "tasks.csv")
AUDIT_PATH = st.secrets.get("AUDIT_PATH", "audit.csv")
LOCK_PATH = st.secrets.get("LOCK_PATH", "locks.csv")  # 予約（将来用）
LOCK_TTL_MIN = int(st.secrets.get("LOCK_TTL_MIN", 10))

DEBUG_MODE = get_bool_secret("DEBUG_MODE", False)
SAVE_WITH_TIME = get_bool_secret("SAVE_WITH_TIME", True)  # True: YYYY-MM-DD HH:MM:SS / False: YYYY-MM-DD

REPLY_KEYWORDS = [k.strip() for k in st.secrets.get(
    "REPLY_KEYWORDS",
    ["返信待ち", "返信無し", "返信なし", "返信ない", "催促"]
) if k.strip()]

MANDATORY_COLS = [
    "ID", "起票日", "更新日", "タスク", "対応状況", "更新者", "次アクション", "備考", "ソース",
]

# タイムゾーン
JST = ZoneInfo("Asia/Tokyo")

def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    fmt = "%Y-%m-%d %H:%M:%S" if SAVE_WITH_TIME else "%Y-%m-%d"
    return now_jst().strftime(fmt)

def today_jst() -> date:
    return now_jst().date()

# ==============================
# CSVインジェクション対策
# ==============================
CSV_INJECTION_PREFIXES = ("=", "+", "-", "@")

def _ensure_str(x) -> str:
    return "" if x is None else str(x)

def _is_csv_hazard(s: str) -> bool:
    return s.startswith(CSV_INJECTION_PREFIXES)

def sanitize_for_csv(val: str) -> str:
    s = _ensure_str(val)
    return "'" + s if _is_csv_hazard(s) else s

def sanitize_df_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    text_cols = ["タスク", "対応状況", "更新者", "次アクション", "備考", "ソース"]
    for c in text_cols:
        if c in df.columns:
            df[c] = df[c].map(sanitize_for_csv)
    return df

# ==============================
# DataFrame 正規化
# ==============================
MISSING_SET = {"", "none", "null", "nan", "na", "n/a", "-", "—"}

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

# ==============================
# ロード／セーブ
# ==============================
def format_ts(dt) -> str:
    """CSV 保存時の日付フォーマット統一。NaT は“いま”で補完。"""
    if pd.isna(dt):
        dt = pd.Timestamp(now_jst())
    else:
        dt = pd.to_datetime(dt, errors="coerce")
        if pd.isna(dt):
            dt = pd.Timestamp(now_jst())
    return dt.strftime("%Y-%m-%d %H:%M:%S" if SAVE_WITH_TIME else "%Y-%m-%d")

def safety_autofill_all(df: pd.DataFrame) -> pd.DataFrame:
    now_ts = pd.Timestamp(now_jst())
    # 起票日は欠損のみ補完（既存起票日は維持）
    df["起票日"] = df["起票日"].apply(lambda x: now_ts if pd.isna(pd.to_datetime(x, errors="coerce")) else pd.to_datetime(x, errors="coerce"))
    # 更新日は欠損なら補完（編集/クローズ時は別途上書き）
    df["更新日"] = df["更新日"].apply(lambda x: now_ts if pd.isna(pd.to_datetime(x, errors="coerce")) else pd.to_datetime(x, errors="coerce"))
    return df

@st.cache_data(ttl=10)
def load_tasks() -> pd.DataFrame:
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    except FileNotFoundError:
        df = pd.DataFrame(columns=MANDATORY_COLS)
    df = _normalize_df(df)
    df = safety_autofill_all(df)  # 読み込み直後に安全弁
    return df

def save_tasks(df: pd.DataFrame):
    df_out = safety_autofill_all(df.copy())
    # CSVインジェクション対策
    df_out = sanitize_df_text_columns(df_out)
    # 日付を統一フォーマットに
    for col in ["起票日", "更新日"]:
        df_out[col] = pd.to_datetime(df_out[col], errors="coerce").apply(format_ts)
    df_out.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

# ==============================
# GitHub 連携
# ==============================
def _retry_after(headers) -> float:
    ra = headers.get("Retry-After")
    try:
        return float(ra)
    except Exception:
        return 5.0  # デフォルト待機

def save_to_github_file(local_path: str, remote_path: str, commit_message: str, debug: bool = False) -> bool:
    required_keys = ["GITHUB_TOKEN", "GITHUB_OWNER", "GITHUB_REPO"]
    missing = [k for k in required_keys if k not in st.secrets]
    branch = st.secrets.get("GITHUB_BRANCH", "main")
    api_base = st.secrets.get("GITHUB_API_BASE", "https://api.github.com")

    if missing:
        st.error(f"Secrets が不足しています: {missing}（Manage app → Settings → Secrets を確認）")
        return False

    token = st.secrets["GITHUB_TOKEN"]
    owner = st.secrets["GITHUB_OWNER"]
    repo = st.secrets["GITHUB_REPO"]
    path = remote_path

    url = f"{api_base}/repos/{owner}/{repo}/contents/{path}"
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
        committer_name = st.session_state.get("current_user", "Streamlit App")
        committer_email = st.secrets.get("COMMITTER_EMAIL", "noreply@example.com")

        payload = {
            "message": f"{commit_message} ({ts})",
            "content": content_b64,
            "branch": branch,
            "committer": {"name": committer_name, "email": committer_email},
        }
        if latest_sha:
            payload["sha"] = latest_sha

        put = requests.put(url, headers=headers, json=payload, timeout=20)
        if debug:
            st.write({"PUT_status": put.status_code, "PUT_text": put.text[:500]})

        # 429: 一度だけ待機してリトライ
        if put.status_code == 429:
            wait = _retry_after(put.headers)
            time.sleep(wait)
            put = requests.put(url, headers=headers, json=payload, timeout=20)

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

def save_audit_to_github(debug: bool = False) -> bool:
    remote_audit = st.secrets.get("GITHUB_PATH_AUDIT")
    if not remote_audit:
        return True  # 設定がなければ成功扱い
    return save_to_github_file(AUDIT_PATH, remote_audit, "Update audit.csv from Streamlit app", debug=debug)

# ==============================
# 監査ログ（JSON構造）
# ==============================
import json

def write_audit(action: str, task_id: str, before: dict, after: dict):
    rec = {
        "ts": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
        "user": st.session_state.get("current_user", "unknown"),
        "action": action,              # "create" | "update" | "delete" | "delete_bulk" | "close"
        "task_id": task_id,
        "before": before or {},
        "after": after or {},
    }
    try:
        df_a = pd.read_csv(AUDIT_PATH, encoding="utf-8-sig")
    except FileNotFoundError:
        df_a = pd.DataFrame(columns=["ts", "user", "action", "task_id", "before", "after"])

    # CSVにはJSON文字列として保存
    rec_out = rec.copy()
    rec_out["before"] = json.dumps(rec["before"], ensure_ascii=False)
    rec_out["after"] = json.dumps(rec["after"], ensure_ascii=False)

    df_a = pd.concat([df_a, pd.DataFrame([rec_out])], ignore_index=True)
    df_a.to_csv(AUDIT_PATH, index=False, encoding="utf-8-sig")
    save_audit_to_github(debug=False)

# ==============================
# 日付表示ヘルパー
# ==============================
def _fmt_display(dt: pd.Timestamp) -> str:
    if pd.isna(dt):
        return "-"
    try:
        ts = pd.Timestamp(dt)
        if getattr(ts, "tzinfo", None) is not None:
            ts = ts.tz_localize(None)
        dt = ts
    except Exception:
        pass
    return dt.strftime("%Y-%m-%d %H:%M:%S" if SAVE_WITH_TIME else "%Y-%m-%d")

# ==============================
# データ読み込み
# ==============================
df = load_tasks()
df_by_id = df.set_index("ID")

# ==============================
# 簡易ログイン（トークン方式）
# ==============================
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

# ==============================
# 手動リフレッシュ
# ==============================
def _do_refresh():
    st.cache_data.clear()
    st.rerun()

st.sidebar.button("最新を読み込む", on_click=_do_refresh)

# ==============================
# サイドバー・フィルター
# ==============================
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

# ==============================
# サマリー
# ==============================
total = len(df)
status_counts = df["対応状況"].value_counts()

# 返信待ち系検出
def contains_any_ci(s: pd.Series, keywords: list[str]) -> pd.Series:
    if s.dtype != "object":
        s = s.astype(str)
    mask = pd.Series(False, index=s.index)
    for k in keywords:
        if not k:
            continue
        mask = mask | s.str.contains(k, case=False, na=False, regex=False)
    return mask

reply_mask = contains_any_ci(df["次アクション"], REPLY_KEYWORDS) | contains_any_ci(df["備考"], REPLY_KEYWORDS)
reply_count = int(df[reply_mask].shape[0])

col1, col2, col3, col4 = st.columns(4)
col1.metric("総タスク数", total)
col2.metric("対応中", int(status_counts.get("対応中", 0)))
col3.metric("クローズ", int(status_counts.get("クローズ", 0)))
col4.metric("返信待ち系", reply_count)

# ==============================
# 一覧（通常表 / Excel風 切替）
# ==============================
st.subheader("一覧")
view_mode = st.radio("表示モード", ["通常表", "Excel風"], horizontal=True, key="list_view_mode")

# 表示で使う列の基本順
PREFERRED_ORDER = ["更新日", "対応状況", "タスク", "更新者", "次アクション", "備考", "ソース", "ID"]

# 共通の前処理
_disp_base = view_df.copy()
if "起票日" in _disp_base.columns:
    _disp_base["起票日"] = _disp_display = _disp_base["起票日"].apply(_fmt_display)
if "更新日" in _disp_base.columns:
    _disp_base["更新日"] = _disp_base["更新日"].apply(_fmt_display)
_cols_present = [c for c in PREFERRED_ORDER if c in _disp_base.columns]
disp = _disp_base[_cols_present].copy()

from pandas import Timestamp, Timedelta
_now_ts = Timestamp(now_jst()).tz_localize(None)
_seven_days_ago = _now_ts - Timedelta(days=7)

def _build_tooltips(df: pd.DataFrame, long_cols: list[str]) -> pd.DataFrame:
    # Styler.set_tooltips は表と同形のDataFrameを要求
    tips = pd.DataFrame("", index=df.index, columns=df.columns)
    for c in long_cols:
        if c in df.columns:
            tips[c] = df[c].astype(str)
    return tips

if view_mode == "通常表":
    def _row_style(r: pd.Series) -> list[str]:
        # 7日超古い更新：行背景オレンジ / 返信待ち：左端アクセント
        styles = [''] * len(r)
        try:
            upd = pd.to_datetime(r.get("更新日"), errors="coerce")
            if getattr(upd, "tzinfo", None) is not None:
                upd = upd.tz_localize(None)
            if pd.notna(upd) and upd < _seven_days_ago:
                styles = ['background-color: rgba(255,165,0,0.08)'] * len(r)
        except Exception:
            pass
        text = f"{r.get('次アクション','')} {r.get('備考','')}"
        if isinstance(text, str) and any(k and (k.lower() in text.lower()) for k in REPLY_KEYWORDS):
            if len(styles) > 0:
                styles[0] = styles[0] + '; border-left: 4px solid #ffa94d;'
        return styles

    disp_sorted = disp.sort_values("更新日", ascending=False)

    # テーブルの余白とフォントの軽調整
    st.markdown("""
    <style>
    [data-testid="stTable"] tbody td, [data-testid="stTable"] thead th {
        padding: 0.4rem 0.6rem;
        vertical-align: top;
        font-size: 0.95rem;
    }
    </style>
    """, unsafe_allow_html=True)

    LONG_COLS = [c for c in ["タスク", "次アクション", "備考"] if c in disp_sorted.columns]
    sty = disp_sorted.style.set_properties(subset=LONG_COLS, **{
        "white-space": "normal",
        "line-height": "1.3",
        "max-width": "42rem",
        "word-wrap": "break-word",
    })
    # ツールチップ（全文）
    try:
        tips = _build_tooltips(disp_sorted, LONG_COLS)
        sty = sty.set_tooltips(tips)
    except Exception:
        pass

    sty = sty.apply(_row_style, axis=1).hide(axis="index")
    st.write(sty)

elif view_mode == "Excel風":
    # 返信待ちサイン列（進捗）
    def _reply_flag(row):
        text = f"{row.get('次アクション','')} {row.get('備考','')}"
        return any(k and (k.lower() in str(text).lower()) for k in REPLY_KEYWORDS)

    disp_x = disp.copy()
    disp_x.insert(1, "進捗", disp_x.apply(_reply_flag, axis=1).map({True: "⚑ 返信待ち", False: ""}))

    ORDER_X = ["更新日", "進捗", "対応状況", "更新者", "タスク", "次アクション", "備考", "ソース", "ID"]
    cols_x = [c for c in ORDER_X if c in disp_x.columns]
    disp_x = disp_x[cols_x]

    # URLはリンク表示、それ以外は青下線の擬似リンク
    def _as_linkish(v: str) -> str:
        s = (v or "").strip()
        if s.startswith("http://") or s.startswith("https://"):
            return f'{s}{s}</a>'
        return f'<span class="excel-link">{s}</span>' if s else ""

    if "ソース" in disp_x.columns:
        disp_x["ソース"] = disp_x["ソース"].apply(_as_linkish)

    # ステータス色ピル
    def _status_pill(v: str) -> str:
        v = (v or "").strip()
        mapping = {"未対応": "stat-未対応", "対応中": "stat-対応中", "クローズ": "stat-クローズ"}
        cls = mapping.get(v, "stat-未対応")
        return f'<span class="status-pill {cls}">{v}</span>'

    if "対応状況" in disp_x.columns:
        disp_x["対応状況"] = disp_x["対応状況"].apply(_status_pill)

    if "更新日" in disp_x.columns:
        disp_x = disp_x.sort_values("更新日", ascending=False)

    # CSS（ヘッダ水色、交互バンド、リンク色、ピル、左アクセント）
    st.markdown("""
    <style>
    [data-testid="stTable"] table {
        border-collapse: separate !important;
        border-spacing: 0;
        font-size: 0.95rem;
    }
    [data-testid="stTable"] thead th {
        background: #dbe7f3;
        color: #0f2940;
        font-weight: 600;
        padding: 8px 10px;
        border-bottom: 2px solid #b7cee3;
        position: sticky;
        top: 0;
        z-index: 1;
    }
    [data-testid="stTable"] tbody td {
        padding: 8px 10px;
        vertical-align: top;
        white-space: normal;
        line-height: 1.35;
        word-wrap: break-word;
        border-bottom: 1px solid #e0e6eb;
    }
    [data-testid="stTable"] tbody tr:nth-child(odd) td {
        background: #fafbfc;
    }
    .excel-link {
        color: #1a73e8;
        text-decoration: underline;
        cursor: pointer;
        word-break: break-all;
    }
    .status-pill {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 999px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    .stat-未対応 { background:#fde68a; color:#5b3e00; }
    .stat-対応中 { background:#c7f8d2; color:#14532d; }
    .stat-クローズ { background:#e5e7eb; color:#111827; }
    </style>
    """, unsafe_allow_html=True)

    def _row_excel_style(r: pd.Series) -> list[str]:
        styles = [''] * len(r)
        if r.get("進捗", ""):
            # 左端セルのみアクセント（オレンジ）
            if styles:
                styles[0] = styles[0] + "border-left: 6px solid #ffb65c;"
        return styles

    sty = disp_x.style.apply(_row_excel_style, axis=1).hide(axis="index").format(na_rep="", escape=False)
    long_cols_x = [c for c in ["タスク", "次アクション", "備考"] if c in disp_x.columns]
    if long_cols_x:
        sty = sty.set_properties(subset=long_cols_x, **{"max-width": "48rem"})
    st.write(sty)

# フィルタ後データのダウンロード
csv_bytes = disp.to_csv(index=False).encode("utf-8-sig")
st.download_button("この一覧（フィルタ済）をCSVでダウンロード", data=csv_bytes, file_name="tasks_filtered.csv", mime="text/csv")

# ==============================
# クローズ候補
# ==============================
st.subheader("クローズ候補（ルール: 対応中かつ返信待ち系、更新が7日以上前）")

now_ts = pd.Timestamp(now_jst()).tz_localize(None)
threshold_dt = now_ts - pd.Timedelta(days=7)

reply_mask_all = contains_any_ci(df["次アクション"], REPLY_KEYWORDS) | contains_any_ci(df["備考"], REPLY_KEYWORDS)
in_progress = df[df["対応状況"].eq("対応中")]
closing_candidates = in_progress[in_progress.index.isin(df[reply_mask_all].index)].copy()

closing_candidates["更新日"] = pd.to_datetime(closing_candidates["更新日"], errors="coerce")
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
    st.dataframe(show.sort_values("更新日"), use_container_width=True, hide_index=True)

    df_by_id = df.set_index("ID")
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

# ==============================
# 新規追加
# ==============================
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
        if not task.strip():
            st.error("タスク（件名）は必須です。")
        else:
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

# ==============================
# 編集・削除
# ==============================
st.subheader("タスク編集・削除（1件を選んで安全に更新／削除）")

if len(df) == 0:
    st.info("編集対象のタスクがありません。まずは追加してください。")
else:
    df_by_id = df.set_index("ID")
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

        fixed_assignees_e = st.secrets.get("FIXED_OWNERS", ["都筑", "二上", "三平", "成瀬", "柿野", "花田", "武藤", "島浦"])
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
        if not task_e.strip():
            st.error("タスク（件名）は必須です。")
        else:
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

# ==============================
# 一括削除
# ==============================
st.subheader("一括削除（複数選択）")
del_targets = st.multiselect(
    "削除したいタスク（複数選択）",
    options=view_df["ID"].tolist() if "ID" in view_df.columns else [],
    format_func=lambda _id: f'{df_by_id.loc[_id,"タスク"]} / {df_by_id.loc[_id,"更新者"]} / {_fmt_display(df_by_id.loc[_id,"更新日"])}' if _id in df_by_id.index else _id
)
confirm_word_bulk = st.text_input("確認ワード（DELETE と入力）", value="", key="confirm_bulk")
if st.button("選択タスクを削除", disabled=(len(del_targets) == 0)):
    if confirm_word_bulk.strip().upper() == "DELETE":
        before_map = {tid: df_by_id.loc[tid, ["タスク","対応状況","更新者","次アクション","備考","ソース"]].to_dict() for tid in del_targets if tid in df_by_id.index}
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

# ==============================
# サイドバー：手動保存＆診断
# ==============================
colA, colB = st.sidebar.columns(2)
if colA.button("GitHubへ手動保存"):
    ok = save_to_github_csv(debug=False)
    if ok:
        st.sidebar.success("GitHubへ保存完了")
    else:
        st.sidebar.error("GitHub保存失敗")
if colB.button("GitHub保存の診断"):
    save_to_github_csv(debug=True)

if DEBUG_MODE:
    st.sidebar.caption(f"Secrets keys: {list(st.secrets.keys())}")

# ==============================
# フッター
# ==============================
st.caption("※ 起票日は新規作成時のみ自動セットし、以後は編集不可（既存値維持）。更新日は編集/クローズ操作でJSTの“いま”に自動更新。GitHub連携はGET→PUTで保存します。")
