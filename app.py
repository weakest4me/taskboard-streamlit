
# -*- coding: utf-8 -*-
"""
タスク管理ボード（完全版 / 複数人運用向け / タイムゾーン安全化 / UI大幅改善 + 一覧の可読性強化）

機能要約:
- CSV 永続化 + GitHub 連携（SHA 楽観的ロック / 成否でUI分岐 / committer情報）
- 起票日は自動・編集不可、更新日は編集/クローズ時に自動更新（JST）
- 簡易ログイン（Secrets USERS によるトークン方式）
- 監査ログ（audit.csv）: 作成 / 更新 / 削除 / 一括削除 / クローズ を記録（任意で GitHub 保存）
- 一覧フィルタ（サイドバー）＋ クイックフィルタ（ページ内）
- クローズ候補抽出（対応中 & 返信待ち系 & 7日以上未更新）
- メトリクス + 棒グラフ
- UI改善（タブ化 / ColumnConfig 書式 / ステータス絵文字 / 軽CSS）
- 一覧の可読性強化（本ファイルの新要素）
  * セルの折り返し / 最適幅 / 行間拡大
  * 左2列（対応状況/タスク）の固定（CSSベース）
  * 表示モード切替：高速 or 行ハイライト or 行＋キーワード強調（Styler）
  * 状態別（未対応/対応中/クローズ）＋返信待ちの淡色行ハイライト
  * （任意）セル内のキーワード強調

注意:
- Secrets の SAVE_WITH_TIME は "true/false/1/0/yes/no/on/off" を解釈。
- GitHub 連携は GITHUB_* が必要。監査ログも保存するなら GITHUB_PATH_AUDIT を設定。
"""

import uuid
import base64
import re
from datetime import datetime, date
from zoneinfo import ZoneInfo

import streamlit as st
import pandas as pd
import requests

# ==============================
#       安全なブールパーサー
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

# ==============================
#       設定 / 定数
# ==============================
AUDIT_PATH = st.secrets.get("AUDIT_PATH", "audit.csv")
CSV_PATH = st.secrets.get("CSV_PATH", "tasks.csv")
LOCK_PATH = st.secrets.get("LOCK_PATH", "locks.csv")  # 予約（将来用）
LOCK_TTL_MIN = int(st.secrets.get("LOCK_TTL_MIN", 10))

JST = ZoneInfo("Asia/Tokyo")
SAVE_WITH_TIME = get_bool_secret("SAVE_WITH_TIME", True)

MANDATORY_COLS = [
    "ID", "起票日", "更新日", "タスク", "対応状況", "更新者", "次アクション", "備考", "ソース",
]

MISSING_SET = {"", "none", "null", "nan", "na", "n/a", "-", "—"}

# ==============================
#       ページ設定 / CSS
# ==============================
st.set_page_config(page_title="タスク管理ボード（完全版）", layout="wide")
st.title("タスク管理ボード（完全版 / 起票日は自動・編集不可、更新者はプルダウン）")

def inject_base_css():
    """ベースの可読性向上（文字サイズ/行間・セル折り返し・行高）"""
    st.markdown(
        """
        <style>
        /* DataFrameの文字サイズ・行間 */
        .stDataFrame table { font-size: 0.95rem; }
        .st-emotion-cache-1gulkj5 p { line-height: 1.35; }

        /* セルを折り返し可能に（一覧の長文対策） */
        [data-testid="stDataFrame"] div[role="gridcell"] div {
            white-space: normal !important;
            line-height: 1.35;
        }

        /* 行高（読みやすい行間へ） */
        [data-testid="stDataFrame"] table tbody tr td { padding-top: 10px; padding-bottom: 10px; }
        [data-testid="stDataFrame"] table thead tr th { padding-top: 10px; padding-bottom: 10px; }

        .stMetric label { font-size: 0.9rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )

def inject_sticky_css(first_col_width_px: int = 110, second_col_offset_px: int = 110):
    """
    簡易的な左2列固定（対応状況/タスク）。CSS だけで実現（環境により効かない場合あり）。
    first_col_width_px と second_col_offset_px は実表示に合わせて微調整可。
    """
    st.markdown(
        f"""
        <style>
        /* 1列目（対応状況）を固定 */
        [data-testid="stDataFrame"] table tbody tr td:nth-child(1),
        [data-testid="stDataFrame"] table thead tr th:nth-child(1) {{
            position: sticky; left: 0px; z-index: 3;
            background: var(--background-color);
        }}
        /* 2列目（タスク）を固定 */
        [data-testid="stDataFrame"] table tbody tr td:nth-child(2),
        [data-testid="stDataFrame"] table thead tr th:nth-child(2) {{
            position: sticky; left: {second_col_offset_px}px; z-index: 3;
            background: var(--background-color);
        }}
        /* 1列目の幅を目安として指定（表ヘッダのレイアウトと合わせる） */
        [data-testid="stDataFrame"] table thead tr th:nth-child(1) {{ min-width: {first_col_width_px}px; }}
        </style>
        """,
        unsafe_allow_html=True,
    )

inject_base_css()

# ==============================
#       時刻ヘルパー
# ==============================
def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    fmt = "%Y-%m-%d %H:%M:%S" if SAVE_WITH_TIME else "%Y-%m-%d"
    return now_jst().strftime(fmt)

def today_jst() -> date:
    return now_jst().date()

# ==============================
#       文字/欠損ユーティリティ
# ==============================
def _ensure_str(x) -> str:
    return "" if x is None else str(x)

def _is_missing(x) -> bool:
    s = _ensure_str(x).strip().lower()
    return s in MISSING_SET

# ==============================
#       データ正規化
# ==============================
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

    # ID 正規化（空/重複を解消）
    df["ID"] = df["ID"].astype(str).replace({"nan": "", "None": ""})
    mask_empty = df["ID"].str.strip().eq("")
    if mask_empty.any():
        df.loc[mask_empty, "ID"] = [str(uuid.uuid4()) for _ in range(mask_empty.sum())]
    dup_mask = df["ID"].duplicated(keep="first")
    if dup_mask.any():
        df.loc[dup_mask, "ID"] = [str(uuid.uuid4()) for _ in range(dup_mask.sum())]

    # 文字列列の正規化
    for col in ["タスク", "対応状況", "更新者", "次アクション", "備考", "ソース"]:
        df[col] = df[col].apply(lambda x: "" if _is_missing(x) else _ensure_str(x))

    # 日付列
    for col in ["起票日", "更新日"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    return df.reset_index(drop=True)

# ==============================
#       日付の安全弁
# ==============================
def safety_autofill_all(df: pd.DataFrame) -> pd.DataFrame:
    now_ts = pd.Timestamp(now_jst())
    # 起票日は欠損のみ補完
    df["起票日"] = df["起票日"].apply(
        lambda x: now_ts if pd.isna(pd.to_datetime(x, errors="coerce")) else pd.to_datetime(x, errors="coerce")
    )
    # 更新日は欠損なら補完
    df["更新日"] = df["更新日"].apply(
        lambda x: now_ts if pd.isna(pd.to_datetime(x, errors="coerce")) else pd.to_datetime(x, errors="coerce")
    )
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

# ==============================
#       CSV ロード/保存
# ==============================
@st.cache_data(ttl=10)
def load_tasks() -> pd.DataFrame:
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    except FileNotFoundError:
        df = pd.DataFrame(columns=MANDATORY_COLS)
    df = _normalize_df(df)
    df = safety_autofill_all(df)
    return df

def save_tasks(df: pd.DataFrame):
    """保存前に安全弁をかけ、CSVへ書き出し"""
    df_out = safety_autofill_all(df.copy())
    for col in ["起票日", "更新日"]:
        df_out[col] = pd.to_datetime(df_out[col], errors="coerce").apply(format_ts)
    df_out.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

# ==============================
#       GitHub 連携
# ==============================
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

def save_audit_to_github(debug: bool = False) -> bool:
    remote_audit = st.secrets.get("GITHUB_PATH_AUDIT")
    if not remote_audit:
        return True
    return save_to_github_file(AUDIT_PATH, remote_audit, "Update audit.csv from Streamlit app", debug=debug)

# ==============================
#       監査ログ
# ==============================
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

# ==============================
#       表示ユーティリティ
# ==============================
def status_badge(s: str) -> str:
    mapping = {"未対応": "⏳ 未対応", "対応中": "🚧 対応中", "クローズ": "✅ クローズ"}
    return mapping.get(str(s).strip(), str(s))

def make_display_df(df: pd.DataFrame) -> pd.DataFrame:
    """一覧表示用（列順・ステータス表記・URL整形・更新日降順）"""
    d = df.copy()
    d["対応状況"] = d["対応状況"].apply(status_badge)

    def to_link(x: str) -> str:
        s = str(x).strip()
        return s if s.startswith("http://") or s.startswith("https://") else s
    d["ソース"] = d["ソース"].apply(to_link)

    order = ["対応状況", "タスク", "更新者", "次アクション", "備考", "起票日", "更新日", "ソース", "ID"]
    for c in order:
        if c not in d.columns: d[c] = ""
    d = d[order].sort_values("更新日", ascending=False)
    return d

def style_rows(df_disp_like: pd.DataFrame, reply_mask: pd.Series):
    """
    状態（未対応/対応中/クローズ）＋返信待ちを淡色で行ハイライト。
    df_disp_like: make_display_df() 後の列構成を想定（先頭列が対応状況）
    """
    import numpy as np
    base = df_disp_like.copy()
    raw_status = base["対応状況"].astype(str)
    colors = np.full((len(base), len(base.columns)), "", dtype=object)

    def paint_row(i, color): colors[i, :] = f"background-color: {color}"

    for i, s in enumerate(raw_status):
        if "クローズ" in s: paint_row(i, "#ECF8EC")
        elif "対応中" in s: paint_row(i, "#EDF5FF")
        elif "未対応" in s: paint_row(i, "#FFF1F1")

    for i, wait in enumerate(reply_mask):
        if bool(wait): colors[i, :] = "background-color: #FFF7DB"  # 返信待ち優先

    return (
        base.style
        .set_properties(**{"font-size": "0.95rem"})
        .set_table_styles([{"selector": "th", "props": [("font-size", "0.9rem")]}])
        .apply(lambda _: colors, axis=None)
        .hide(axis="index")
    )

def style_cells_keyword(df_disp_like: pd.DataFrame, kw: str, target_cols=("タスク","次アクション","備考")):
    """
    target_cols に含まれるセルで kw を含む部分を強調（背景淡黄）。
    """
    base = df_disp_like.copy()
    # マスク作成
    mask = pd.DataFrame(False, index=base.index, columns=base.columns)
    if kw:
        pattern = re.escape(str(kw))
        for c in target_cols:
            if c in base.columns:
                mask[c] = base[c].astype(str).str.contains(pattern, na=False)

    styles = pd.DataFrame("", index=base.index, columns=base.columns)
    styles[mask] = "background-color: #FFF0B3;"

    return (
        base.style
        .set_properties(**{"font-size": "0.95rem"})
        .set_table_styles([{"selector": "th", "props": [("font-size", "0.9rem")]}])
        .apply(lambda _: styles, axis=None)
        .hide(axis="index")
    )

def _fmt_display(dt: pd.Timestamp) -> str:
    if pd.isna(dt): return "-"
    try:
        ts = pd.Timestamp(dt)
        if getattr(ts, "tzinfo", None) is not None: ts = ts.tz_localize(None)
        dt = ts
    except Exception: pass
    return dt.strftime("%Y-%m-%d %H:%M:%S" if SAVE_WITH_TIME else "%Y-%m-%d")

def compute_reply_mask(df_in: pd.DataFrame) -> pd.Series:
    rm = pd.Series(False, index=df_in.index)
    for k in ["返信待ち", "返信無し", "返信なし", "返信ない", "催促"]:
        rm = rm | df_in["次アクション"].str.contains(k, na=False) | df_in["備考"].str.contains(k, na=False)
    return rm

# ==============================
#       データ読み込み
# ==============================
df = load_tasks()
df_by_id = df.set_index("ID")

# ==============================
#       簡易ログイン
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
#       手動リフレッシュ
# ==============================
def _do_refresh():
    st.cache_data.clear()
    st.rerun()
st.sidebar.button("最新を読み込む", on_click=_do_refresh)

# ==============================
#       サイドバー・フィルター
# ==============================
st.sidebar.header("フィルター")
status_options = ["すべて"] + sorted(df["対応状況"].dropna().unique().tolist())
status_sel = st.sidebar.selectbox("対応状況", status_options)
assignees = sorted([a for a in df["更新者"].dropna().unique().tolist() if str(a).strip() != ""])
assignee_sel = st.sidebar.multiselect("担当者", assignees)
kw = st.sidebar.text_input("キーワード（タスク/備考/次アクション）")

filtered_df = df.copy()
if status_sel != "すべて":
    filtered_df = filtered_df[filtered_df["対応状況"] == status_sel]
if assignee_sel:
    filtered_df = filtered_df[filtered_df["更新者"].isin(assignee_sel)]
if kw:
    mask_kw = (
        filtered_df["タスク"].str.contains(kw, na=False, regex=False)
        | filtered_df["備考"].str.contains(kw, na=False, regex=False)
        | filtered_df["次アクション"].str.contains(kw, na=False, regex=False)
    )
    filtered_df = filtered_df[mask_kw]

# ==============================
#       サマリー + グラフ
# ==============================
total = len(df)
status_counts = df["対応状況"].value_counts()
reply_mask_all = compute_reply_mask(df)
reply_count = int(df[reply_mask_all].shape[0])

c1, c2, c3, c4 = st.columns(4)
c1.metric("総タスク数", total)
c2.metric("対応中", int(status_counts.get("対応中", 0)))
c3.metric("クローズ", int(status_counts.get("クローズ", 0)))
c4.metric("返信待ち系", reply_count)

st.bar_chart(status_counts.rename_axis("対応状況"), height=140, use_container_width=True)

# ==============================
#       タブ構成
# ==============================
tab_list, tab_close, tab_add, tab_edit, tab_del = st.tabs(
    ["📋 一覧", "✅ クローズ候補", "➕ 新規追加", "✏️ 編集・削除", "🗑️ 一括削除"]
)

# ColumnConfig（古い Streamlit では無いことがあるのでフォールバック）
try:
    from streamlit import column_config as cc
except Exception:
    cc = None

# ------------------------------
# 📋 一覧（可読性強化）
# ------------------------------
with tab_list:
    st.subheader("一覧")

    left, right = st.columns([2, 1])
    with left:
        quick = st.radio("クイックフィルタ", ["すべて", "未対応", "対応中", "クローズ"], horizontal=True)
    with right:
        show_sticky = st.toggle("左2列（状態/タスク）を固定", value=True)

    base = filtered_df.copy()
    if quick != "すべて":
        base = base[base["対応状況"] == quick]

    disp_raw = base.copy()  # 生
    disp = make_display_df(base)  # 表示用

    # 固定列CSS（環境により効かない場合あり）
    if show_sticky:
        # 1列目の幅（状態）はおよそ 110px を目安、タスク列はそれを基準にずらす
        inject_sticky_css(first_col_width_px=110, second_col_offset_px=110)

    # 表示モード切替
    mode = st.radio(
        "表示モード",
        ["高速（推奨）", "高可読：行ハイライト", "高可読：行ハイライト＋キーワード強調"],
        horizontal=True,
        help="件数が多い場合は『高速』を推奨。Stylerを使うモードは重くなることがあります。",
    )

    # 列幅/書式（ColumnConfig）
    df_kwargs = dict(use_container_width=True, hide_index=True, height=min(700, 100 + max(320, len(disp) * 34)))
    if cc is not None:
        COL_WIDTH = {"対応状況": 110, "更新者": 80, "ID": 220}
        def _cfg_text(label, width="medium", help_=""):
            # StreamlitのColumnConfig幅指定は "small/medium/large" が基本。px指定不可のため概ねの幅で調整。
            return cc.TextColumn(label, width=width, help=help_)
        def _cfg_date(label): return cc.DatetimeColumn(label, format="YYYY-MM-DD HH:mm", width="small")
        def _cfg_link(label): return cc.LinkColumn(label, display_text="リンク", width="small")

        df_kwargs["column_config"] = {
            "タスク": _cfg_text("タスク", width="large"),
            "次アクション": _cfg_text("次アクション", width="large"),
            "備考": _cfg_text("備考", width="large"),
            "対応状況": _cfg_text("対応状況", width="small"),
            "更新者": _cfg_text("更新者", width="small"),
            "起票日": _cfg_date("起票日"),
            "更新日": _cfg_date("更新日"),
            "ソース": _cfg_link("ソース"),
            "ID": _cfg_text("ID", width="medium", help_="内部ID"),
        }

    # 表示
    if mode == "高速（推奨）":
        st.dataframe(disp, **df_kwargs)

    elif mode == "高可読：行ハイライト":
        # 返信待ち判定は disp_raw の行順に合わせる
        rm = compute_reply_mask(disp_raw).reindex(disp.index)
        sty = style_rows(disp, rm)
        st.dataframe(sty, use_container_width=True, height=df_kwargs["height"])

    else:  # 行ハイライト + キーワード強調
        rm = compute_reply_mask(disp_raw).reindex(disp.index)
        # まず行色
        sty = style_rows(disp, rm)
        # さらにキーワード強調を上書き（対象セルのみ淡黄）
        if kw:
            sty_kw = style_cells_keyword(disp, kw)
            # pandas Styler は合成がやや難しいため、簡易的に「キーワード強調版」を別枠で表示
            st.caption("※ 行ハイライトに加えて、セル内のキーワードも淡黄で強調表示しています。")
            st.dataframe(sty_kw, use_container_width=True, height=df_kwargs["height"])
        else:
            st.dataframe(sty, use_container_width=True, height=df_kwargs["height"])

# ------------------------------
# ✅ クローズ候補
# ------------------------------
with tab_close:
    st.subheader("クローズ候補（対応中かつ返信待ち系、更新が7日以上前）")

    now_ts = pd.Timestamp(now_jst()).tz_localize(None)
    threshold_dt = now_ts - pd.Timedelta(days=7)

    in_progress = df[df["対応状況"].eq("対応中")]
    reply_df = df[reply_mask_all]
    closing_candidates = in_progress[in_progress.index.isin(reply_df.index)].copy()

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
        show = make_display_df(closing_candidates)
        df_kwargs2 = dict(use_container_width=True, hide_index=True, height=360)
        if cc is not None:
            df_kwargs2["column_config"] = {
                "起票日": cc.DatetimeColumn("起票日", format="YYYY-MM-DD HH:mm"),
                "更新日": cc.DatetimeColumn("更新日", format="YYYY-MM-DD HH:mm"),
                "ソース": cc.LinkColumn("ソース", display_text="リンク"),
            }
        st.dataframe(show, **df_kwargs2)

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

# ------------------------------
# ➕ 新規追加
# ------------------------------
with tab_add:
    st.subheader("新規タスク追加（起票日/更新日は自動でJSTの“いま”）")
    with st.form("add"):
        c1, c2, c3 = st.columns(3)
        c1.markdown(f"起票日: **{now_jst_str()}**")
        c2.markdown(f"更新日: **{now_jst_str()}**")
        status = c3.selectbox("対応状況", ["未対応", "対応中", "クローズ"], index=1)

        task = st.text_input("タスク（件名）")
        fixed_assignees = st.secrets.get("FIXED_OWNERS", ["都筑", "二上", "三平", "成瀬", "柿野", "花田", "武藤", "島浦"])
        ass_choices = sorted(set([a for a in df["更新者"].tolist() if str(a).strip() != ""] + list(fixed_assignees)))
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
            df2 = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            save_tasks(df2)
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

# ------------------------------
# ✏️ 編集・削除
# ------------------------------
with tab_edit:
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

            fixed_assignees_e = st.secrets.get("FIXED_OWNERS", ["都筑", "二上", "三平", "成瀬", "柿野", "花田", "武藤", "島浦"])
            ass_choices_e = sorted(set([a for a in df["更新者"].tolist() if str(a).strip() != ""] + list(fixed_assignees_e)))
            default_assignee = df_by_id.loc[choice_id, "更新者"]
            ass_index = ass_choices_e.index(default_assignee) if default_assignee in ass_choices_e else 0
            assignee_e = c3.selectbox("更新者（担当）", options=ass_choices_e, index=ass_index, key=f"assignee_{choice_id}")

            next_action_e = st.text_area("次アクション", df_by_id.loc[choice_id, "次アクション"], key=f"next_{choice_id}")
            notes_e = st.text_area("備考", df_by_id.loc[choice_id, "備考"], key=f"notes_{choice_id}")
            source_e = st.text_input("ソース（ID/リンクなど）", df_by_id.loc[choice_id, "ソース"], key=f"source_{choice_id}")

            st.caption(f"起票日: {_fmt_display(df_by_id.loc[choice_id, '起票日'])} / 最終更新: {_fmt_display(df_by_id.loc[choice_id, '更新日'])}")

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
                df2 = df[~df["ID"].eq(choice_id)].copy()
                save_tasks(df2)
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

# ------------------------------
# 🗑️ 一括削除
# ------------------------------
with tab_del:
    st.subheader("一括削除（複数選択）")
    del_targets = st.multiselect(
        "削除したいタスク（複数選択）",
        options=filtered_df["ID"].tolist(),
        format_func=lambda _id: f'{df_by_id.loc[_id,"タスク"]} / {df_by_id.loc[_id,"更新者"]} / {_fmt_display(df_by_id.loc[_id,"更新日"])}'
    )
    confirm_word_bulk = st.text_input("確認ワード（DELETE と入力）", value="", key="confirm_bulk")
    if st.button("選択タスクを削除", disabled=(len(del_targets) == 0)):
        if confirm_word_bulk.strip().upper() == "DELETE":
            before_map = {tid: df_by_id.loc[tid, ["タスク","対応状況","更新者","次アクション","備考","ソース"]].to_dict() for tid in del_targets}
            df2 = df[~df["ID"].isin(del_targets)].copy()
            save_tasks(df2)
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
#       サイドバー：手動保存＆診断
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

st.sidebar.caption(f"Secrets keys: {list(st.secrets.keys())}")

# ==============================
#       フッター
# ==============================
st.caption("※ 起票日は新規作成時のみ自動セットし、以後は編集不可（既存値維持）。更新日は編集/クローズ操作でJSTの“いま”に自動更新。GitHub連携はGET→PUTで保存します。")
