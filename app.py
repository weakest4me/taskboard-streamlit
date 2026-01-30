
# -*- coding: utf-8 -*-
"""
タスク管理ボード（完成版 / 複数人運用向け / セキュリティ&堅牢化）
- アトミックCSV書き込み、GitHub 422自動解消
- JST tz-aware & ISO8601 保存
- 簡易ログイン（共通/個別・ハッシュ対応）+ セッションTTL
- 簡易CSRF、監査ログ、バックアップ/復元
- 一覧フィルタ、クローズ候補、UI改善（行ハイライト/固定列/Styler）
"""
import os, re, base64, hmac, secrets, tempfile
from datetime import datetime, date
from zoneinfo import ZoneInfo
import pandas as pd
import requests
import streamlit as st

# ==============================
# 基本設定
# ==============================
JST = ZoneInfo("Asia/Tokyo")

# 安全なブール
def get_bool_secret(key: str, default: bool = True) -> bool:
    v = st.secrets.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1","true","yes","on"}
    return bool(v)

CSV_PATH  = st.secrets.get("CSV_PATH", "data/tasks.csv")
AUDIT_PATH= st.secrets.get("AUDIT_PATH", "data/audit.csv")
SAVE_WITH_TIME = get_bool_secret("SAVE_WITH_TIME", True)
SESSION_TTL_MIN = int(st.secrets.get("SESSION_TTL_MIN", 480))

# Streamlit page
st.set_page_config(page_title="タスク管理ボード（完成版）", layout="wide")
st.title("タスク管理ボード（完成版）")

# ==============================
# CSS
# ==============================
BASE_CSS = """
<style>
.stDataFrame table { font-size: 0.95rem; }
[data-testid="stDataFrame"] div[role="gridcell"] div { white-space: normal !important; line-height: 1.35; }
[data-testid="stDataFrame"] table tbody tr td, [data-testid="stDataFrame"] table thead tr th { padding-top: 10px; padding-bottom: 10px; }
</style>
"""
st.markdown(BASE_CSS, unsafe_allow_html=True)

def inject_sticky_css(first_col_width_px: int = 110, second_col_offset_px: int = 110):
    st.markdown(f"""
    <style>
    [data-testid="stDataFrame"] table tbody tr td:nth-child(1),
    [data-testid="stDataFrame"] table thead tr th:nth-child(1) {{ position: sticky; left:0px; z-index:3; background: var(--background-color); }}
    [data-testid="stDataFrame"] table tbody tr td:nth-child(2),
    [data-testid="stDataFrame"] table thead tr th:nth-child(2) {{ position: sticky; left:{second_col_offset_px}px; z-index:3; background: var(--background-color); }}
    [data-testid="stDataFrame"] table thead tr th:nth-child(1) {{ min-width: {first_col_width_px}px; }}
    </style>
    """, unsafe_allow_html=True)

# ==============================
# 時刻処理
# ==============================
now = lambda: datetime.now(JST)

def now_str():
    return now().strftime("%Y-%m-%d %H:%M:%S")

# ==============================
# セッション/CSRF/認証
# ==============================
app_secret = st.secrets.get("SECRET_KEY") or secrets.token_urlsafe(32)
st.session_state.setdefault("_app_secret", app_secret)

from werkzeug.security import check_password_hash

USERS = st.secrets.get("USERS", {})
APP_PASSWORD = st.secrets.get("APP_PASSWORD")
APP_PASSWORD_HASH = st.secrets.get("APP_PASSWORD_HASH")


def _issue_csrf():
    st.session_state.setdefault("_csrf", secrets.token_urlsafe(16))
    return st.session_state["_csrf"]


def _check_csrf(token: str) -> bool:
    expect = st.session_state.get("_csrf", "")
    return bool(token) and hmac.compare_digest(token, expect)


def _verify_password(raw: str, stored: str) -> bool:
    try:
        return check_password_hash(stored, raw)
    except Exception:
        return hmac.compare_digest(str(stored), str(raw))


def _is_authed() -> bool:
    ok = st.session_state.get("authed") is True
    if not ok:
        return False
    last = st.session_state.get("last_active")
    if not last:
        return False
    alive = (now() - last).total_seconds() <= (SESSION_TTL_MIN * 60)
    if alive:
        st.session_state["last_active"] = now()
    return alive


def render_login():
    st.sidebar.header("ログイン")
    token_input = st.sidebar.text_input("ログイン用パスワード/トークン", type="password")
    user_sel = st.sidebar.selectbox("ユーザー", ["(共通)"] + list(USERS.keys()))
    if st.sidebar.button("ログイン"):
        ok = False
        if user_sel == "(共通)":
            if APP_PASSWORD_HASH:
                ok = _verify_password(token_input, APP_PASSWORD_HASH)
            elif APP_PASSWORD is not None:
                ok = hmac.compare_digest(APP_PASSWORD, token_input)
        else:
            stored = USERS.get(user_sel)
            ok = _verify_password(token_input, stored) if stored else False
        if ok:
            st.session_state["authed"] = True
            st.session_state["current_user"] = user_sel if user_sel != "(共通)" else "shared"
            st.session_state["last_active"] = now()
            _issue_csrf()
            st.sidebar.success(f"{st.session_state['current_user']} としてログインしました")
        else:
            st.sidebar.error("認証に失敗しました。")

render_login()
if not _is_authed():
    st.stop()

# ==============================
# CSV ユーティリティ
# ==============================
MANDATORY_COLS = ["ID","起票日","更新日","タスク","対応状況","更新者","次アクション","備考","ソース"]
MISSING_SET = {"","none","null","nan","na","n/a","-","—"}

@st.cache_data(ttl=10)
def load_tasks() -> pd.DataFrame:
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    except FileNotFoundError:
        df = pd.DataFrame(columns=MANDATORY_COLS)
    # 正規化
    df.columns = [c.replace("　"," ").strip() for c in df.columns]
    for col in MANDATORY_COLS:
        if col not in df.columns:
            df[col] = ""
    # ID
    df["ID"] = df["ID"].astype(str).replace({"nan":"","None":""})
    mask_empty = df["ID"].str.strip().eq("")
    if mask_empty.any():
        import uuid
        df.loc[mask_empty, "ID"] = [str(uuid.uuid4()) for _ in range(mask_empty.sum())]
    # 文字列正規化
    for col in ["タスク","対応状況","更新者","次アクション","備考","ソース"]:
        df[col] = df[col].apply(lambda x: "" if str(x).strip().lower() in MISSING_SET else str(x))
    # 日付
    for col in ["起票日","更新日"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df.reset_index(drop=True)


def _atomic_write(path: str, data: bytes):
    d = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(d, exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False, dir=d) as tmp:
        tmp.write(data)
        tmp.flush()
        os.fsync(tmp.fileno())
        temp_name = tmp.name
    os.replace(temp_name, path)


def _format_ts(dt) -> str:
    if pd.isna(dt):
        ts = now()
    else:
        ts = pd.to_datetime(dt, errors="coerce")
        if pd.isna(ts):
            ts = now()
        elif getattr(ts, "tzinfo", None) is None:
            ts = ts.tz_localize(JST)
        else:
            ts = ts.tz_convert(JST)
    if SAVE_WITH_TIME:
        return ts.isoformat(timespec="seconds")
    return ts.strftime("%Y-%m-%d")


def save_tasks(df: pd.DataFrame):
    df_out = df.copy()
    for col in ["起票日","更新日"]:
        df_out[col] = pd.to_datetime(df_out[col], errors="coerce").apply(_format_ts)
    csv_bytes = df_out.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    _atomic_write(CSV_PATH, csv_bytes)

# ==============================
# GitHub 連携
# ==============================

def save_to_github_file(local_path: str, remote_path: str, commit_message: str, debug: bool=False) -> bool:
    req = ["GITHUB_TOKEN","GITHUB_OWNER","GITHUB_REPO"]
    missing = [k for k in req if k not in st.secrets]
    branch = st.secrets.get("GITHUB_BRANCH","main")
    if missing:
        st.warning("GitHub連携のSecretsが未設定です（省略可）")
        return True
    token = st.secrets["GITHUB_TOKEN"]; owner=st.secrets["GITHUB_OWNER"]; repo=st.secrets["GITHUB_REPO"]
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{remote_path}"
    headers = {"Authorization": f"Bearer {token}","Accept":"application/vnd.github+json","X-GitHub-Api-Version":"2022-11-28","User-Agent":"streamlit-app"}
    try:
        r = requests.get(url, headers=headers, params={"ref": branch}, timeout=20)
        if debug: st.write({"GET":r.status_code, "txt":r.text[:300]})
        latest_sha = r.json().get("sha") if r.status_code==200 else None
        with open(local_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode("utf-8")
        ts = now().strftime("%Y-%m-%d %H:%M:%S %Z")
        payload = {"message": f"{commit_message} ({ts})","content": content_b64,"branch": branch,"committer": {"name":"Streamlit App","email":"noreply@example.com"}}
        if latest_sha:
            payload["sha"] = latest_sha
        put = requests.put(url, headers=headers, json=payload, timeout=20)
        if debug: st.write({"PUT":put.status_code, "txt":put.text[:500]})
        if put.status_code in (200,201):
            st.toast("GitHubへ保存完了", icon="✅"); return True
        if put.status_code == 422:
            r2 = requests.get(url, headers=headers, params={"ref": branch}, timeout=20)
            if r2.status_code==200 and "sha" in r2.json():
                payload["sha"] = r2.json()["sha"]
                put2 = requests.put(url, headers=headers, json=payload, timeout=20)
                if put2.status_code in (200,201):
                    st.toast("競合解消して保存完了", icon="✅"); return True
            st.warning("GitHub競合：最新読み込み後に再保存してください。")
            return False
        if put.status_code == 401:
            st.error("401 Unauthorized: トークンを確認してください"); return False
        if put.status_code == 403:
            st.error("403 Forbidden: PAT権限やブランチ保護を確認"); return False
        st.error(f"GitHub保存失敗: {put.status_code}"); return False
    except Exception as e:
        st.error(f"GitHub保存中に例外: {e}")
        return False


def save_to_github_csv(debug: bool=False) -> bool:
    remote = st.secrets.get("GITHUB_PATH");
    if not remote: return True
    return save_to_github_file(CSV_PATH, remote, "Update tasks.csv from Streamlit", debug)


def save_audit_to_github(debug: bool=False) -> bool:
    remote = st.secrets.get("GITHUB_PATH_AUDIT");
    if not remote: return True
    return save_to_github_file(AUDIT_PATH, remote, "Update audit.csv from Streamlit", debug)

# ==============================
# 監査ログ
# ==============================

def write_audit(action: str, task_id: str, before: dict, after: dict):
    rec = {"ts": now().strftime("%Y-%m-%d %H:%M:%S"),"user": st.session_state.get("current_user","unknown"),"action": action,"task_id": task_id,"before": str(before or {}),"after": str(after or {})}
    try:
        df_a = pd.read_csv(AUDIT_PATH, encoding="utf-8-sig")
    except FileNotFoundError:
        df_a = pd.DataFrame(columns=rec.keys())
    df_a = pd.concat([df_a, pd.DataFrame([rec])], ignore_index=True)
    _atomic_write(AUDIT_PATH, df_a.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"))
    save_audit_to_github(False)

# ==============================
# 表示ヘルパ
# ==============================

def status_badge(s: str) -> str:
    mapping = {"未対応":"⏳ 未対応","対応中":"🚧 対応中","クローズ":"✅ クローズ"}
    return mapping.get(str(s).strip(), str(s))


def make_display_df(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["対応状況"] = d["対応状況"].apply(status_badge)
    order = ["対応状況","タスク","更新者","次アクション","備考","起票日","更新日","ソース","ID"]
    for c in order:
        if c not in d.columns: d[c] = ""
    d = d[order].sort_values("更新日", ascending=False)
    return d


def compute_reply_mask(df_in: pd.DataFrame) -> pd.Series:
    rm = pd.Series(False, index=df_in.index)
    for k in ["返信待ち","返信無し","返信なし","返信ない","催促"]:
        rm = rm | df_in["次アクション"].astype(str).str.contains(k, na=False) | df_in["備考"].astype(str).str.contains(k, na=False)
    return rm

# ==============================
# データ読み込み
# ==============================

df = load_tasks()
df_by_id = df.set_index("ID") if len(df)>0 else pd.DataFrame().set_index(pd.Index([]))

# ==============================
# サイドバー共通
# ==============================

def _do_refresh():
    st.cache_data.clear(); st.rerun()

st.sidebar.button("最新を読み込む", on_click=_do_refresh)

# バックアップ/復元
from io import BytesIO
import zipfile
with st.sidebar.expander("バックアップ / 復元"):
    if st.button("CSVをZIPでダウンロード"):
        buf = BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            if os.path.exists(CSV_PATH): z.write(CSV_PATH, arcname="tasks.csv")
            if os.path.exists(AUDIT_PATH): z.write(AUDIT_PATH, arcname="audit.csv")
        st.download_button("保存する", data=buf.getvalue(), file_name="tasks_backup.zip", mime="application/zip")
    up = st.file_uploader("復元する tasks.csv を選択", type=["csv"])
    if up and st.button("復元を実行（上書き）"):
        _atomic_write(CSV_PATH, up.read()); st.success("復元しました。再読込します。"); _do_refresh()

# ==============================
# フィルタ
# ==============================

st.sidebar.header("フィルター")
status_options = ["すべて"] + sorted(df["対応状況"].dropna().unique().tolist())
status_sel = st.sidebar.selectbox("対応状況", status_options)
assignees = sorted([a for a in df["更新者"].dropna().unique().tolist() if str(a).strip() != ""]) if len(df)>0 else []
assignee_sel = st.sidebar.multiselect("担当者", assignees)
kw = st.sidebar.text_input("キーワード（タスク/備考/次アクション）")

filtered_df = df.copy()
if status_sel != "すべて":
    filtered_df = filtered_df[filtered_df["対応状況"] == status_sel]
if assignee_sel:
    filtered_df = filtered_df[filtered_df["更新者"].isin(assignee_sel)]
if kw:
    mask_kw = (
        filtered_df["タスク"].astype(str).str.contains(kw, na=False, regex=False) |
        filtered_df["備考"].astype(str).str.contains(kw, na=False, regex=False) |
        filtered_df["次アクション"].astype(str).str.contains(kw, na=False, regex=False)
    )
    filtered_df = filtered_df[mask_kw]

# ==============================
# メトリクス & グラフ
# ==============================

total = len(df)
status_counts = df["対応状況"].value_counts() if len(df) else pd.Series(dtype=int)
reply_mask_all = compute_reply_mask(df) if len(df) else pd.Series(dtype=bool)
reply_count = int(df[reply_mask_all].shape[0]) if len(df) else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("総タスク数", total)
c2.metric("対応中", int(status_counts.get("対応中", 0)))
c3.metric("クローズ", int(status_counts.get("クローズ", 0)))
c4.metric("返信待ち系", reply_count)

if len(status_counts):
    st.bar_chart(status_counts.rename_axis("対応状況"), height=140, use_container_width=True)

# ==============================
# タブ
# ==============================

tab_list, tab_close, tab_add, tab_edit, tab_del = st.tabs(["📋 一覧","✅ クローズ候補","➕ 新規追加","✏️ 編集・削除","🗑️ 一括削除"])

# ColumnConfig（存在しない環境では None）
try:
    from streamlit import column_config as cc
except Exception:
    cc = None

MAX_STYLER_ROWS = int(st.secrets.get("MAX_STYLER_ROWS", 100))

# ------------------------------
# 📋 一覧
# ------------------------------
with tab_list:
    st.subheader("一覧")
    left, right = st.columns([2,1])
    with left:
        quick = st.radio("クイックフィルタ", ["すべて","未対応","対応中","クローズ"], horizontal=True)
    with right:
        show_sticky = st.toggle("左2列（状態/タスク）を固定", value=True)

    base = filtered_df.copy()
    if quick != "すべて":
        base = base[base["対応状況"] == quick]

    disp_raw = base.copy()
    disp = make_display_df(base)

    if show_sticky:
        inject_sticky_css(110,110)

    mode = st.radio("表示モード", ["高速（推奨）","高可読：行ハイライト"], horizontal=True)
    if len(disp) > MAX_STYLER_ROWS and mode != "高速（推奨）":
        st.info(f"行数が {len(disp)} 件のため『高速』に自動切替しました（閾値 {MAX_STYLER_ROWS}）。")
        mode = "高速（推奨）"

    df_kwargs = dict(use_container_width=True, hide_index=True, height=min(700, 100 + max(320, len(disp) * 34)))
    if cc is not None:
        def _cfg_text(label, width="medium", help_=""):
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

    if mode == "高速（推奨）":
        st.dataframe(disp, **df_kwargs)
    else:
        import numpy as np
        base2 = disp.copy()
        raw_status = base2["対応状況"].astype(str)
        colors = np.full((len(base2), len(base2.columns)), "", dtype=object)
        def paint_row(i,color): colors[i,:] = f"background-color: {color}"
        for i,s in enumerate(raw_status):
            if "クローズ" in s: paint_row(i, "#ECF8EC")
            elif "対応中" in s: paint_row(i, "#EDF5FF")
            elif "未対応" in s: paint_row(i, "#FFF1F1")
        rm = compute_reply_mask(disp_raw).reindex(disp.index)
        for i,wait in enumerate(rm):
            if bool(wait): colors[i,:] = "background-color: #FFF7DB"
        sty = (base2.style.set_properties(**{"font-size":"0.95rem"}).set_table_styles([{"selector":"th","props":[("font-size","0.9rem")]}]).apply(lambda _ : colors, axis=None).hide(axis="index"))
        st.dataframe(sty, use_container_width=True, height=df_kwargs["height"])

# ------------------------------
# ✅ クローズ候補
# ------------------------------
with tab_close:
    st.subheader("クローズ候補（対応中かつ返信待ち系、更新が7日以上前）")
    if len(df)==0:
        st.info("該当なし")
    else:
        now_naive = pd.Timestamp(now()).tz_localize(None)
        threshold_dt = now_naive - pd.Timedelta(days=7)
        in_progress = df[df["対応状況"].eq("対応中")]
        reply_df = df[compute_reply_mask(df)]
        closing_candidates = in_progress[in_progress.index.isin(reply_df.index)].copy()
        closing_candidates["更新日"] = pd.to_datetime(closing_candidates["更新日"], errors="coerce")
        try:
            if getattr(closing_candidates["更新日"].dt, "tz", None) is not None:
                closing_candidates["更新日"] = closing_candidates["更新日"].dt.tz_localize(None)
        except Exception:
            pass
        closing_candidates = closing_candidates[closing_candidates["更新日"].notna() & (closing_candidates["更新日"] < threshold_dt)]
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
            ids = st.multiselect("クローズするタスク", closing_candidates["ID"].tolist(), format_func=lambda _id: f"{df_by_id.loc[_id,'タスク']} / {df_by_id.loc[_id,'更新者']}")
            if ids:
                if st.button("選択タスクをクローズ"):
                    befores = {tid: df_by_id.loc[tid,["対応状況","更新日"]].to_dict() for tid in ids}
                    df.loc[df["ID"].isin(ids), "対応状況"] = "クローズ"
                    df.loc[df["ID"].isin(ids), "更新日"] = pd.Timestamp(now())
                    save_tasks(df)
                    ok = save_to_github_csv(False)
                    if ok:
                        for tid in ids:
                            write_audit("close", tid, befores.get(tid), {"対応状況":"クローズ","更新日": _format_ts(pd.Timestamp(now()))})
                        st.success(f"{len(ids)}件をクローズしました。")
                        st.cache_data.clear(); st.rerun()
                    else:
                        st.error("GitHub保存に失敗しました。")

# ------------------------------
# ➕ 新規追加
# ------------------------------
with tab_add:
    st.subheader("新規タスク追加（起票日/更新日はJSTの“いま”で自動）")
    import uuid
    with st.form("add"):
        csrf = _issue_csrf()
        st.text_input("csrf", value=csrf, type="password", label_visibility="collapsed", key="csrf_add")
        c1, c2, c3 = st.columns(3)
        c1.markdown(f"起票日: **{now_str()}**")
        c2.markdown(f"更新日: **{now_str()}**")
        status = c3.selectbox("対応状況", ["未対応","対応中","クローズ"], index=1)
        task = st.text_input("タスク（件名）")
        fixed_assignees = st.secrets.get("FIXED_OWNERS", ["都筑","二上","三平","成瀬","柿野","花田","武藤","島浦"]) 
        ass_choices = sorted(set([a for a in df["更新者"].tolist() if str(a).strip() != ""] + list(fixed_assignees))) if len(df)>0 else list(fixed_assignees)
        assignee = st.selectbox("更新者（担当）", options=ass_choices)
        next_action = st.text_area("次アクション")
        notes = st.text_area("備考")
        source = st.text_input("ソース（ID/リンクなど）")
        submit = st.form_submit_button("追加", type="primary")
        if submit:
            if not _check_csrf(st.session_state.get("csrf_add")): st.error("CSRF token mismatch"); st.stop()
            # 軽バリデーション
            errs = []
            if not task or len(task.strip())==0: errs.append("タスク（件名）は必須です。")
            if len(task) > 200: errs.append("タスクは200文字以内にしてください。")
            if source and (not re.match(r"^https?://", source)) and len(source)>80:
                errs.append("ソースがURLでない場合、80文字以内で記入してください。")
            if len(next_action)>2000: errs.append("次アクションは2000文字以内。")
            if len(notes)>2000: errs.append("備考は2000文字以内。")
            if errs:
                for e in errs: st.error(e)
                st.stop()
            now_ts = pd.Timestamp(now())
            new_row = {"ID": str(uuid.uuid4()), "起票日": now_ts, "更新日": now_ts, "タスク": task, "対応状況": status, "更新者": assignee, "次アクション": next_action, "備考": notes, "ソース": source}
            df2 = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            save_tasks(df2)
            ok = save_to_github_csv(False)
            if ok:
                write_audit("create", new_row["ID"], None, {k: (new_row[k] if k not in ["起票日","更新日"] else _format_ts(new_row[k])) for k in new_row.keys()})
                st.success("追加しました。")
                st.cache_data.clear(); st.rerun()
            else:
                st.error("GitHub保存に失敗しました。競合の可能性があります。")

# ------------------------------
# ✏️ 編集・削除
# ------------------------------
with tab_edit:
    st.subheader("タスク編集・削除（1件）")
    if len(df)==0:
        st.info("編集対象がありません。まずは追加してください。")
    else:
        choice_id = st.selectbox("編集対象", options=df_by_id.index.tolist(), format_func=lambda _id: f"[{df_by_id.loc[_id,'対応状況']}] {df_by_id.loc[_id,'タスク']} / {df_by_id.loc[_id,'更新者']}", key="selected_id")
        if choice_id not in df_by_id.index:
            st.warning("選択したIDが見つかりません。再読み込みします。"); st.cache_data.clear(); st.rerun()
        with st.form(f"edit_{choice_id}"):
            csrf = _issue_csrf(); st.text_input("csrf", value=csrf, type="password", label_visibility="collapsed", key=f"csrf_edit_{choice_id}")
            c1,c2,c3 = st.columns(3)
            task_e = c1.text_input("タスク（件名）", df_by_id.loc[choice_id, "タスク"])
            status_e = c2.selectbox("対応状況", ["未対応","対応中","クローズ"], index=( ["未対応","対応中","クローズ"].index(df_by_id.loc[choice_id,"対応状況"]) if df_by_id.loc[choice_id,"対応状況"] in ["未対応","対応中","クローズ"] else 1 ))
            fixed_assignees_e = st.secrets.get("FIXED_OWNERS", ["都筑","二上","三平","成瀬","柿野","花田","武藤","島浦"]) 
            ass_choices_e = sorted(set([a for a in df["更新者"].tolist() if str(a).strip() != ""] + list(fixed_assignees_e))) if len(df)>0 else list(fixed_assignees_e)
            default_assignee = df_by_id.loc[choice_id, "更新者"]
            ass_index = ass_choices_e.index(default_assignee) if default_assignee in ass_choices_e else 0
            assignee_e = c3.selectbox("更新者（担当）", options=ass_choices_e, index=ass_index)
            next_action_e = st.text_area("次アクション", df_by_id.loc[choice_id, "次アクション"]) 
            notes_e = st.text_area("備考", df_by_id.loc[choice_id, "備考"]) 
            source_e = st.text_input("ソース（ID/リンクなど）", df_by_id.loc[choice_id, "ソース"]) 
            st.caption(f"起票日: {_format_ts(df_by_id.loc[choice_id,'起票日'])} / 最終更新: {_format_ts(df_by_id.loc[choice_id,'更新日'])}")
            col_ok, col_sp, col_del = st.columns([1,1,1])
            submit_edit = col_ok.form_submit_button("更新する", type="primary")
            st.markdown("##### 削除（危険）")
            st.warning("元に戻せません。削除する場合、確認ワードに `DELETE` と入力してください。")
            confirm_word = st.text_input("確認ワード", value="", key=f"confirm_{choice_id}")
            delete_btn = col_del.form_submit_button("このタスクを削除")
        if submit_edit:
            if not _check_csrf(st.session_state.get(f"csrf_edit_{choice_id}")): st.error("CSRF token mismatch"); st.stop()
            before = df_by_id.loc[choice_id, ["タスク","対応状況","更新者","次アクション","備考","ソース"]].to_dict()
            df.loc[df["ID"]==choice_id, ["タスク","対応状況","更新者","次アクション","備考","ソース"]] = [task_e, status_e, assignee_e, next_action_e, notes_e, source_e]
            df.loc[df["ID"]==choice_id, "更新日"] = pd.Timestamp(now())
            save_tasks(df)
            ok = save_to_github_csv(False)
            if ok:
                write_audit("update", choice_id, before, {"タスク":task_e, "対応状況":status_e, "更新者":assignee_e, "次アクション":next_action_e, "備考":notes_e, "ソース":source_e})
                st.success("更新しました。"); st.cache_data.clear(); st.rerun()
            else:
                st.error("GitHub保存に失敗しました。")
        elif delete_btn:
            if confirm_word.strip().upper() == "DELETE":
                before = df_by_id.loc[choice_id, ["タスク","対応状況","更新者","次アクション","備考","ソース"]].to_dict()
                df2 = df[~df["ID"].eq(choice_id)].copy()
                save_tasks(df2)
                ok = save_to_github_csv(False)
                if ok:
                    write_audit("delete", choice_id, before, None)
                    st.success("削除しました。"); st.cache_data.clear(); st.rerun()
                else:
                    st.error("GitHub保存に失敗しました。")
            else:
                st.error("確認ワードが正しくありません。`DELETE` と入力してください。")

# ------------------------------
# 🗑️ 一括削除
# ------------------------------
with tab_del:
    st.subheader("一括削除（複数選択）")
    del_targets = st.multiselect("削除したいタスク", options=filtered_df["ID"].tolist(), format_func=lambda _id: f"{df_by_id.loc[_id,'タスク']} / {df_by_id.loc[_id,'更新者']}") if len(filtered_df) else []
    confirm_word_bulk = st.text_input("確認ワード（DELETE）", value="", key="confirm_bulk")
    if st.button("選択タスクを削除", disabled=(len(del_targets)==0)):
        if confirm_word_bulk.strip().upper() == "DELETE":
            before_map = {tid: df_by_id.loc[tid,["タスク","対応状況","更新者","次アクション","備考","ソース"]].to_dict() for tid in del_targets}
            df2 = df[~df["ID"].isin(del_targets)].copy()
            save_tasks(df2)
            ok = save_to_github_csv(False)
            if ok:
                for tid in del_targets:
                    write_audit("delete_bulk", tid, before_map.get(tid), None)
                st.success(f"{len(del_targets)}件を削除しました。")
                st.cache_data.clear(); st.rerun()
            else:
                st.error("GitHub保存に失敗しました。")

# ==============================
# サイドバー：GitHub保存
# ==============================
colA, colB = st.sidebar.columns(2)
if colA.button("GitHubへ手動保存"):
    ok = save_to_github_csv(False)
    if ok: st.sidebar.success("保存完了")
    else: st.sidebar.error("保存失敗")
if colB.button("GitHub保存の診断"):
    save_to_github_csv(True)

st.caption("※ 起票日は新規作成時のみ自動セット、更新日は編集/クローズでJSTの“いま”。GitHub は GET→PUT。")
