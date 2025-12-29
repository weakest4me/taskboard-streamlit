
import streamlit as st
import pandas as pd
from datetime import datetime
import uuid
from zoneinfo import ZoneInfo  # ★ 追加：タイムゾーン

# ===== タイムゾーン・ヘルパー =====
JST = ZoneInfo("Asia/Tokyo")

def now_jst() -> datetime:
    """現在日時（JST）"""
    return datetime.now(JST)

def today_jst():
    """今日（日付のみ, JST）"""
    return now_jst().date()

# ===== ページ設定 =====
st.set_page_config(page_title="タスク管理ボード", layout="wide")
st.title("タスク管理ボード（試作）")

CSV_PATH = "tasks.csv"
MANDATORY_COLS = ["ID", "起票日", "更新日", "タスク", "対応状況", "更新者", "次アクション", "備考", "ソース"]

# ===== ユーティリティ =====
def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """CSV読み込み後の型整備・欠損列追加（IDの空/重複も必ず解消）"""
    # 必須列が無ければ追加
    for col in MANDATORY_COLS:
        if col not in df.columns:
            df[col] = ""

    # --- ID 正規化（最重要） ---
    df["ID"] = df["ID"].astype(str)
    # 'nan' 文字を空扱い
    df["ID"] = df["ID"].replace({"nan": ""})
    # 空IDに UUID 付与
    mask_empty = df["ID"].str.strip().eq("")
    if mask_empty.any():
        df.loc[mask_empty, "ID"] = [str(uuid.uuid4()) for _ in range(mask_empty.sum())]
    # 重複IDは後続に新UUID
    dup_mask = df["ID"].duplicated(keep="first")
    if dup_mask.any():
        df.loc[dup_mask, "ID"] = [str(uuid.uuid4()) for _ in range(dup_mask.sum())]

    # 日付の型
    for col in ["起票日", "更新日"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # 文字列列
    for col in ["タスク", "対応状況", "更新者", "次アクション", "備考", "ソース"]:
        df[col] = df[col].astype(str)

    # 見た目用に index を連番（識別は ID を使用）
    return df.reset_index(drop=True)

@st.cache_data(ttl=30)
def load_tasks() -> pd.DataFrame:
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    except FileNotFoundError:
        df = pd.DataFrame(columns=MANDATORY_COLS)
    return _normalize_df(df)

def save_tasks(df: pd.DataFrame):
    df_out = df.copy()
    # 日付を ISO 保存（YYYY-MM-DD）
    for col in ["起票日", "更新日"]:
        df_out[col] = pd.to_datetime(df_out[col], errors="coerce").dt.strftime("%Y-%m-%d")
    df_out.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

# ===== データ読み込み =====
df = load_tasks()
df_by_id = df.set_index("ID")  # 以降は必ず ID で突き合わせ

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

reply_mask = False
for k in ["返信待ち", "返信無し", "返信なし", "返信ない", "催促"]:
    reply_mask = reply_mask | df["次アクション"].str.contains(k, na=False) | df["備考"].str.contains(k, na=False)
reply_count = int(df[reply_mask].shape[0])

col1, col2, col3, col4 = st.columns(4)
col1.metric("総タスク数", total)
col2.metric("対応中", int(status_counts.get("対応中", 0)))
col3.metric("クローズ", int(status_counts.get("クローズ", 0)))
col4.metric("返信待ち系", reply_count)

# ===== 一覧 =====
st.subheader("一覧")
st.dataframe(view_df.sort_values("更新日", ascending=False), use_container_width=True)


# ===== クローズ候補 =====
st.subheader("クローズ候補（ルール: 対応中かつ返信待ち系、更新が7日以上前）")
- now_ts = pd.Timestamp(now_jst())                # ★ JST 現在（tz-aware）
+ now_ts = pd.Timestamp(now_jst()).tz_localize(None)  # ★ JST 現在 → tz-naive に変換
threshold = now_ts - pd.Timedelta(days=7)
in_progress = df[df["対応状況"].str.contains("対応中", na=False)]
reply_df = df[reply_mask]
closing_candidates = in_progress[in_progress.index.isin(reply_df.index)]
closing_candidates = closing_candidates[closing_candidates["更新日"] < threshold]

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
        df.loc[df["ID"].isin(to_close_ids), "対応状況"] = "クローズ"
        df.loc[df["ID"].isin(to_close_ids), "更新日"] = pd.Timestamp(today_jst())  # ★ JSTの“今日”
        save_tasks(df)
        st.success(f"{len(to_close_ids)}件をクローズに更新しました。")
        st.cache_data.clear()
        st.rerun()

# ===== 新規追加 =====
st.subheader("新規タスク追加")
with st.form("add"):
    c1, c2, c3 = st.columns(3)
    created = c1.date_input("起票日", today_jst())  # ★ JST
    updated = c2.date_input("更新日", today_jst())  # ★ JST
    status = c3.selectbox("対応状況", ["未対応", "対応中", "クローズ"], index=1)

    task = st.text_input("タスク（件名）")
    ass_choices = sorted(set(df["更新者"].tolist() + ["都筑", "二上", "三平", "成瀬", "柿野", "花田", "武藤", "島浦"]))
    assignee = st.selectbox("更新者（担当）", options=ass_choices)

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
            "対応状況": status,
            "更新者": assignee,
            "次アクション": next_action,
            "備考": notes,
            "ソース": source,
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        save_tasks(df)
        st.success("追加しました。")
        st.cache_data.clear()
        st.rerun()

# ===== 編集・削除 =====
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

    # 存在ガード
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
            f"起票日: {df_by_id.loc[choice_id, '起票日'].strftime('%Y-%m-%d') if pd.notnull(df_by_id.loc[choice_id, '起票日']) else '-'} / "
            f"最終更新: {df_by_id.loc[choice_id, '更新日'].strftime('%Y-%m-%d') if pd.notnull(df_by_id.loc[choice_id, '更新日']) else '-'}"
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
        df.loc[df["ID"] == choice_id, "更新日"] = pd.Timestamp(today_jst())  # ★ JSTの“今日”
        save_tasks(df)
        st.success("タスクを更新しました。")
        st.cache_data.clear()
        st.rerun()

    elif delete_btn:
        if confirm_word.strip().upper() == "DELETE":
            df = df[~df["ID"].eq(choice_id)].copy()
            save_tasks(df)
            # 削除後に古い選択IDを参照しないようクリア
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
    format_func=lambda _id: f'{df_by_id.loc[_id,"タスク"]} / {df_by_id.loc[_id,"更新者"]} / '
                            f'{df_by_id.loc[_id,"更新日"].strftime("%Y-%m-%d") if pd.notnull(df_by_id.loc[_id,"更新日"]) else "-"}'
)
confirm_word_bulk = st.text_input("確認ワード（DELETE と入力）", value="", key="confirm_bulk")
if st.button("選択タスクを削除", disabled=(len(del_targets) == 0)):
    if confirm_word_bulk.strip().upper() == "DELETE":
        df = df[~df["ID"].isin(del_targets)].copy()
        save_tasks(df)
        st.success(f"{len(del_targets)}件のタスクを削除しました。")
        st.cache_data.clear()
        st.rerun()
    else:
        st.error("確認ワードが正しくありません。`DELETE` と入力してください。")

# ===== フッター =====
st.caption("※ この試作はローカルCSV保存です。複数人での同時編集には SharePoint/Dataverse/Database を推奨。")
