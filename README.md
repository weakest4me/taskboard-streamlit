
# タスク管理ボード（Streamlit試作）

Excelの「タスク」シート構成（起票日／更新日／タスク／対応状況／更新者／次アクション／備考／ソース）をベースに、
ブラウザでフィルタ・追加・クローズ更新ができる簡易ボードです。

## 使い方
1. Python 3.10+ を用意し、必要パッケージをインストール：
   ```bash
   pip install streamlit pandas
   ```
2. このフォルダで起動：
   ```bash
   streamlit run app.py
   ```
3. ブラウザで表示されたUIからフィルタ／追加／クローズ更新を行います。

## 注意点
- データは `tasks.csv` に保存します（UTF-8）。複数人同時編集は想定していないため、実運用はクラウドDBやSharePointを推奨。
- クローズ候補は「対応中」かつ「返信待ち系キーワード含む」かつ「更新が7日以上前」を自動抽出します。

## 次の一手（本番化案）
- **データ基盤**：SharePoint List / Dataverse / Azure SQL / Supabase
- **認証**：Microsoft Entra ID（Azure AD）
- **業務ルール**：Power Automateで未返信1週間の自動催促／自動クローズ案内
- **ダッシュボード**：Power BIで担当者別／カテゴリ別推移を可視化
