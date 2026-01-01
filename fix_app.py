#!/usr/bin/env python3
import re
from pathlib import Path

TARGET = Path('app.py')

if not TARGET.exists():
    print('app.py が見つかりません。現在のフォルダで実行してください。')
    raise SystemExit(1)

src = TARGET.read_text(encoding='utf-8', errors='ignore')

# 1) HTMLエンコードを生記号へ（順序が重要）
src = src.replace('&amp;', '&')
src = src.replace('&lt;', '<')
src = src.replace('&gt;', '>')
src = src.replace('-&gt;', '->')  # 念のため

# 2) ndef -> def （行頭/単語境界を広めに）
src = re.sub(r'(\b|\n)ndef\s+', 'def ', src)

# 3) バッククォートのみの行を削除
src = '\n'.join(line for line in src.splitlines() if not re.fullmatch(r"\s*`{1,3}\s*", line))

# 4) 予防：全角ハイフンや長音記号を半角に
src = src.replace('－>', '->').replace('ー>', '->')

backup = TARGET.with_suffix('.py.bak')
backup.write_text(TARGET.read_text(encoding='utf-8', errors='ignore'), encoding='utf-8')
TARGET.write_text(src, encoding='utf-8')

print('app.py を修正しました。バックアップ:', backup.name)
