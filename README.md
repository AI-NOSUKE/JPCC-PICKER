# JPCC Picker

ABEJA-CC-JA（Common Crawl 日本語版）の JSONL（`.jsonl` / `.jsonl.gz`）から、条件に合うテキストを抽出して CSV に保存するシンプルなツールです。

- ✅ 匿名アクセス（AWS 認証不要 / `UNSIGNED`）
- ✅ `.gz` / 非 gz の両方に対応
- ✅ 複数ファイルを自動処理（Paginator）
- ✅ 進捗表示（`[STAT] lines=... hits=...` を上書き表示）
- ✅ 大規模データでも落ちにくい（タイムアウト / リトライ設定）

---

## 要件

- Python 3.9+
- 依存: `boto3`

```bash
pip install boto3
```

---

## 使い方

1. `jpcc_picker.py` の先頭 **ユーザー設定** を必要に応じて変更
2. 実行

### macOS / Linux

```bash
python jpcc_picker.py
```

### Windows（PowerShell）

```powershell
python .\jpcc_picker.py
```

> Excel で開く場合は、CSV を `utf-8-sig`（BOM付き）にすると文字化けしにくいです。必要なら `open(..., encoding="utf-8-sig")` に変更してください。

---

## ユーザー設定

```python
# ===== ユーザー設定 =====
OUTFILE = "output.csv"         # 出力ファイル名
KEYWORD = "ももクロ"           # 抽出したいキーワード
MINL, MAXL = 100, 2000         # 最小・最大文字数
LIMIT = 2000                   # 抽出件数（allモード時は無視）
CHUNK_SIZE = 10 * 1024 * 1024  # 非gzのチャンクサイズ（10MB）
MODE = "simple"                # "simple" / "random" / "all"
# ========================
```

### MODE の違い

- `simple`：ヒット順に **LIMIT 件** 到達で終了（最速）
- `all`：条件に合うものを **全件** 出力（LIMIT 無視）
- `random`：**全件を走査後**、リザーバサンプリングで LIMIT 件を出力（公平なサンプリング）

### 進捗表示の見方

- `lines`：読み込んだレコード数
- `hits`：条件に合致した件数
- 例：`[STAT] lines=120,000 hits=18/2000`

---

## FAQ

**Q. randomモードは時間がかかる？**  
A. 公平性のため **全スキャン**します。進捗（lines）は最後に全行数まで伸びます。

**Q. NoSuchKey エラーは？**  
A. 出ません。S3のファイル一覧をページング列挙して処理するため。

**Q. ネットワーク切れ対策は？**  
A. boto3 のタイムアウト・リトライを有効化済み。改善しない場合は再実行してください。

**Q. 進捗をもっと細かく見たい**  
A. コードの `LOG_INTERVAL` を小さくしてください。
