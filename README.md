# JPCC-PICKER

**好きなキーワードで、ABEJA-CC-JA（Common Crawl 日本語版）のJSONLから文章を抽出**するシンプルツール。
設定はファイル冒頭の数項目だけ。ローカル/テスト/.env はありません。

## 使い方
```bash
pip install boto3
python jpcc_picker.py
```
※ 実行前に `jpcc_picker.py` の「ユーザー設定」を編集してください。

## ユーザー設定（`jpcc_picker.py` の冒頭）
```python
OUTFILE = "output.csv"   # 出力CSV
KEYWORD = "ももクロ"     # 抽出ワード（部分一致）
MINL    = 100            # 最小文字数
MAXL    = 2000           # 最大文字数
LIMIT   = 2000           # 件数上限（simple/randomで使用）
MODE    = "simple"       # "simple" / "random" / "all"
```

- `simple`: ヒットから先頭 `LIMIT` 件（速い）
- `random`: ヒット全体から均等確率で `LIMIT` 件（1パス・リザーバサンプリング）
- `all`: 条件に合うものを全件（`LIMIT`無視）

## 注意
- データは ABEJA-CC-JA の **公開S3** から匿名アクセス（UNSIGNED）で読み込みます。
- ネットワーク品質やファイルサイズによって処理時間は変わります。`random`/`all` は**全行スキャン**のため時間がかかります。

## 📜 ライセンス
- コード: MIT License
- データ: **ABEJA-CC-JA (based on Common Crawl)**。ABEJA が AWS Open Data Program を通じて公開。  
  商用利用可ですが、**ABEJA / Common Crawl の利用規約を遵守**してください。
- 本リポジトリはコードのみを提供し、外部データの権利は含みません。
