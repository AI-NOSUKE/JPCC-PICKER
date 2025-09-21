# jpcc-picker

**任意のキーワードで、巨大な日本語JSONL（S3 / ローカル）から文章を抽出する“拾い出し”ツール。**
- 初心者: まずは同梱の `examples/mini.jsonl` でオフライン確認 → 成功体験
- プロ: Range GET / S3（公開オブジェクト）に切替可能、再現性/配慮を明記

> **Not affiliated. Data rights are separate from the code license. Use public S3 responsibly and follow the provider’s Terms.**

---

## 🚀 まずはオフラインで（最短30秒）
```bash
# 1) 依存を入れる（仮想環境は任意）
pip install -r requirements.txt

# 2) .env を用意（サンプルをコピー）
cp .env.sample .env

# 3) 実行（デフォルトは examples/mini.jsonl を対象）
python jpcc_picker.py
# => output.csv が生成されます（id,text,char_len）
```

> デフォルトは **ローカルファイル** を使います。まずはネットワーク無しで動作確認できます。

---

## 🎯 できること
- **キーワード抽出 + 文字数フィルタ**（`MINL..MAXL`）
- 抽出モード：
  - `simple`: 条件に合う行を先頭から `LIMIT` 件まで
  - `random`: 条件に合う母集団から **均等確率のリザーバサンプリング**（再現用 `SEED`）
  - `all`: 条件に合う行をすべて
- 出力は `output.csv`（列: `id,text,char_len`）
- 入力は **ローカルJSONL** または **S3の公開JSONL**（CLIや認証は不要/UNSIGNED想定）

---

## 🔧 設定（.env または環境変数）
`.env.sample` をコピーして `.env` を編集します。主要項目：

```ini
# 抽出の基本
KEYWORD=ももクロ         # 抽出キーワード
USE_REGEX=false          # trueで正規表現、そのほかはリテラル一致（安全）
MINL=100                 # 文字数の下限
MAXL=2000                # 文字数の上限
MODE=random              # simple / random / all
LIMIT=500                # simple/random の件数上限
SEED=42                  # random再現用
OUTFILE=output.csv

# 入力ソース（まずはローカル）
USE_S3=false
LOCAL_JSONL=examples/mini.jsonl

# S3を使う場合の設定（公開オブジェクト想定）
USE_BOTO3=false          # trueでboto3、falseならAWS CLIを使用（どちらか片方）
BUCKET=your-public-bucket
KEY=path/to.jsonl
REGION=ap-northeast-1
CHUNK=104857600          # 100MB（Range GET用。CLI使用時のみ）
```

> **ワンポイント**: 一時的な上書きも可能です → `KEYWORD=ラーメン MODE=simple python jpcc_picker.py`

---

## ▶️ S3モードに切り替える（公開オブジェクト）
1. `.env` で `USE_S3=true` にする
2. `BUCKET`, `KEY`, `REGION` を設定
3. **boto3** を使うなら `USE_BOTO3=true`（匿名/UNSIGNED）  
   **AWS CLI** を使うなら `USE_BOTO3=false`（`--no-sign-request` を内部で利用）

**到達性チェック（CLI派・任意）**
```bash
aws s3api head-object   --bucket <bucket> --key <path/to.jsonl> --region <ap-northeast-1>   --no-sign-request
```
成功すれば匿名到達性OK。`ContentLength`/`ETag`が確認できます。

---

## 🧪 まずはローカル検証
`examples/mini.jsonl` は小さなJSONLサンプルです（UTF-8、1行1JSON）。  
`text` or `content` などのキーから本文を抽出します。

```jsonl
{"text": "ももクロが好き"}
{"text": "関係ない文章"}
{"text": "ももいろクローバーZのライブ"}
```

---

## 📁 構成
```
jpcc-picker/
├─ jpcc_picker.py
├─ README.md
├─ .env.sample
├─ requirements.txt
├─ .gitignore
├─ LICENSE
├─ examples/
│  └─ mini.jsonl
└─ .github/
   └─ workflows/
      └─ ci.yml
```

---

## 🔁 再現性と配慮
- `SEED`, `LIMIT`, `MINL..MAXL`, `KEYWORD` など**抽出条件は .env で固定**可能
- **第三者データの配慮**：本リポジトリは第三者S3データの所有者ではありません。入手可能性と正確性は保証しません。各データ提供者の**利用規約/レート制限**を遵守してください。
- **負荷対策**：初期は `LIMIT`/`CHUNK` を小さめに、アクセス間隔を空ける、失敗時は控えめに再試行。

---

## 🛠 CI
- **ネットワークを使いません**。ローカル `examples/mini.jsonl` を対象に、実行スモークを行います。

---

## 📜 ライセンス
- コード: MIT License
- データ: ABEJA-CC-JA (based on Common Crawl)  
  ABEJA が AWS Open Data Program を通じて公開している日本語コーパスです。  
  商用利用も可能ですが、利用にあたっては必ず ABEJA および Common Crawl の利用規約を遵守してください。
- 本リポジトリはコードのみを提供し、外部データの権利は含みません。
