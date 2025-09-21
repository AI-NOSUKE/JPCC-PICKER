# jpcc-picker
**巨大JSONL（S3）からキーワードを含む行を抜き出す、ワンファイルの“拾い出し”ツール。**  
初心者でも数分で動かせる最小手順にしつつ、プロ用途での配慮（再現性・第三者データの扱い・負荷と法令順守）を明記しています。

---

## ✨ できること（Features）
- **部分取得（Range GET）**で大きなJSONLをチャンクに分けて読み込み
- **キーワードマッチ + 文字数フィルタ**（`MINL..MAXL`）
- **抽出モード**
  - `simple`: 先頭から順に、上限`LIMIT`まで
  - `random`: ヒット母集団から**均等確率のリザーバサンプリング**（`SEED`で再現性）
  - `all`: 条件に合うものをすべて出力
- **出力**: `output.csv`（列: `id,text,char_len`）
- **接続方法**: AWS CLI（匿名 `--no-sign-request`）または **boto3（UNSIGNED）** のどちらかを選択

> ⚠️ 本ツールは**コードのみ**を提供します。外部のデータ（S3オブジェクト）は第三者のもので、可用性や内容を保証しません。

---

## 🧱 リポジトリ構成（Minimum）
```
jpcc-picker/
├─ jpcc_picker.py
├─ README.md
├─ requirements.txt
├─ .gitignore
├─ LICENSE
└─ .github/
   └─ workflows/
      └─ ci.yml
```

- `jpcc_picker.py` … 本体（スクリプト先頭の定数 or `.env` で設定想定）
- `requirements.txt` … 使うなら `boto3` を記載（*AWS CLIだけなら空でもOK*）
- `.gitignore` … `__pycache__/`, `*.pyc`, `.venv/`, `.env`, `.DS_Store` など
- `ci.yml` … **ネットワークに触れない**インポートのスモークテスト

---

## 🚀 クイックスタート（初心者向け）
### 0) 前提
- **OS**: Windows 11 / macOS 14 / Ubuntu 22.04 で動作想定
- **Python**: 3.11+（仮想環境を推奨）

### 1) セットアップ
```bash
# 任意: 仮想環境
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

# 依存（boto3を使う場合のみ）
pip install -U pip
pip install -r requirements.txt  # ここに boto3 を書くか、後述の“設定”でCLI利用に切替
```

### 2) 接続方法を選ぶ（A か B）
**A. AWS CLI を使う（推奨・匿名アクセス）**
- インストール：
  - Windows: 公式 MSI / `winget install AWS.AWSCLI`
  - macOS: `brew install awscli`
  - Ubuntu: `sudo apt-get install -y awscli` など
- 匿名アクセス：本ツールは**公開S3**を想定し `--no-sign-request` を使います。`aws configure` は不要。

**B. boto3 を使う（匿名アクセス）**
- `requirements.txt` に `boto3` を書き、スクリプト内 `USE_BOTO3=True` に設定
- 署名なし（UNSIGNED）で公開オブジェクトにアクセスします

### 3) スクリプトの設定を編集
`jpcc_picker.py` の冒頭、または `.env` に以下を設定します（例）：
```
OUTFILE=output.csv
KEYWORD=ももクロ
MINL=100
MAXL=2000
LIMIT=500
MODE=random
CHUNK=104857600      # 100MB
BUCKET=your-public-bucket
KEY=path/to.jsonl
REGION=ap-northeast-1
USE_BOTO3=false      # CLIなら false / boto3なら true
SEED=42
```

### 4) 実行
```bash
python jpcc_picker.py
# → output.csv が生成されます（id,text,char_len）
```

---

## ⚙️ 設定パラメータ（要点だけ）
| 変数 | 例 | 役割 |
|---|---|---|
| `KEYWORD` | `ももクロ` | 抽出キーワード（正規表現。`re.escape`でエスケープ）|
| `MINL`/`MAXL` | `100` / `2000` | 文字数フィルタ（本文の長さ）|
| `MODE` | `simple` / `random` / `all` | 抽出モード |
| `LIMIT` | `500` | `simple`/`random`の最大出力件数 |
| `CHUNK` | `104857600` | 取得チャンク（バイト）。回線やメモリで調整 |
| `BUCKET` / `KEY` | `…` | 取得対象のS3オブジェクト |
| `REGION` | `ap-northeast-1` | 近いリージョンにすると安定・高速化の可能性 |
| `USE_BOTO3` | `true/false` | 接続方式の切替（boto3 or AWS CLI）|
| `SEED` | `42` | `random`モードの再現性 |

---

## 🔁 再現性（Reproducibility）
**簡易に再現できる情報**を残すと、チームでもトラブルが減ります：
- 使用 OS / Python バージョン
- 依存（`pip freeze > reproducible-requirements.txt`）
- 参照S3オブジェクトの **キー名・サイズ・ETag**（MD5相当の場合あり）
- 主要パラメータ（`KEYWORD / MINL / MAXL / MODE / LIMIT / CHUNK / SEED`）
- 可能なら `examples/mini.jsonl` を同梱（ローカルでも同じロジックで抽出できる）

> CI ではネットワークを使わず、**`import jpcc_picker` が通るか**のみを検証しています。

---

## 🤝 データソースの配慮（第三者S3 / ABEJA など）
- 本リポジトリは**第三者のS3データの所有者ではありません**。入手可能性・正確性・継続性は保証しません。
- **商標・ブランド**：本リポジトリは第三者と**非提携/非公式**です。ロゴ等は使用しません。
- **利用規約・ライセンス**：参照先の**Terms of Use / ライセンス / robots 的方針 / レート制限**を必ず守ってください。
- **負荷対策**：
  - 初期は小さめの `LIMIT` と `CHUNK` で試験
  - 連続アクセスの間隔を空ける
  - 失敗時の再試行は**指数バックオフ**（将来的にオプション化予定）
- **二次配布**：生成される `output.csv` の扱いは利用者責任。データの再配布・商用利用は各データのライセンスに従ってください。

> **Disclaimer**  
> This repository is **not affiliated with, endorsed by, or sponsored by** any third-party data provider.  
> It provides a generic extraction utility for publicly accessible JSONL files on S3.  
> The **code license** here does **not** grant any rights to the **data** you access with it.

---

## 🧪 ローカル検証サンプル（任意）
`examples/mini.jsonl` を用意（例）：
```json
{"content": "ももクロが好き"}
{"content": "関係ない文章"}
{"content": "ももいろクローバーZのライブ"}
```
- 同じロジックで `KEYWORD` マッチ & 文字数フィルタが働くことを確認

---

## 🧯 トラブルシュート
- **`AccessDenied` / `404`**: 公開オブジェクトか、`BUCKET/KEY` が正しいか確認。別リージョンの場合 `REGION` を合わせる
- **遅い/途中で止まる**: `CHUNK` を小さくする、回線時間帯を変える
- **文字化け**: JSONLのエンコーディング（UTF-8）を確認
- **CSVが崩れる**: 文中の改行/カンマを適切にエスケープ（本ツールは `csv` モジュールで出力）

---

## 📜 ライセンス
- **コード**: MIT（このリポジトリ内の `LICENSE` を参照）
- **外部データ**: それぞれの利用規約に従う（本リポジトリのライセンスは、外部データの権利を一切付与しません）

---

## 🛠 付録：CI と補助ファイル（コピペ）
### `.github/workflows/ci.yml`
```yaml
name: CI
on:
  push:
  pull_request:
jobs:
  smoke:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install deps (optional)
        run: |
          python -m pip install -U pip
          if [ -f requirements.txt ]; then pip install -r requirements.txt; fi
      - name: Import smoke test
        run: python -c "import jpcc_picker as m; print('import_ok')"
```

### `requirements.txt`
```
# boto3 を使う場合のみ有効化
boto3>=1.34.0
```

### `.gitignore`
```
__pycache__/
*.pyc
.venv/
.env
.DS_Store
```

---

## 🧭 方針（スコープ）
- **PVMのような巨大機能は目指さない**：あくまで“拾い出し（picker）”に特化
- ただし、**初心者が迷わない導線**と、**プロが安心できる配慮（再現性・配布可否・負荷）**は担保
- 将来の拡張（`--config`, バックオフ/レート制御, 並列化）は Issue で議論


---

### `.env.sample`（任意）
このリポジトリには `.env.sample` を同梱しています。ファイル名を `.env` にリネームすると、**追加の設定不要で自動読み込み**され、冒頭定数を上書きできます（`python-dotenv`が同梱）。
環境変数で渡す場合も同じキー名を使えます（例: `KEYWORD=ラーメン python jpcc_picker.py`）。
