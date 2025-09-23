# JPCC-PICKER
![python](https://img.shields.io/badge/python-3.11%2B-blue)  ![license](https://img.shields.io/badge/License-MIT-green)  ![ci](https://github.com/AI-NOSUKE/JPCC-PICKER/actions/workflows/ci.yml/badge.svg)  ![release](https://img.shields.io/github/v/release/AI-NOSUKE/JPCC-PICKER?color=orange)  

JPCC-PICKER は、公開されている Japanese Common Crawl (JPCC) データセットから、指定したキーワードを含む文章を簡単に抽出できるツールです。

過去のウェブに残された文章を大規模データから検索し、どんなことが語られてきたのかを抽出できます。  
研究・調査・分析にすぐ使える形で見える化できます。

---

## ✨ 特徴
- **ネット言説を手軽に収集**  
  特定のキーワードに関する文章を抽出し、過去にネット上でどのように語られてきたのかを把握できます。  

- **大規模データでもサクサク動作**  
  数億件規模の元データもストリーミング処理で1行ずつ読み込むため、メモリを圧迫せず動作します。  

- **検索方法の柔軟さ**  
  - 部分一致検索（文字列がそのまま含まれていればヒット）  
  - 複数キーワード OR 検索（いずれかが一致すればヒット）  

- **モード選択が可能**  
  - simple … 見つかった順に指定件数まで保存  
  - all … 全件保存  
  - random … 全件処理したうえでランダムに指定件数を抽出（リザーバサンプリング。偏りを抑えたい調査に有用）  

- **進捗がリアルタイムでわかる**  
  処理中は「lines / hits」を1行で更新表示。大規模データでも進捗を追いやすい。  

- **正規化 & 誤ヒット防止**  
  本文を Unicode NFKC で正規化してから検索。ASCII キーワードは単語境界を考慮しているため、誤ヒットを抑制できます。  

---

## 🔧 技術的な仕組み
- **データの所在**  
  JPCC（Japanese Common Crawl）は、Common Crawl の日本語部分を整理した大規模なテキストデータセットです。  
  **2019年〜2023年** に収集されたウェブページを基に構築、ABEJA によって AWS S3 バケット（`abeja-cc-ja`） として公開されています。  

- **アクセス方法**  
  匿名アクセス（署名なし UNSIGNED リクエスト）で直接読み込みます。  
  → AWSアカウントや認証設定は不要。**AWS CLI も不要**です。  

- **対応フォーマット**  
  `.jsonl` と `.jsonl.gz` の両形式に対応。圧縮データも直接ストリーミング処理できます。  

- **UX 改善**  
  実行時ログや制御フローを改善。  
  - simple モードでの `reached limit` 表示が改行付きで安定  
  - 進捗ログをより読みやすく整理  

---

## 📥 インストール

```bash
git clone https://github.com/AI-NOSUKE/JPCC-PICKER.git
cd JPCC-PICKER
pip install -r requirements.txt
```

- **依存関係**：`boto3`（= AWS SDK for Python のみ）  
- OS や AWS 周辺ツールの追加インストールは不要です（AWS CLI / 認証設定は使いません）。  

requirements.txt 例：

```
boto3==1.34.*
```

---

## ▶️ 最小実行例

`jpcc_picker.py` の冒頭を編集して実行します。

```python
# 設定例
OUTFILE = "output.csv"
KEYWORDS = ["ももクロ", "ももいろクローバーZ"]  # 複数指定で OR 検索
MINL, MAXL = 100, 2000
LIMIT = 2000
MODE = "simple"  # "simple" / "all" / "random"
```

```bash
python jpcc_picker.py
```

---

## 📂 出力例

```csv
id,text,char_len
a1b2c3d4e5f6g7h8,ももクロのライブに行ってきた！最高！,22
z9y8x7w6v5u4t3s2,昨日はももいろクローバーZを久々に見た,25
```

---

## ❓ Q&A
- **Q. AWS アカウントは必要ですか？**  
  A. 不要です。 匿名（UNSIGNED）アクセスで公開データを直接読み込みます。AWS CLI も不要。  

- **Q. データは最新のインターネットですか？**  
  A. いいえ。 JPCC は **2019〜2023年** のウェブを基にしたコーパスで、リアルタイム更新ではありません。  

- **Q. 大量データでも動きますか？**  
  A. はい。 `.jsonl(.gz)` をストリーミング処理するため、PCでも実行できます（メモリ常駐なし）。  

- **Q. 複数キーワードはどのように一致しますか？**  
  A. OR 条件です。 いずれかの文字列が本文に“そのまま”含まれていればヒット（部分一致）。  

---

## 📜 ライセンス

MIT License のもとで自由に利用可能です（PVM とは無関係）。  
