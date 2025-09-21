
"""
jpcc_picker.py (minimal)

必要最小限のユーザー設定だけで、ABEJA-CC-JA（Common Crawl 日本語版）のJSONLから
キーワードを含む行を抽出して CSV に書き出します。

# ===== 使い方 =====
1) 依存インストール: pip install boto3
2) 下の「ユーザー設定」を編集（特に KEYWORD, MODE, LIMIT など）
3) 実行: python jpcc_picker.py
   → output.csv（id,text,char_len）が生成されます

# モード
- simple : 条件にヒットした行を先頭から LIMIT 件まで
- random : ヒット全体から均等確率で LIMIT 件（内部でリザーバサンプリング）
- all    : 条件に合う行すべて（LIMITは無視）
"""

# ===== ユーザー設定（必要最小限） =====
OUTFILE = "output.csv"      # 出力CSVファイル名
KEYWORD = "ももクロ"        # 抽出したいキーワード（単純部分一致）
MINL    = 100               # 最小文字数
MAXL    = 2000              # 最大文字数
LIMIT   = 2000              # 抽出件数（MODE=simple/randomで使用）
MODE    = "simple"          # "simple" / "random" / "all"
# ======================================

import csv
import json
import random
import boto3
from botocore.config import Config
from botocore import UNSIGNED

# --- ABEJA-CC-JA（公開S3; 変更不要） ---
BUCKET = "abeja-cc-ja"
KEY    = "common_crawl_0.jsonl"
REGION = "ap-northeast-1"
# -------------------------------------

def iter_records():
    """S3のJSONLをストリームで1行ずつ読む（UNSIGNED=匿名アクセス）"""
    s3 = boto3.client("s3", region_name=REGION, config=Config(signature_version=UNSIGNED))
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    for b in obj["Body"].iter_lines():
        if not b:
            continue
        try:
            line = b.decode("utf-8", errors="replace").strip()
            yield json.loads(line)
        except Exception:
            continue

def extract_text(obj):
    """よくあるキーから本文を取り出す（ABEJA-CC-JAは通常 'content'）"""
    for k in ("content", "text", "body", "message", "doc"):
        v = obj.get(k)
        if isinstance(v, str):
            return v
    for v in obj.values():
        if isinstance(v, str):
            return v
    return None

def match_text(text):
    """長さフィルタ + キーワード部分一致"""
    if not isinstance(text, str):
        return False
    n = len(text)
    if n < MINL or n > MAXL:
        return False
    return KEYWORD in text

def reservoir_sample(source_iter, k):
    """均等確率で k 件を抽出（1パス・リザーバサンプリング）"""
    reservoir = []
    t = 0
    for obj in source_iter:
        text = extract_text(obj)
        if not match_text(text):
            continue
        t += 1
        if len(reservoir) < k:
            reservoir.append(obj)
        else:
            j = random.randint(1, t)
            if j <= k:
                reservoir[j-1] = obj
    return reservoir

def run():
    src = iter_records()
    rows = []  # (id, text, char_len)

    if MODE == "random":
        hits = reservoir_sample(src, LIMIT)
        for i, obj in enumerate(hits, 1):
            text = extract_text(obj)
            if text is None:
                continue
            _id = obj.get("id", i)
            rows.append((_id, text, len(text)))

    elif MODE == "all":
        i = 0
        for obj in src:
            text = extract_text(obj)
            if not match_text(text):
                continue
            i += 1
            _id = obj.get("id", i)
            rows.append((_id, text, len(text)))

    else:  # simple
        i = 0
        for obj in src:
            text = extract_text(obj)
            if not match_text(text):
                continue
            i += 1
            _id = obj.get("id", i)
            rows.append((_id, text, len(text)))
            if len(rows) >= LIMIT:
                break

    with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "text", "char_len"])
        for r in rows:
            w.writerow(r)

    print(f"✅ Done: {len(rows)} rows -> {OUTFILE} (MODE={MODE}, KEYWORD={KEYWORD})")

if __name__ == "__main__":
    run()
