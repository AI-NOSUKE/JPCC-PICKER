\
"""
jpcc_picker.py

任意のキーワードを .env / 環境変数で指定して、
ローカルJSONL または S3上の公開JSONL から条件に合う行を抽出します。

.env の例:
    KEYWORD="ももクロ"
    MODE="random"
    LIMIT=500
    USE_S3=false
    LOCAL_JSONL=examples/mini.jsonl
"""

import os, re, csv, json, random, subprocess, sys
from typing import Iterable, Dict, Any, Optional

# ---- .env 読み込み（任意） ----
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---- 設定 ----
def _getenv_str(name: str, default: str) -> str:
    return os.getenv(name, default)

def _getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")

KEYWORD   = _getenv_str("KEYWORD", "ももクロ")
USE_REGEX = _getenv_bool("USE_REGEX", False)
MINL      = _getenv_int("MINL", 100)
MAXL      = _getenv_int("MAXL", 2000)
MODE      = _getenv_str("MODE", "simple")   # simple / random / all
LIMIT     = _getenv_int("LIMIT", 500)
SEED      = _getenv_int("SEED", 42)
OUTFILE   = _getenv_str("OUTFILE", "output.csv")
DEBUG     = _getenv_bool("DEBUG", False)

USE_S3     = _getenv_bool("USE_S3", False)
LOCAL_JSON = _getenv_str("LOCAL_JSONL", "examples/mini.jsonl")

USE_BOTO3 = _getenv_bool("USE_BOTO3", False)
BUCKET    = _getenv_str("BUCKET", "abeja-cc-ja")
KEY       = _getenv_str("KEY", "common_crawl_0.jsonl")
REGION    = _getenv_str("REGION", "ap-northeast-1")
CHUNK     = _getenv_int("CHUNK", 100 * 1024 * 1024)

random.seed(SEED)

# ---- ヘルパ ----
def log(*args):
    if DEBUG:
        print("[DEBUG]", *args, file=sys.stderr)

def _extract_text(obj: Dict[str, Any]) -> Optional[str]:
    # よくあるキーの順に取り出す
    for k in ("text", "content", "body", "message", "doc"):
        if k in obj and isinstance(obj[k], str):
            return obj[k]
    # それ以外の場合は代表候補を探索（最初のstr値）
    for v in obj.values():
        if isinstance(v, str):
            return v
    return None

def _iter_local_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def _iter_s3_jsonl_via_cli(bucket: str, key: str, region: str) -> Iterable[Dict[str, Any]]:
    # aws s3 cp --no-sign-request s3://bucket/key - で標準出力に流す
    cmd = ["aws", "s3", "cp",
           f"s3://{bucket}/{key}", "-",
           "--region", region, "--no-sign-request"]
    log("Running:", " ".join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8")
    assert p.stdout is not None
    for line in p.stdout:
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except Exception:
            continue
    p.wait()

def _iter_s3_jsonl_via_boto3(bucket: str, key: str, region: str) -> Iterable[Dict[str, Any]]:
    import boto3
    from botocore.config import Config
    from botocore import UNSIGNED
    s3 = boto3.client("s3", region_name=region, config=Config(signature_version=UNSIGNED))
    obj = s3.get_object(Bucket=bucket, Key=key)
    for b in obj["Body"].iter_lines():
        if not b:
            continue
        try:
            line = b.decode("utf-8", errors="replace").strip()
            yield json.loads(line)
        except Exception:
            continue

def iter_records() -> Iterable[Dict[str, Any]]:
    if not USE_S3:
        log("Source=LOCAL", LOCAL_JSON)
        return _iter_local_jsonl(LOCAL_JSON)
    else:
        log("Source=S3", BUCKET, KEY, "via", "boto3" if USE_BOTO3 else "awscli")
        if USE_BOTO3:
            return _iter_s3_jsonl_via_boto3(BUCKET, KEY, REGION)
        else:
            return _iter_s3_jsonl_via_cli(BUCKET, KEY, REGION)

def match_text(text: str) -> bool:
    if text is None:
        return False
    n = len(text)
    if n < MINL or n > MAXL:
        return False
    if USE_REGEX:
        try:
            pattern = re.compile(KEYWORD)
        except re.error:
            pattern = re.compile(re.escape(KEYWORD))
        return bool(pattern.search(text))
    else:
        return KEYWORD in text

def reservoir_sample(source_iter: Iterable[Dict[str, Any]], k: int) -> Iterable[Dict[str, Any]]:
    """リザーバサンプリング: ヒットした要素から均等確率でk件を抽出"""
    reservoir = []
    t = 0
    for obj in source_iter:
        text = _extract_text(obj)
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

    header = ["id", "text", "char_len"]
    out_rows = []

    if MODE == "random":
        hits = reservoir_sample(src, LIMIT)
        for i, obj in enumerate(hits, 1):
            text = _extract_text(obj)
            if text is None:
                continue
            _id = obj.get("id", i)
            out_rows.append((_id, text, len(text)))
    elif MODE == "all":
        i = 0
        for obj in src:
            text = _extract_text(obj)
            if not match_text(text):
                continue
            i += 1
            _id = obj.get("id", i)
            out_rows.append((_id, text, len(text)))
    else:  # simple
        i = 0
        for obj in src:
            text = _extract_text(obj)
            if not match_text(text):
                continue
            i += 1
            _id = obj.get("id", i)
            out_rows.append((_id, text, len(text)))
            if len(out_rows) >= LIMIT:
                break

    with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in out_rows:
            w.writerow(row)

    print(f"✅ Done: {len(out_rows)} rows -> {OUTFILE} (MODE={MODE}, KEYWORD={KEYWORD})")

if __name__ == "__main__":
    run()
