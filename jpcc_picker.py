# JPCC-PICKER（精密版＋UX改善・完全版）
# - JSONを全行パースして本文を組み立て、正規化(NFKC)してから検索・長さ判定
# - 正規表現でキーワード検索（ASCIIは擬似単語境界で誤ヒット抑制、日本語は部分一致）
# - MODE: simple / random / all（randomはリザーバサンプリング）
# - UX: STEPログ, [STAT], reached limit, done. は従来踏襲
#   変更点:
#     * STEP2: ファイル名を出さず「データにアクセス成功。」のみ
#     * STEP3: 「キーワードピックアップ開始...」
#     * CONFIG行を表示
#     * simple到達時に改行を入れて 'reached limit=11' 合成バグを解消

import os, csv, gzip, time, json, re, random, hashlib, unicodedata
from typing import Iterator, List, Dict, Any

import boto3
from botocore.client import Config
from botocore import UNSIGNED
from botocore.exceptions import ReadTimeoutError, EndpointConnectionError, ClientError

# ===== ユーザー設定 =====
OUTFILE = "output.csv"                     # 出力ファイル名
KEYWORDS: list[str] = ["ももクロ","ももいろクローバーZ"]          # 抽出したいキーワード（複数可・部分一致＝本文のどこかにそのまま含まれていればヒット）
MINL, MAXL = 100, 2000                     # 最小・最大文字数（短すぎる/長すぎるテキストを除外）
LIMIT = 2000                                  # 抽出件数（allモード時は無視）
CHUNK_SIZE = 10 * 1024 * 1024              # 非gzの行復元用チャンク（10MB）
MODE = "simple"                            # simple=見つけ次第終了, random=ランダム抽出, all=全件抽出
# ========================

LOG_INTERVAL = 1_000  # [STAT] linesの表示間隔（必要なら 表示を増やす100_000 などへ）

# 匿名S3クライアント
S3 = boto3.client(
    "s3",
    region_name="ap-northeast-1",
    config=Config(
        signature_version=UNSIGNED,
        connect_timeout=60,
        read_timeout=600,
        retries={"max_attempts": 5, "mode": "standard"},
    ),
)
BUCKET = "abeja-cc-ja"

# 出力列互換（id,text,char_len[,matched_keyword]）
APPEND_MATCHED_COL = False
def ensure_outfile(path: str):
    global APPEND_MATCHED_COL
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            with open(path, "r", encoding="utf-8") as f:
                header = f.readline().strip().split(",")
            APPEND_MATCHED_COL = (len(header) == 4 and header[-1].strip().lower() == "matched_keyword")
            return
        except Exception:
            pass
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["id", "text", "char_len"])
    APPEND_MATCHED_COL = False

def write_row(writer: csv.writer, obj_id: str, text: str):
    safe = text.replace("\n", " ")
    row_id = obj_id if obj_id and obj_id != "?" else hashlib.md5(safe.encode("utf-8")).hexdigest()[:16]
    if APPEND_MATCHED_COL:
        writer.writerow([row_id, safe, len(safe), ""])
    else:
        writer.writerow([row_id, safe, len(safe)])

# 本文抽出：代表キーを結合→NFKC正規化
TEXT_KEYS = ("content","text","body","article","title","raw_text","message","desc","description")
def normalize_text_fields(obj: Dict[str, Any]) -> str:
    parts: List[str] = []
    for k in TEXT_KEYS:
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
    s = "\n".join(parts).strip()
    return unicodedata.normalize("NFKC", s)

# キーワード正規表現（ASCIIは境界、日本語は部分一致）
def build_pat(keywords: List[str]) -> re.Pattern:
    parts: List[str] = []
    for kw in keywords:
        if not kw:
            continue
        if kw.isascii():
            parts.append(r"(?<![A-Za-z0-9_])" + re.escape(kw) + r"(?![A-Za-z0-9_])")
        else:
            parts.append(re.escape(kw))
    return re.compile("|".join(parts)) if parts else re.compile(r"(?!a)a")

PAT = build_pat(KEYWORDS)

# CHUNK安全クランプ（非gz）
_CH_MIN, _CH_MAX = 1*1024*1024, 16*1024*1024
CHUNK = max(_CH_MIN, min(CHUNK_SIZE, _CH_MAX))

def iter_lines_from_s3(key: str) -> Iterator[str]:
    obj = S3.get_object(Bucket=BUCKET, Key=key)
    body = obj["Body"]
    if key.endswith(".gz"):
        fh = gzip.GzipFile(fileobj=body)
        for raw in fh:
            if raw:
                yield raw.decode("utf-8", errors="ignore").rstrip("\n")
    else:
        buf = ""
        for chunk in body.iter_chunks(chunk_size=CHUNK):
            if not chunk:
                continue
            buf += chunk.decode("utf-8", errors="ignore")
            while True:
                pos = buf.find("\n")
                if pos == -1:
                    break
                yield buf[:pos]
                buf = buf[pos+1:]
        if buf:
            yield buf

def list_jsonl_keys(limit: int = 20) -> List[str]:
    keys: List[str] = []
    paginator = S3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".jsonl") or k.endswith(".jsonl.gz"):
                keys.append(k)
                if len(keys) >= limit:
                    return keys
    return keys

def run():
    mode = (MODE or "simple").lower()
    if mode not in ("simple","random","all"):
        print(f"[WARN] MODE={MODE} は不正のため simple にフォールバックします。")
        mode = "simple"

    print("STEP1: 出力ファイル準備中...")
    ensure_outfile(OUTFILE)

    print("STEP2: データセット接続確認中...")
    try:
        first_keys = list_jsonl_keys(limit=20)
        if not first_keys:
            print("STEP2: データにアクセスできません（対象ファイルが見つからない）")
            return
        # 素人向け：ファイル名は出さない
        print("STEP2: データにアクセス成功。")
    except ClientError as e:
        print(f"STEP2: データセット接続失敗: {e}")
        return
    except Exception as e:
        print(f"STEP2: 想定外の失敗: {e}")
        return

    print("STEP3: キーワードピックアップ開始...")
    # 実行設定のエコー
    print(f"[INFO] CONFIG KEYWORDS={KEYWORDS}, MODE={mode}, MINL={MINL}, MAXL={MAXL}, "
          f"LIMIT={'ALL' if mode=='all' else LIMIT}")

    print(f"[STAT] lines=0 hits=0/{LIMIT if mode!='all' else '-'}", end="\r", flush=True)

    start = time.time()
    lines = 0
    hits = 0
    seen_hits = 0
    rnd = random.Random(42)
    reservoir = []

    with open(OUTFILE, "a", newline="", encoding="utf-8", buffering=1_048_576) as f:
        writer = csv.writer(f)
        try:
            paginator = S3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=BUCKET):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not (key.endswith(".jsonl") or key.endswith(".jsonl.gz")):
                        continue
                    try:
                        for line in iter_lines_from_s3(key):
                            lines += 1
                            if lines == 1 or (lines % LOG_INTERVAL) == 0:
                                print(f"[STAT] lines={lines:,} hits="
                                      f"{(hits if mode!='random' else seen_hits)}/"
                                      f"{(LIMIT if mode!='all' else '-')}",
                                      end="\r", flush=True)

                            try:
                                data = json.loads(line)
                            except json.JSONDecodeError:
                                continue

                            text = normalize_text_fields(data)
                            if not text:
                                continue
                            if not PAT.search(text):
                                continue
                            n = len(text)
                            if n < MINL or n > MAXL:
                                continue

                            obj_id = data.get("id", "?")

                            if mode == "all":
                                write_row(writer, obj_id, text)
                                hits += 1
                            elif mode == "simple":
                                write_row(writer, obj_id, text)
                                hits += 1
                                if LIMIT and hits >= LIMIT:
                                    # 直前の [STAT]（end="\r"）を確定させないと "11" に見えるため改行
                                    print()
                                    print(f"[INFO] reached limit={LIMIT}")
                                    raise StopIteration
                            else:  # random
                                seen_hits += 1
                                if LIMIT and LIMIT > 0:
                                    if len(reservoir) < LIMIT:
                                        reservoir.append((obj_id, text))
                                    else:
                                        j = rnd.randrange(seen_hits)
                                        if j < LIMIT:
                                            reservoir[j] = (obj_id, text)
                                else:
                                    write_row(writer, obj_id, text)
                                    hits += 1

                    except (ReadTimeoutError, EndpointConnectionError):
                        continue
        except StopIteration:
            pass

        if mode == "random" and LIMIT and LIMIT > 0:
            for obj_id, text in reservoir:
                write_row(writer, obj_id, text)
            hits = min(LIMIT, seen_hits)

    elapsed = time.time() - start
    print()
    summary_hits = (
        f"{hits if mode!='all' else str(hits)+'(all)'}"
        if mode != "random" else f"{hits}/{seen_hits} (sample/seen)"
    )
    print(f"[INFO] done. lines={lines:,} hits={summary_hits} time={elapsed:.1f}s")
    print(f"STEP4: 完了  lines={lines:,}  hits={summary_hits}  time={elapsed:.1f}s")
    print(f"✅ Done: {hits} rows -> {OUTFILE} (MODE={mode}, KEYWORDS={KEYWORDS})")

if __name__ == "__main__":
    run()
