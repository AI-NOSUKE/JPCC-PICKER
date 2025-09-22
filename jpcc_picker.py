# jpcc_picker.py
# ABEJA-CC-JA から JSONL(.gz 含む) を匿名アクセスで読み、キーワードヒットを CSV 出力
# - 決め打ちKey禁止（Paginatorで全件列挙）
# - 匿名S3（UNSIGNED）+ タイムアウト/リトライ
# - .gz/非gzどちらもストリーミング
# - 進捗は1行上書き（lines / hits）

import os, sys, json, csv, gzip, random, hashlib
import boto3
from botocore.client import Config
from botocore import UNSIGNED
from botocore.exceptions import ReadTimeoutError, EndpointConnectionError

# ===== ユーザー設定 =====
OUTFILE = "output.csv"                     # 出力ファイル名
KEYWORDS: list[str] = ["ももクロ","ももいろクローバーZ"]          # 抽出したいキーワード（複数可・部分一致＝本文のどこかにそのまま含まれていればヒット）
MINL, MAXL = 100, 2000                     # 最小・最大文字数
LIMIT = 1                               # 抽出件数（allモード時は無視）
CHUNK_SIZE = 10 * 1024 * 1024              # 非gzの行復元用チャンク（10MB）
MODE = "simple"                            # "simple" / "random" / "all"
# ========================

# 進捗の更新間隔（行数）
LOG_INTERVAL = 10_000

# S3 匿名クライアント（タイムアウト/リトライ強化）
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

# 既存CSVの列構成に追従するためのフラグ（起動時に自動判定）
APPEND_MATCHED_COL = False

# ---------- ユーティリティ ----------
def ensure_outfile(path: str):
    """既存CSVがあれば先頭行の列数を見て追従。無ければ3列ヘッダを作成。"""
    global APPEND_MATCHED_COL
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                header = f.readline().strip().split(",")
            # 4列かつ末尾が matched_keyword なら4列運用に追従
            if len(header) == 4 and header[-1].strip().lower() == "matched_keyword":
                APPEND_MATCHED_COL = True
            else:
                APPEND_MATCHED_COL = False
            return
        except Exception:
            pass
    # 新規作成（3列）
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["id", "text", "char_len"])
    APPEND_MATCHED_COL = False

def write_row(writer: csv.writer, row_id: str, txt: str):
    """既存CSVの列構成に合わせて1行を書き出す（改行はスペース化）。"""
    safe = txt.replace("\n", " ")
    if APPEND_MATCHED_COL:
        writer.writerow([row_id, safe, len(safe), ""])  # 旧4列ファイルに追従
    else:
        writer.writerow([row_id, safe, len(safe)])        # デフォは3列

def obj_id_from_text(txt: str) -> str:
    return hashlib.md5(txt.encode("utf-8")).hexdigest()[:16]

def extract_text(o: dict) -> str | None:
    # 既存仕様：代表的キーを優先
    for k in ("content", "text", "body", "message", "doc", "article", "raw_text", "desc", "description", "title"):
        v = o.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return None

def match_text(txt: str) -> bool:
    n = len(txt)
    if not (MINL <= n <= MAXL):
        return False
    # 複数キーワードの OR 判定（部分一致）
    for kw in KEYWORDS:
        if kw and kw in txt:
            return True
    return False

# ---------- S3 列挙 & ストリーム ----------
def list_jsonl_keys():
    """バケット内の .jsonl / .jsonl.gz を全件ページング列挙"""
    paginator = S3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl") or key.endswith(".jsonl.gz"):
                yield key

def iter_lines_plain(stream_body) -> str:
    """非gzの JSONL をチャンク読みして行復元（UTF-8）"""
    buf = b""
    for chunk in stream_body.iter_chunks(chunk_size=CHUNK_SIZE):
        if not chunk:
            continue
        buf += chunk
        while True:
            pos = buf.find(b"\n")
            if pos == -1:
                break
            line = buf[:pos]
            buf = buf[pos+1:]
            yield line.decode("utf-8", errors="ignore")
    if buf:
        yield buf.decode("utf-8", errors="ignore")

def iter_records():
    """S3 上の全 JSONL(.gz含む) を1行ずつ JSON にして流す（匿名・堅牢）"""
    for key in list_jsonl_keys():
        try:
            obj = S3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"]
            if key.endswith(".gz"):
                fh = gzip.GzipFile(fileobj=body)
                for raw in fh:
                    if not raw:
                        continue
                    try:
                        yield json.loads(raw.decode("utf-8", errors="ignore"))
                    except Exception:
                        continue
            else:
                for line in iter_lines_plain(body):
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except Exception:
                        continue
        except (ReadTimeoutError, EndpointConnectionError) as e:
            print(f"\n[WARN] ネットワークで一時エラー: {key} をスキップ ({e})")
            continue
        except Exception as e:
            print(f"\n[WARN] 取得失敗: {key} をスキップ ({e})")
            continue

# ---------- メイン ----------
def run():
    print("STEP1: 出力ファイル準備中...")
    ensure_outfile(OUTFILE)

    print("STEP2: データセット接続確認中...")
    try:
        # まずは存在確認として1ページだけ列挙
        first_keys = []
        paginator = S3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, PaginationConfig={"MaxItems": 20}):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".jsonl") or k.endswith(".jsonl.gz"):
                    first_keys.append(k)
            break
        if not first_keys:
            print("STEP2: データセット接続OK（対象ファイルが見つかりません）")
            print("中断します。対象の .jsonl / .jsonl.gz がバケットに存在するか確認してください。")
            return
        print(f"STEP2: データセット接続確認OK（例: {first_keys[0]} 他 {len(first_keys)}件）")
    except Exception as e:
        print(f"STEP2: データセット接続失敗: {e}")
        return

    print("STEP3: データ読み込み開始...")
    print(f"[STAT] lines=0 hits=0/{LIMIT if MODE!='all' else '-'}", end="\r", flush=True)

    lines = 0
    hits = 0
    reservoir = []  # random時のみ使用

    with open(OUTFILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if MODE == "random":
            # リザーバサンプリング（ヒットのみを対象）
            t = 0  # ヒット総数
            for rec in iter_records():
                lines += 1
                if lines == 1 or (lines % LOG_INTERVAL == 0):
                    print(f"[STAT] lines={lines:,} hits={t}/{LIMIT}", end="\r", flush=True)

                txt = extract_text(rec)
                if not txt or not match_text(txt):
                    continue

                t += 1
                row_id = obj_id_from_text(txt)
                if len(reservoir) < LIMIT:
                    reservoir.append((row_id, txt))
                else:
                    j = random.randint(1, t)
                    if j <= LIMIT:
                        reservoir[j-1] = (row_id, txt)

            # 書き出し（最後にまとめて）
            for row_id, txt in reservoir:
                write_row(writer, row_id, txt)
                hits += 1

        else:
            # simple / all：ヒットした順に逐次書き出し
            for rec in iter_records():
                lines += 1
                if lines == 1 or (lines % LOG_INTERVAL == 0):
                    print(f"[STAT] lines={lines:,} hits={hits}/{LIMIT if MODE!='all' else '-'}", end="\r", flush=True)

                txt = extract_text(rec)
                if not txt or not match_text(txt):
                    continue

                write_row(writer, obj_id_from_text(txt), txt)
                hits += 1

                if MODE == "simple" and hits >= LIMIT:
                    break

    # 最終行で改行してからサマリ
    print()
    print(f"STEP4: 完了  lines={lines:,}  hits={hits if MODE!='all' else str(hits)+'(all)'}")
    print(f"✅ Done: {hits} rows -> {OUTFILE} (MODE={MODE}, KEYWORDS={KEYWORDS})")

if __name__ == "__main__":
    run()
