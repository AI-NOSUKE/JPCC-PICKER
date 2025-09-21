# jpcc_picker.py
# 巨大JSONL(S3)からキーワードを含む行を抽出するスクリプト

import os, json, re, csv, subprocess, random, signal, sys
from typing import Tuple, List, Dict

# ===== 設定 =====
OUTFILE   = "output.csv"
KEYWORD   = "ももクロ"
MINL,MAXL = 100, 2000
LIMIT     = 2000
MODE      = "simple"            # "simple" / "random" / "all"
CHUNK     = 100 * 1024 * 1024   # 100MB
SEED      = 42
DEBUG     = False

BUCKET    = "abeja-cc-ja"
KEY       = "common_crawl_0.jsonl"
REGION    = "ap-northeast-1"

USE_BOTO3 = False
# ==============

random.seed(SEED)
PAT = re.compile(re.escape(KEYWORD))

STOP = False
def _stop(*_):
    global STOP; STOP = True
signal.signal(signal.SIGINT, _stop)

# ---------- IO helpers (with type hints) ----------

def _get_chunk_via_cli(start: int, end: int, fname: str) -> Tuple[int, str]:
    cmd = [
        "aws","s3api","get-object",
        "--bucket", BUCKET, "--key", KEY,
        "--range", f"bytes={start}-{end}", fname,
        "--no-sign-request","--region", REGION
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.returncode, (r.stderr or "").strip()

def _get_chunk_via_boto3(start: int, end: int, fname: str) -> Tuple[int, str]:
    try:
        import boto3
        from botocore import UNSIGNED
        from botocore.client import Config
        s3 = boto3.client("s3", region_name=REGION, config=Config(signature_version=UNSIGNED))
        resp = s3.get_object(Bucket=BUCKET, Key=KEY, Range=f"bytes={start}-{end}")
        with open(fname, "wb") as fo:
            fo.write(resp["Body"].read())
        return 0, ""
    except Exception as e:
        return 1, str(e)

def get_chunk(start: int, end: int, fname: str) -> Tuple[int, str]:
    if USE_BOTO3:
        return _get_chunk_via_boto3(start, end, fname)
    return _get_chunk_via_cli(start, end, fname)

def is_range_end_error(err: str) -> bool:
    e = (err or "").lower()
    return ("invalidrange" in e) or ("416" in e) or ("out of range" in e)

# ---------- Core processing (DRY化) ----------

def _emit_row(writer: csv.writer, mode: str, limit: int,
              state: Dict[str, object], row: tuple) -> bool:
    """
    1行の採択後に、modeに応じて書き出し/リザーバ更新を行う。
    戻り値: Trueなら処理終了（simpleでLIMIT到達）。
    state keys:
      written:int, reservoir:List[tuple], seen_hits:int
    """
    if mode == "simple":
        writer.writerow(row)
        state["written"] = int(state["written"]) + 1
        return state["written"] >= limit
    elif mode == "all":
        writer.writerow(row)
        return False
    else:  # random (Reservoir Sampling)
        state["seen_hits"] = int(state["seen_hits"]) + 1
        reservoir: List[tuple] = state["reservoir"]  # type: ignore
        if len(reservoir) < limit:
            reservoir.append(row)
        else:
            j = random.randint(0, state["seen_hits"] - 1)  # type: ignore
            if j < limit:
                reservoir[j] = row
        return False

def _process_json_line(line: str, rec_id: str, writer: csv.writer, mode: str,
                       limit: int, state: Dict[str, object]) -> bool:
    """
    1行分のJSONLテキストをパースし、フィルタ→採択まで行う。
    戻り値: Trueなら処理終了（simpleでLIMIT到達）。
    """
    if not line:
        return False
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as e:
        if DEBUG:
            print(f"[DEBUG] JSONDecodeError at {rec_id}: {e}")
        return False
    except Exception as e:
        if DEBUG:
            print(f"[DEBUG] Unexpected parse error at {rec_id}: {e}")
        return False

    text = obj.get("content")
    if not isinstance(text, str):
        return False
    if not PAT.search(text):
        return False
    n = len(text)
    if not (MINL <= n <= MAXL):
        return False

    clean = text.replace("\n"," ").replace("\r"," ").replace("\t"," ")
    row = (rec_id, clean, n)
    return _emit_row(writer, mode, limit, state, row)

# ---------- Main ----------

def main():
    start = 0
    chunk_id = 0

    print("STEP1: 出力CSVを準備中...")
    with open(OUTFILE, "w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["id","text","char_len"])

        state: Dict[str, object] = {
            "written": 0,
            "reservoir": [],   # type: ignore[list-item]
            "seen_hits": 0,
        }

        carry = ""
        total_hits_processed = 0

        while not STOP:
            end = start + CHUNK - 1
            chunk_file = f"chunk_{chunk_id:03}.jsonl"
            print(f"STEP2: 取得中 {chunk_file}  bytes={start}-{end}")
            code, err = get_chunk(start, end, chunk_file)

            if code != 0:
                if is_range_end_error(err):
                    print("ℹ️ 取得完了（終端）。")
                else:
                    print(f"⚠️ 取得失敗(code={code}): {err}")
                break

            print(f"STEP3: 処理中 {chunk_file}")
            try:
                with open(chunk_file, "r", encoding="utf-8", errors="ignore") as cf:
                    data = carry + cf.read()
            finally:
                if os.path.exists(chunk_file):
                    try:
                        os.remove(chunk_file)
                    except Exception:
                        pass

            if "\n" in data:
                *full_lines, tail = data.split("\n")
                carry = tail
            else:
                # 改行がない＝未完行を carry に保持し、次チャンクで連結
                full_lines, carry = [], data

            hits_this_chunk = 0
            for i, line in enumerate(full_lines):
                if STOP:
                    break
                stop_now = _process_json_line(
                    line=line,
                    rec_id=f"{chunk_id}-{i}",
                    writer=writer,
                    mode=MODE,
                    limit=LIMIT,
                    state=state
                )
                # “採択候補”の総試行回数を概ね把握（random時はseen_hits増で分かる）
                if PAT.search(line) if isinstance(line, str) else False:
                    hits_this_chunk += 1
                if stop_now:
                    print("STEP4: simple の規定件数に到達。終了。")
                    return

            total_hits_processed += hits_this_chunk

            # 進捗の軽い観測（modeに応じた値を表示）
            if MODE == "random":
                print(f"…chunk={chunk_id} hits_chunk≈{hits_this_chunk} / seen_total={state['seen_hits']} / reservoir={len(state['reservoir'])}")  # type: ignore
            elif MODE == "simple":
                print(f"…chunk={chunk_id} hits_chunk≈{hits_this_chunk} / written_total={state['written']}")
            else:  # all
                print(f"…chunk={chunk_id} hits_chunk≈{hits_this_chunk} / written_total={state['written']}")

            if STOP:
                break
            start += CHUNK
            chunk_id += 1

        # EOF（最終行が改行なし）の救済：carry を一度だけパース
        if carry and carry.strip():
            stop_now = _process_json_line(
                line=carry,
                rec_id=f"{chunk_id}-EOF",
                writer=writer,
                mode=MODE,
                limit=LIMIT,
                state=state
            )
            if stop_now:
                print("STEP4: simple の規定件数に到達（EOF行）。終了。")
                return

        # random の後処理（最終書き出し）
        if MODE == "random" and state["reservoir"]:
            print(f"STEP4: random リザーバ {len(state['reservoir'])} 件を書き出し")  # type: ignore
            for row in state["reservoir"]:  # type: ignore
                writer.writerow(row)

    print(f"✅ done: mode={MODE} → {OUTFILE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"💥 fatal: {e}")
        sys.exit(1)
