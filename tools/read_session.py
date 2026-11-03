"""读取 DSH 会话日志，定位 BV1bo7WzVEgQ 相关的工具调用与错误。

用法:
    python tools/read_session.py <session.v4.jsonl.zstd>
未传参数时取 ~/.dsh/sessions 下最近修改的一个会话文件。
也可用环境变量 DSH_SESSION_FILE / DSH_SESSIONS_DIR 指定。
"""

import json
import os
import sys
from pathlib import Path

import zstandard

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

SESSIONS_DIR = Path(os.getenv("DSH_SESSIONS_DIR") or (Path.home() / ".dsh" / "sessions"))

if len(sys.argv) > 1:
    SESSION = Path(sys.argv[1])
elif os.getenv("DSH_SESSION_FILE"):
    SESSION = Path(os.environ["DSH_SESSION_FILE"])
else:
    found = sorted(
        SESSIONS_DIR.rglob("session*.jsonl.zstd"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not found:
        sys.exit(f"未找到会话文件，请传入路径，或用 DSH_SESSIONS_DIR 指定目录（当前: {SESSIONS_DIR}）")
    SESSION = found[0]

if not SESSION.is_file():
    sys.exit(f"会话文件不存在: {SESSION}")

raw = zstandard.ZstdDecompressor().decompress(SESSION.read_bytes(), max_output_size=200 * 1024 * 1024)
lines = raw.decode("utf-8", errors="replace").splitlines()
print(f"LINES={len(lines)} FILE={SESSION.parent.name}")

hits = 0
for line in lines:
    if "BV1bo7WzVEgQ" in line or "bili_postwatch" in line or "finish_watching" in line:
        hits += 1
        try:
            obj = json.loads(line)
        except Exception:
            print("RAW:", line[:300])
            continue
        ts = obj.get("time") or obj.get("timestamp") or obj.get("at") or ""
        typ = obj.get("type") or obj.get("kind") or ""
        text = json.dumps(obj, ensure_ascii=False)
        print(f"--- HIT {hits} ts={ts} type={typ} len={len(text)}")
        print(text[:1200])

print(f"TOTAL_HITS={hits}")
print("=== LAST 6 ENTRIES (types) ===")
for line in lines[-6:]:
    try:
        obj = json.loads(line)
        print(obj.get("time") or obj.get("timestamp") or "", obj.get("type") or obj.get("kind") or "", json.dumps(obj, ensure_ascii=False)[:300])
    except Exception:
        print("RAW:", line[:200])
