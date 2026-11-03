"""扫描最近 60 分钟内修改的 DSH 会话，找 BV1bo7WzVEgQ 与 MCP 工具调用。"""

import json
import os
import sys
import time
from pathlib import Path

import zstandard

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# DSH 会话目录：可用环境变量 DSH_SESSIONS_DIR 指定，默认 ~/.dsh/sessions
ROOT = Path(os.getenv("DSH_SESSIONS_DIR") or (Path.home() / ".dsh" / "sessions"))
if not ROOT.is_dir():
    sys.exit(f"未找到 DSH 会话目录: {ROOT}（可用环境变量 DSH_SESSIONS_DIR 指定）")
CUTOFF = time.time() - 90 * 60
files = [
    p for p in ROOT.rglob("session*.jsonl.zstd")
    if p.stat().st_mtime > CUTOFF
]
files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
print(f"RECENT_FILES={len(files)}")

for path in files:
    try:
        raw = zstandard.ZstdDecompressor().decompress(
            path.read_bytes(), max_output_size=300 * 1024 * 1024
        )
    except Exception as err:
        print(f"SKIP {path.parent.name}: {err}")
        continue
    text = raw.decode("utf-8", errors="replace")
    has_bv = "BV1bo7WzVEgQ" in text
    has_tool = "bili_postwatch" in text or "finish_watching" in text
    print(
        f"{time.strftime('%H:%M:%S', time.localtime(path.stat().st_mtime))} "
        f"{path.parent.name} lines={text.count(chr(10)) + 1} BV={has_bv} TOOL={has_tool}"
    )
    if not (has_bv or has_tool):
        continue
    for line in text.splitlines():
        if "BV1bo7WzVEgQ" in line or "bili_postwatch" in line or "finish_watching" in line:
            try:
                obj = json.loads(line)
            except Exception:
                print("  RAW:", line[:400])
                continue
            keys = {k: obj.get(k) for k in ("time", "timestamp", "type", "role", "name") if k in obj}
            print("  ", keys, json.dumps(obj, ensure_ascii=False)[:700])
