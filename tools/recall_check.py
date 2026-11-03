"""验证缓存命中与 recall 检索（基于已建档的真实数据）。"""

import asyncio
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

from src.server import process_episode
from src.storage.episode_db import EpisodeDB


async def main() -> None:
    t0 = time.perf_counter()
    bvid, digest = await process_episode("https://www.bilibili.com/video/BV1GJ411x7h7/", "16k_en")
    print(f"[缓存验证] 第二次调用耗时 {time.perf_counter()-t0:.2f}s（档案 {len(digest)} 字）")

    db = EpisodeDB()
    kw = await db.query_lines(bvid, keyword="never give you up")
    rng = await db.query_lines(bvid, start_ms=45000, end_ms=60000)
    print(f"[recall-关键词] never give you up 命中 {len(kw)} 句")
    for line in kw[:2]:
        print("   ", line.time_range_str, "|", line.final_zh, "| 原文:", line.original_text)
    print(f"[recall-时间] 00:45-01:00 命中 {len(rng)} 句")
    for line in rng[:2]:
        print("   ", line.time_range_str, "|", line.final_zh)


asyncio.run(main())
