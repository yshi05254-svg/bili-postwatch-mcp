"""整集流水线测试：BV1Zssf64E4h（强制刷新，走新的仅音频下载逻辑 + 登录态）。"""

import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src import config  # noqa: E402
from src.server import process_episode  # noqa: E402


async def main() -> None:
    config.configure_logging()
    episode_key, digest = await process_episode(
        "BV1Zssf64E4h", language="16k_ja", force_refresh=True
    )
    print(f"PIPELINE_OK: episode_key={episode_key}")
    print(f"digest: {digest}")

    # 抽样展示入库结果
    from src.storage.episode_db import EpisodeDB

    db = EpisodeDB()
    await db.init_db()
    lines = await db.query_lines(episode_key, limit=40)
    print(f"lines_total_sample: {len(lines)}")
    for line in lines[:5]:
        print(f"  [{line.time_range_str}] {line.final_zh} | {line.original_text}")


if __name__ == "__main__":
    asyncio.run(main())
