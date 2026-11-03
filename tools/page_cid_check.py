"""验证分 P 视频的 cid 选取：从热门列表找一个多 P 视频，比对 _fetch_cid(page=2)。"""

import asyncio
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.extractor.audio_chunker import _HEADERS, _fetch_cid


async def main() -> None:
    async with httpx.AsyncClient(headers=_HEADERS, timeout=20.0) as client:
        popular = await client.get(
            "https://api.bilibili.com/x/web-interface/popular?ps=50&pn=1"
        )
        items = ((popular.json().get("data") or {}).get("list")) or []
        target = next((it for it in items if int(it.get("videos", 1)) > 1), None)
        if not target:
            print("SKIP: 热门列表中没有找到多 P 视频")
            return
        bvid = target["bvid"]
        view = (await client.get(
            f"https://api.bilibili.com/x/web-interface/view?bvid={bvid}"
        )).json()["data"]
        pages = view["pages"]
        expected = int(pages[1]["cid"])
        got = await _fetch_cid(client, bvid, 2)
        print(f"BVID={bvid} PAGES={len(pages)} EXPECTED_P2_CID={expected} GOT={got}")
        assert got == expected, "分 P cid 选取错误"
        print("PAGE_CID_OK")


asyncio.run(main())
