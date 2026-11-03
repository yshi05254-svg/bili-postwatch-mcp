"""验证 finish_watching_bilibili 现在返回全集字幕而非总结档案。"""

import asyncio
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parent.parent


async def main() -> None:
    params = StdioServerParameters(
        command=str(ROOT / ".venv" / "Scripts" / "python.exe"),
        args=["-X", "utf8", str(ROOT / "src" / "server.py")],
        cwd=str(ROOT),
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "finish_watching_bilibili",
                {"url": "https://www.bilibili.com/video/BV1GJ411x7h7/"},
            )
            text = result.content[0].text
            lines = [l for l in text.splitlines() if l.startswith("[")]
            print("TOTAL_CHARS:", len(text))
            print("SUBTITLE_LINES:", len(lines))
            print("HEAD:")
            print("\n".join(text.splitlines()[:6]))
            print("...")
            print("TAIL:")
            print("\n".join(text.splitlines()[-2:]))
            assert "全集字幕" in text and len(lines) >= 10, "返回的不是字幕"
            print("TRANSCRIPT_MODE_OK")


asyncio.run(main())
