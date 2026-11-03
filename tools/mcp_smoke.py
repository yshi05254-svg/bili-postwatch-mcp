"""用 MCP 客户端协议冒烟测试：拉起 stdio Server，验证两个 Tool 可被发现。"""

import asyncio
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parent.parent


async def main() -> None:
    params = StdioServerParameters(
        command=sys.executable,
        args=["-X", "utf8", "-m", "src.server"],
        cwd=str(ROOT),
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            names = sorted(t.name for t in tools.tools)
            print("TOOLS:", names)
            assert "finish_watching_bilibili" in names, "缺少 finish_watching_bilibili"
            assert "recall_scene_lines" in names, "缺少 recall_scene_lines"
            # recall_scene_lines 对未建档视频应优雅返回提示串而不是崩溃
            result = await session.call_tool(
                "recall_scene_lines",
                {"url_or_bvid": "BV1GJ411x7h7", "keyword": "不存在的词xyz"},
            )
            print("RECALL_UNINDEXED ->", result.content[0].text[:120])
            print("MCP_SMOKE_OK")


asyncio.run(main())
