"""模拟 DSH 场景：不设 cwd、直接用脚本绝对路径拉起 server，验证能初始化并列出工具。"""

import asyncio
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "Scripts" / "python.exe")
SCRIPT = str(ROOT / "src" / "server.py")


async def main() -> None:
    params = StdioServerParameters(command=PY, args=["-X", "utf8", SCRIPT], cwd=str(Path.home()))
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            names = sorted(t.name for t in (await session.list_tools()).tools)
            print("TOOLS:", names)
            assert "finish_watching_bilibili" in names and "recall_scene_lines" in names
    print("DSH_STYLE_LAUNCH_OK")


asyncio.run(main())
