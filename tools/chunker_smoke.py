"""audio_chunker 真实链路冒烟：yt-dlp 下载 -> 16k/mono/48k 转码 -> 600s 切片。"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config import resolve_ffmpeg_path
from src.extractor.audio_chunker import cleanup_workdir, download_and_chunk
from src.extractor.bili_meta import extract_bvid

URL = "https://www.bilibili.com/video/BV1GJ411x7h7/"


async def main() -> None:
    bvid = await extract_bvid(URL)
    print("BVID:", bvid)
    chunks = await download_and_chunk(URL, bvid)
    print("CHUNKS:", [(c.name, round(c.stat().st_size / 1024 / 1024, 2)) for c in chunks])

    ffprobe = str(Path(resolve_ffmpeg_path()).with_name("ffprobe.exe"))
    proc = await asyncio.create_subprocess_exec(
        ffprobe, "-v", "error", "-select_streams", "a:0",
        "-show_entries", "stream=sample_rate,channels,bit_rate:format=duration",
        "-of", "default=noprint_wrappers=1", str(chunks[0]),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, _ = await proc.communicate()
    print("FFPROBE:\n" + out.decode(errors="replace"))
    cleanup_workdir(bvid)
    print("CHUNKER_SMOKE_OK")


asyncio.run(main())
