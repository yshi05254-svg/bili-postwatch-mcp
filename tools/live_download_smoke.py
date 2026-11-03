"""真机烟测：匿名走一遍新的仅音频下载路径（WBI 签名 playurl -> 探测 -> 分块下载 -> 切片）。

用法: python tools/live_download_smoke.py BV1UqHi6uEie
不依赖登录凭证；验证通过后产物会清理。
"""

import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src import config  # noqa: E402
from src.extractor import audio_chunker  # noqa: E402


async def main() -> None:
    bvid = sys.argv[1] if len(sys.argv) > 1 else "BV1UqHi6uEie"
    url = f"https://www.bilibili.com/video/{bvid}"
    config.configure_logging()

    out = audio_chunker.work_dir_for(bvid) / "smoke" / f"{bvid}_raw.m4s"
    out.parent.mkdir(parents=True, exist_ok=True)

    ok = await audio_chunker._try_direct_dash_download(url, bvid, out)
    if not ok or not out.is_file():
        print(f"FAIL: 直链下载未成功（{bvid}）")
        sys.exit(1)
    print(f"PASS: 直链下载 {out} ({out.stat().st_size / 1024 / 1024:.2f} MB)")

    ffmpeg = config.resolve_ffmpeg_path()
    await audio_chunker._slice_audio(ffmpeg, out, out.parent, bvid)
    chunks = sorted(out.parent.glob(f"{bvid}_chunk_*.mp3"))
    if not chunks:
        print("FAIL: FFmpeg 未切出分片")
        sys.exit(1)
    print(f"PASS: FFmpeg 切片 {len(chunks)} 片，首片 {chunks[0].stat().st_size / 1024:.0f} KB")

    import shutil
    shutil.rmtree(out.parent, ignore_errors=True)
    print("PASS: 产物已清理")


if __name__ == "__main__":
    asyncio.run(main())
