"""音频下载与定长分片。

腾讯云 CreateRecTask 的 SourceType=1（Base64 直传）限制原始音频 < 5MB，
因此先用 yt-dlp 下载并转码为 16000Hz 单声道 48kbps MP3，
再用 FFmpeg segment 按 600 秒定长无损切片（每片约 3.4MB，安全低于上限）。
"""

import asyncio
import logging
import shutil
from pathlib import Path

from src import config

logger = logging.getLogger("bili-postwatch.audio_chunker")

CHUNK_SECONDS = 600

# Step 1: 下载并转码（yt-dlp 输出模板带 %(ext)s，最终得到 <bvid>.mp3）
_YTDLP_BASE = [
    "--no-playlist",
    "-x",
    "--audio-format", "mp3",
    "--postprocessor-args", "ffmpeg:-ar 16000 -ac 1 -b:a 48k",
]


async def _run_command(cmd: list[str]) -> None:
    """异步执行外部命令，非零退出码时抛出带 stderr 尾部的异常。"""
    logger.info("执行命令: %s", " ".join(str(c) for c in cmd))
    proc = await asyncio.create_subprocess_exec(
        *[str(c) for c in cmd],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        tail = (stderr or stdout).decode(errors="replace")[-2000:]
        raise RuntimeError(
            f"命令失败 (exit={proc.returncode}): {cmd[0]} ...\n{tail}"
        )


def _append_cookies(cmd: list[str]) -> list[str]:
    cookies = config.BILI_COOKIES_FILE
    if cookies and Path(cookies).is_file():
        cmd.extend(["--cookies", str(Path(cookies).resolve())])
    return cmd


def work_dir_for(bvid: str) -> Path:
    return config.TEMP_AUDIO_DIR / bvid


def cleanup_workdir(bvid: str) -> None:
    """删除该 bvid 的整个临时目录（含残留分片）。"""
    shutil.rmtree(work_dir_for(bvid), ignore_errors=True)


async def download_and_chunk(url: str, bvid: str) -> list[Path]:
    """下载 -> 转码 -> 600s 切片，返回按序号排序的分片路径列表。

    任一环节失败时清理残留文件后向上抛异常（try...finally 语义）。
    """
    ffmpeg_path = config.resolve_ffmpeg_path()
    work_dir = work_dir_for(bvid)
    # 幂等：清掉上一次运行可能留下的半成品
    if work_dir.exists():
        shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    try:
        raw_mp3_path = work_dir / f"{bvid}.mp3"
        cmd = config.resolve_ytdlp_cmd() + _YTDLP_BASE + [
            "--ffmpeg-location", ffmpeg_path,
            "-o", str(work_dir / f"{bvid}.%(ext)s"),
            url,
        ]
        await _run_command(_append_cookies(cmd))

        if not raw_mp3_path.is_file():
            # 某些站点扩展名处理差异时，退化为在目录中寻找唯一 mp3
            candidates = sorted(work_dir.glob("*.mp3"))
            if not candidates:
                raise RuntimeError(f"yt-dlp 未产出音频文件，工作目录: {work_dir}")
            raw_mp3_path = candidates[0]
            logger.info("使用降级匹配到的音频文件: %s", raw_mp3_path.name)

        # Step 2: 600 秒无损切片（-c copy 沿用已转码的 16k/mono/48k 流）
        await _run_command([
            ffmpeg_path, "-y",
            "-i", raw_mp3_path,
            "-f", "segment",
            "-segment_time", str(CHUNK_SECONDS),
            "-c", "copy",
            str(work_dir / f"{bvid}_chunk_%03d.mp3"),
        ])

        chunks = sorted(work_dir.glob(f"{bvid}_chunk_*.mp3"))
        if not chunks:
            raise RuntimeError("FFmpeg 切片失败：未生成任何分片文件")

        for idx, chunk in enumerate(chunks):
            size = chunk.stat().st_size
            if size >= config.MAX_CHUNK_BYTES:
                raise RuntimeError(
                    f"分片 {chunk.name} 体积 {size / 1024 / 1024:.2f}MB 超过腾讯云 5MB 直传上限"
                )
            logger.info("分片 %02d: %s (%.2f MB)", idx, chunk.name, size / 1024 / 1024)
        return chunks
    except Exception:
        cleanup_workdir(bvid)
        raise
