"""FastMCP Server 入口：bili-postwatch-companion。

暴露两个 Tool：
1. finish_watching_bilibili  — 整集流水线（下载/切片/ASR/断句/术语/翻译/档案）
2. recall_scene_lines        — 复盘过程中的逐句台词回溯检索
"""

import asyncio
import logging
import sys
from pathlib import Path

# 允许以 `python src/server.py` 方式直接启动（MCP 客户端常见配置）
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mcp.server.fastmcp import FastMCP

from src import config
from src.asr.tencent_batch_asr import TencentBatchASR
from src.captioner.digest_builder import build_digest
from src.captioner.glossary import build_glossary
from src.captioner.splitter import split_words_into_segments
from src.captioner.translator import translate_segments
from src.extractor.audio_chunker import cleanup_workdir, download_and_chunk
from src.extractor.bili_meta import extract_bvid, fetch_bili_meta
from src.schemas import BiliMeta
from src.storage.episode_db import EpisodeDB, parse_time_to_ms

config.configure_logging()
logger = logging.getLogger("bili-postwatch.server")

SUPPORTED_LANGUAGES = {
    "16k_ja": "日语",
    "16k_en": "英语",
    "16k_ko": "韩语",
    "16k_th": "泰语",
    "16k_vi": "越南语",
}

mcp = FastMCP("bili-postwatch-companion")
db = EpisodeDB()

# 同一 bvid 的并发构建互斥，避免重复下载/扣费
_build_locks: dict[str, asyncio.Lock] = {}


def _recall_hint() -> str:
    return (
        "\n\n（提示：如需回忆某段具体几分几秒的逐句对话或搜索某个关键词的原话，"
        "请调用 recall_scene_lines 工具）"
    )


async def process_episode(
    source: str,
    language: str = "16k_ja",
    force_refresh: bool = False,
) -> tuple[str, str]:
    """执行整集流水线，返回 (bvid, digest_markdown)。

    已缓存且未强制刷新时毫秒级返回；否则依次执行：
    拉取 BiliMeta -> 下载并 600s 切片 -> 并发 ASR -> LLM 语义断句
    -> 提取全局术语表 -> 并发反思翻译 -> 生成全景档案 -> 写入 SQLite。
    """
    if language not in SUPPORTED_LANGUAGES:
        raise ValueError(
            f"不支持的语言引擎 {language}，可选值: {', '.join(SUPPORTED_LANGUAGES)}"
        )
    bvid = await extract_bvid(source)
    await db.init_db()

    if not force_refresh:
        cached = await db.get_episode_digest(bvid)
        if cached:
            logger.info("命中缓存，直接返回 %s 的档案", bvid)
            return bvid, cached

    lock = _build_locks.setdefault(bvid, asyncio.Lock())
    async with lock:
        if not force_refresh:  # 双重检查：等锁期间可能已被并发构建完成
            cached = await db.get_episode_digest(bvid)
            if cached:
                return bvid, cached

        logger.info("开始构建 %s（语言引擎: %s）", bvid, language)
        meta: BiliMeta = await fetch_bili_meta(source)
        logger.info("元数据: %s", meta.title)

        chunks = await download_and_chunk(
            f"https://www.bilibili.com/video/{meta.bvid}", meta.bvid
        )
        try:
            words = await TencentBatchASR(engine_model_type=language).transcribe_chunks(
                chunks
            )
        finally:
            cleanup_workdir(meta.bvid)

        segments = await split_words_into_segments(words, language=language)
        glossary = await build_glossary(segments, meta)
        lines = await translate_segments(segments, meta, glossary)
        digest = await build_digest(lines, meta, glossary)
        await db.save_episode_data(meta, glossary, lines, digest)
        logger.info("%s 构建完成: %d 句台词", bvid, len(lines))
        return bvid, digest


async def _build_transcript(bvid: str) -> str:
    """从数据库读取全集逐句字幕并格式化（时间码 + 中文台词 + 外语原文）。"""
    meta = await db.get_episode_meta(bvid)
    lines = await db.query_lines(bvid, limit=999_999)
    if not lines:
        return "（未找到台词记录）"
    body = "\n".join(
        f"[{line.time_range_str}] {line.final_zh} | 原文: {line.original_text}"
        for line in lines
    )
    title = meta.title if meta else bvid
    return f"# 《{title}》 全集字幕（{len(lines)} 句）\n\n{body}"


@mcp.tool()
async def finish_watching_bilibili(
    url: str, language: str = "16k_ja", force_refresh: bool = False
) -> str:
    """当用户表示刚看完某个 Bilibili 视频、生肉动画或切片，想要一起复盘讨论时调用。返回该视频的逐句双语字幕（时间码 + 中文台词 + 外语原文），可在复盘讨论中直接引用原话。

    Args:
        url: B 站视频链接（支持 b23.tv 短链）或纯 BV 号。
        language: 音频语言引擎，可选值: 16k_ja(日语, 默认), 16k_en(英语),
            16k_ko(韩语), 16k_th(泰语), 16k_vi(越南语)。
        force_refresh: 为 true 时忽略缓存强制重新构建。
    """
    try:
        bvid, _digest = await process_episode(
            url, language=language, force_refresh=force_refresh
        )
        transcript = await _build_transcript(bvid)
        return f"{transcript}{_recall_hint()}"
    except Exception as err:  # noqa: BLE001 - 以字符串形式回传错误，便于聊天 Bot 提示用户
        logger.exception("finish_watching_bilibili 失败")
        return f"处理失败：{err}"


@mcp.tool()
async def recall_scene_lines(
    url_or_bvid: str, start_time: str = "", end_time: str = "", keyword: str = ""
) -> str:
    """在与用户复盘讨论过程中，需要查看某个具体时间段（如 start_time="12:00", end_time="15:30"）或包含某个关键词（keyword="契约"）的逐句精细台词、外语原文与纠错注释时调用。

    Args:
        url_or_bvid: B 站视频链接（支持 b23.tv 短链）或纯 BV 号，须为已建档视频。
        start_time: 起始时间 "MM:SS"（也支持 "HH:MM:SS"），留空表示从头开始。
        end_time: 结束时间 "MM:SS"，留空表示直到片尾。
        keyword: 关键词，会同时匹配中文台词、外语原文与纠错注释。
    """
    try:
        bvid = await extract_bvid(url_or_bvid)
        await db.init_db()
        start_ms = parse_time_to_ms(start_time) if start_time else None
        end_ms = parse_time_to_ms(end_time) if end_time else None
        if start_time and start_ms is None:
            return f"无法解析 start_time: {start_time}（请使用 MM:SS 格式）"
        if end_time and end_ms is None:
            return f"无法解析 end_time: {end_time}（请使用 MM:SS 格式）"

        lines = await db.query_lines(
            bvid, start_ms=start_ms, end_ms=end_ms, keyword=keyword or None, limit=40
        )
        if not lines:
            return (
                f"未找到匹配的台词（bvid={bvid}, start_time={start_time or '不限'}, "
                f"end_time={end_time or '不限'}, keyword={keyword or '不限'}）。"
                "若尚未对该视频调用过 finish_watching_bilibili，请先调用它生成档案。"
            )
        body = "\n".join(
            f"[{line.time_range_str}] {line.final_zh} | 原文: {line.original_text}"
            f" (注: {line.reflection_note or '无'})"
            for line in lines
        )
        return f"共 {len(lines)} 句（最多返回 40 句）：\n{body}"
    except Exception as err:  # noqa: BLE001
        logger.exception("recall_scene_lines 失败")
        return f"查询失败：{err}"


if __name__ == "__main__":
    mcp.run()
