"""FastMCP Server 入口：bili-postwatch-companion。

暴露五个 Tool：
1. finish_watching_bilibili  — 整集流水线（下载/切片/ASR/断句/术语/翻译/档案）
2. recall_scene_lines        — 复盘过程中的逐句台词回溯检索
3. bili_login_qrcode         — 发起 B 站扫码登录（返回二维码）
4. bili_login_qrcode_check   — 轮询扫码结果，成功后把凭证长期保存到本地
5. bili_login_status         — 查看本地保存的登录凭证是否仍有效
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
from src.captioner.glossary import build_glossary
from src.captioner.splitter import split_words_into_segments
from src.captioner.translator import translate_segments
from src.extractor.audio_chunker import cleanup_workdir, download_and_chunk
from src.extractor import bili_auth
from src.extractor.bili_meta import fetch_bili_meta, resolve_bili_input
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

# 单次返回的字幕总字符上限：超出部分由 recall_scene_lines 按时间段检索
MAX_TRANSCRIPT_CHARS = 60_000

mcp = FastMCP("bili-postwatch-companion")
db = EpisodeDB()

# 同一视频（含分P）的并发构建互斥，避免重复下载/扣费
_build_locks: dict[str, asyncio.Lock] = {}


def _episode_key(bvid: str, page: int) -> str:
    """分 P 视频用独立缓存键，避免 P2 覆盖 P1 的字幕库。"""
    return bvid if page <= 1 else f"{bvid}_p{page}"


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
    """执行整集流水线，返回 (episode_key, digest_markdown)。

    已缓存且未强制刷新时毫秒级返回；否则依次执行：
    拉取 BiliMeta -> 下载并 600s 切片 -> 并发 ASR -> LLM 语义断句
    -> 提取全局术语表 -> 并发反思翻译 -> 写入 SQLite。
    """
    if language not in SUPPORTED_LANGUAGES:
        raise ValueError(
            f"不支持的语言引擎 {language}，可选值: {', '.join(SUPPORTED_LANGUAGES)}"
        )
    bvid, page, download_url = await resolve_bili_input(source)
    episode_key = _episode_key(bvid, page)
    await db.init_db()

    if not force_refresh:
        cached = await db.get_episode_digest(episode_key)
        if cached:
            logger.info("命中缓存，直接返回 %s 的字幕", episode_key)
            return episode_key, cached

    lock = _build_locks.setdefault(episode_key, asyncio.Lock())
    async with lock:
        if not force_refresh:  # 双重检查：等锁期间可能已被并发构建完成
            cached = await db.get_episode_digest(episode_key)
            if cached:
                return episode_key, cached

        logger.info("开始构建 %s（分P=%d，语言引擎: %s）", bvid, page, language)
        meta: BiliMeta = await fetch_bili_meta(source)
        logger.info("元数据: %s", meta.title)

        chunks = await download_and_chunk(download_url, bvid, page)
        try:
            words = await TencentBatchASR(engine_model_type=language).transcribe_chunks(
                chunks
            )
        finally:
            cleanup_workdir(bvid, page)

        segments = await split_words_into_segments(words, language=language)
        glossary = await build_glossary(segments, meta)
        lines = await translate_segments(segments, meta, glossary)
        # 工具直接返回逐句字幕，不再生成千字摘要（省一次 LLM 调用与等待）；
        # digest 字段仅作为"已建档"标记供缓存判断。
        digest = f"# 《{meta.title}》已建档（共 {len(lines)} 句台词）"
        await db.save_episode_data(meta, glossary, lines, digest, episode_key=episode_key)
        logger.info("%s 构建完成: %d 句台词", episode_key, len(lines))
        return episode_key, digest


async def _build_transcript(episode_key: str, page: int = 1) -> str:
    """从数据库读取全集逐句字幕并格式化（时间码 + 中文台词 + 外语原文）。"""
    meta = await db.get_episode_meta(episode_key)
    lines = await db.query_lines(episode_key, limit=20_000)
    if not lines:
        return "（未找到台词记录）"

    header = f"# 《{meta.title if meta else episode_key}》"
    if page > 1:
        header += f" P{page}"
    header += f" 全集字幕（{len(lines)} 句）\n\n"

    out: list[str] = []
    used = len(header)
    truncated = False
    for line in lines:
        row = f"[{line.time_range_str}] {line.final_zh} | 原文: {line.original_text}\n"
        if used + len(row) > MAX_TRANSCRIPT_CHARS:
            truncated = True
            break
        out.append(row)
        used += len(row)

    body = "".join(out)
    if truncated:
        body += (
            "\n……（字幕过长已截断；剩余部分请用 recall_scene_lines "
            "按时间段或关键词检索）"
        )
    return header + body


@mcp.tool()
async def bili_login_qrcode() -> str:
    """开始 B 站扫码登录：生成登录二维码并返回。用户用手机 B 站 App 扫码并确认后，调用 bili_login_qrcode_check 完成登录并长期保存凭证。"""
    try:
        result = await bili_auth.start_qr_login()
    except Exception as err:  # noqa: BLE001
        logger.exception("生成登录二维码失败")
        return f"生成二维码失败：{err}"

    return (
        "请用手机 B 站 App 扫描下面的二维码并确认登录（约 3 分钟内有效）：\n\n"
        "```\n"
        f"{result['qr_text']}\n"
        "```\n\n"
        f"- 扫码不便时：二维码 PNG 已保存到 {result['qr_png']}，可打开后用手机扫描；\n"
        f"- 二维码内容是登录确认链接 {result['qr_url']}，也可以在手机浏览器（已登录 B 站）中打开完成确认。\n"
        "扫码并确认后，请调用 bili_login_qrcode_check 保存凭证。"
    )


@mcp.tool()
async def bili_login_qrcode_check(wait_seconds: int = 20) -> str:
    """检查 B 站扫码登录结果：用户在手机上完成扫码确认后调用。登录成功时凭证会长期保存到本地（data/bili_credentials.json），此后下载自动带上登录态。"""
    wait_seconds = max(1, min(int(wait_seconds), 60))
    try:
        result = await bili_auth.check_qr_login(wait_seconds=float(wait_seconds))
    except Exception as err:  # noqa: BLE001
        logger.exception("检查扫码状态失败")
        return f"检查扫码状态失败：{err}"

    message = result["message"]
    if result["status"] == "success":
        creds = result.get("creds") or {}
        return f"{message}（账号: {creds.get('uname', '')}，uid: {creds.get('mid', '')}）"
    return message


@mcp.tool()
async def bili_login_status() -> str:
    """查看本地保存的 B 站登录凭证状态（是否已扫码登录、账号名、凭证是否仍有效）。"""
    try:
        result = await bili_auth.verify_login()
    except Exception as err:  # noqa: BLE001
        logger.exception("校验登录凭证失败")
        return f"校验登录凭证失败：{err}"

    if not result["logged_in"]:
        return result["message"]
    saved_at = result.get("saved_at") or "未知时间"
    return f"{result['message']}（uid: {result.get('mid', '')}，凭证保存于 {saved_at}）"


@mcp.tool()
async def finish_watching_bilibili(
    url: str, language: str = "16k_ja", force_refresh: bool = False
) -> str:
    """当用户表示刚看完某个 Bilibili 视频、生肉动画或切片，想要一起复盘讨论时调用。返回该视频的逐句双语字幕（时间码 + 中文台词 + 外语原文），可在复盘讨论中直接引用原话。

    Args:
        url: B 站视频链接（支持 b23.tv 短链与 ?p=N 分P）或纯 BV 号。
        language: 音频语言引擎，可选值: 16k_ja(日语, 默认), 16k_en(英语),
            16k_ko(韩语), 16k_th(泰语), 16k_vi(越南语)。
        force_refresh: 为 true 时忽略缓存强制重新构建。
    """
    try:
        episode_key, _digest = await process_episode(
            url, language=language, force_refresh=force_refresh
        )
        _, page, _ = await resolve_bili_input(url)
        transcript = await _build_transcript(episode_key, page)
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
        bvid, page, _ = await resolve_bili_input(url_or_bvid)
        episode_key = _episode_key(bvid, page)
        await db.init_db()
        start_ms = parse_time_to_ms(start_time) if start_time else None
        end_ms = parse_time_to_ms(end_time) if end_time else None
        if start_time and start_ms is None:
            return f"无法解析 start_time: {start_time}（请使用 MM:SS 格式）"
        if end_time and end_ms is None:
            return f"无法解析 end_time: {end_time}（请使用 MM:SS 格式）"

        lines = await db.query_lines(
            episode_key, start_ms=start_ms, end_ms=end_ms, keyword=keyword or None, limit=40
        )
        if not lines and page > 1:
            # 分 P 未单独建档时，回退查询整片档案
            lines = await db.query_lines(
                bvid, start_ms=start_ms, end_ms=end_ms, keyword=keyword or None, limit=40
            )
            if lines:
                episode_key = bvid
        if not lines:
            return (
                f"未找到匹配的台词（视频={episode_key}, start_time={start_time or '不限'}, "
                f"end_time={end_time or '不限'}, keyword={keyword or '不限'}）。"
                "若尚未对该视频调用过 finish_watching_bilibili，请先调用它生成字幕。"
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
