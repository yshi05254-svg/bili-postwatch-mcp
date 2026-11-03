"""LLM 语义断句 + 滑动游标时间戳回溯对齐。

ASR 默认按气口断句极易把一句话腰斩。本模块把 list[WordItem] 按 150 词分块
拼接成纯文本交给 LLM 插入 || 断句符，再用滑动游标按有效字符长度回溯出
每句话精确的 start_ms / end_ms。
"""

import asyncio
import json
import logging
import re
import unicodedata

from src import config
from src.captioner import prompts
from src.schemas import RawSegment, WordItem

logger = logging.getLogger("bili-postwatch.splitter")

# 150 是规约值：单块越小，LLM 断句越细、对齐漂移越小（实测 400 词块会把多句合并成一句）。
# 速度问题由下方的 Semaphore 并发解决，而不是放大分块。
WORDS_PER_CHUNK = 150
MAX_SPLIT_CONCURRENCY = 5
_SPLIT_RETRIES = 2  # LLM 断句失败时的额外重试次数


def clean_str(s: str) -> str:
    """移除空白字符与标点/符号，仅保留核心文字用于长度对齐。

    等价于正则 `\\s\\p{P}\\p{S}` 语义；Python 标准库 re 不支持 \\p 转义，
    故用 Unicode 类别判断实现（P*=标点，S*=符号）。
    """
    return "".join(
        ch
        for ch in s
        if not ch.isspace()
        and not unicodedata.category(ch).startswith(("P", "S"))
    )


def join_words_naturally(words: list[WordItem]) -> str:
    """把词拼接回纯文本：日文等直接相连，西文单词之间补空格。"""
    parts: list[str] = []
    for item in words:
        token = item.word
        if not token:
            continue
        if (
            parts
            and re.search(r"[A-Za-z0-9]$", parts[-1])
            and re.match(r"^[A-Za-z0-9]", token)
        ):
            parts.append(" ")
        parts.append(token)
    return "".join(parts)


def align_split_sentences(
    words: list[WordItem], llm_split_text: str, start_index: int = 1
) -> list[RawSegment]:
    """滑动游标时间戳回溯算法（核心算法）。

    LLM 偶发会多吐或少吐 1~2 个标点或假名，直接精确匹配会失败；
    因此按"清理后的有效字符长度"推进游标，直到累积长度达到目标句长。
    """
    raw_sentences = [s.strip() for s in llm_split_text.split("||") if s.strip()]
    segments: list[RawSegment] = []
    word_cursor = 0
    total_words = len(words)
    last_used_index = start_index - 1

    for idx, sent in enumerate(raw_sentences, start=start_index):
        if word_cursor >= total_words:
            break
        target_clean_len = len(clean_str(sent))
        if target_clean_len == 0:
            continue

        seg_start_ms = words[word_cursor].start_ms
        seg_end_ms = words[word_cursor].end_ms
        accumulated_len = 0

        # 推进 word_cursor 直到累积有效字符数达到 target_clean_len
        while word_cursor < total_words:
            w_clean = clean_str(words[word_cursor].word)
            accumulated_len += len(w_clean)
            seg_end_ms = words[word_cursor].end_ms
            word_cursor += 1
            if accumulated_len >= target_clean_len:
                break

        segments.append(
            RawSegment(
                index=idx,
                start_ms=seg_start_ms,
                end_ms=seg_end_ms,
                text=sent,
            )
        )
        last_used_index = idx

    # 尾部安全网：LLM 漏断（或漏吐）导致剩余词未被消费时，追加为最后一段，
    # 避免整段台词静默丢失；若残留仅为标点/空白（有效字符为 0），则并入前段
    # 只延长时间轴、不生成幽灵分段。
    if word_cursor < total_words:
        leftover = words[word_cursor:]
        tail_text = join_words_naturally(leftover)
        if clean_str(tail_text):
            segments.append(
                RawSegment(
                    index=last_used_index + 1,
                    start_ms=leftover[0].start_ms,
                    end_ms=leftover[-1].end_ms,
                    text=tail_text,
                )
            )
        elif segments:
            segments[-1].end_ms = leftover[-1].end_ms
    return segments


async def _llm_split_once(client, text: str) -> str:
    messages = [
        {"role": "system", "content": prompts.SPLIT_SYSTEM_PROMPT},
        {"role": "user", "content": prompts.SPLIT_USER_TEMPLATE.format(text=text)},
    ]
    content = await config.chat_text(client, messages, temperature=0.1)
    # 剥离 LLM 偶尔包裹的代码块围栏
    stripped = content.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z]*\n?", "", stripped)
        stripped = re.sub(r"```\s*$", "", stripped)
    # 若 LLM 违规把文本包进 <<< >>>，提取中间部分
    match = re.search(r"<<<\n?(.*?)\n?>>>", stripped, re.DOTALL)
    return match.group(1).strip() if match else stripped.strip()


async def split_words_into_segments(
    words: list[WordItem],
    language: str = "16k_ja",
    client=None,
    max_concurrency: int = MAX_SPLIT_CONCURRENCY,
) -> list[RawSegment]:
    """按 150 词分块 -> 并发请求 LLM 插入 || -> 游标对齐，返回全集 RawSegment 列表。

    单块 LLM 失败时重试 _SPLIT_RETRIES 次；仍失败则整块退化为一个粗句，
    绝不中断整集流水线。各块互不依赖，用 Semaphore 限流并发执行。
    """
    if not words:
        return []
    if client is None:
        client = config.require_llm_client()

    semaphore = asyncio.Semaphore(max_concurrency)
    chunks = [
        words[i : i + WORDS_PER_CHUNK] for i in range(0, len(words), WORDS_PER_CHUNK)
    ]

    async def _process_chunk(chunk_idx: int, chunk: list[WordItem]) -> list[RawSegment]:
        chunk_text = join_words_naturally(chunk)
        llm_split_text = ""
        last_err: Exception | None = None
        for attempt in range(_SPLIT_RETRIES + 1):
            try:
                async with semaphore:
                    llm_split_text = await _llm_split_once(client, chunk_text)
                break
            except Exception as err:  # noqa: BLE001
                last_err = err
                logger.warning(
                    "断句块 %d 第 %d 次调用失败: %s", chunk_idx + 1, attempt + 1, err
                )
                await asyncio.sleep(1.0 * (attempt + 1))

        if llm_split_text:
            # 块内 index 从 1 开始，合并后统一重排
            return align_split_sentences(chunk, llm_split_text, start_index=1)

        logger.error(
            "断句块 %d 彻底失败(%s)，整块退化为粗句: %s",
            chunk_idx + 1, last_err, chunk_text[:50],
        )
        return [
            RawSegment(
                index=1,
                start_ms=chunk[0].start_ms,
                end_ms=chunk[-1].end_ms,
                text=chunk_text,
            )
        ]

    block_results = await asyncio.gather(
        *(_process_chunk(idx, chunk) for idx, chunk in enumerate(chunks))
    )

    segments: list[RawSegment] = []
    for block in block_results:  # gather 保持提交顺序，即原音频时间顺序
        segments.extend(block)

    # index 防御性重排，保证 1..N 连续
    for i, seg in enumerate(segments, start=1):
        seg.index = i
    logger.info("语义断句完成: %d 词 -> %d 句", len(words), len(segments))
    return segments


def segments_to_payload(segments: list[RawSegment]) -> list[dict]:
    """RawSegment 列表的 JSON 化辅助（供测试与调试）。"""
    return [json.loads(seg.model_dump_json()) for seg in segments]
