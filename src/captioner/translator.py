"""并发反思翻译：30 句一批、Semaphore(5) 并发、JSON 解析失败自动重试与原文兜底。"""

import asyncio
import json
import logging
import re
from typing import Any

from src import config
from src.captioner import prompts
from src.captioner.glossary import _extract_json_object
from src.schemas import BiliMeta, RawSegment, TranslatedLine
from src.storage.episode_db import ms_to_mmss

logger = logging.getLogger("bili-postwatch.translator")

BATCH_SIZE = 30
MAX_CONCURRENCY = 5
_JSON_RETRIES = 2  # JSON 解析失败时的额外重试次数（共 3 次尝试）


def _format_batch_lines(segments: list[RawSegment]) -> str:
    """把一批台词格式化为 "编号. [MM:SS - MM:SS] 原文"。"""
    lines = []
    for seg in segments:
        time_range = f"{ms_to_mmss(seg.start_ms)} - {ms_to_mmss(seg.end_ms)}"
        lines.append(f"{seg.index}. [{time_range}] {seg.text}")
    return "\n".join(lines)


def _prev_context(segments: list[RawSegment], batch_start: int) -> str:
    """取上一批（即 batch_start 之前）的最后 2 句原文作为前情衔接。"""
    if batch_start <= 0:
        return "（本批为全集开头，无前情）"
    prev_texts = [seg.text for seg in segments[max(0, batch_start - 2) : batch_start]]
    return "\n".join(prev_texts)


def _fallback_line(seg: RawSegment) -> TranslatedLine:
    return TranslatedLine(
        index=seg.index,
        start_ms=seg.start_ms,
        end_ms=seg.end_ms,
        time_range_str=f"{ms_to_mmss(seg.start_ms)} - {ms_to_mmss(seg.end_ms)}",
        original_text=seg.text,
        reflection_note="（反思翻译失败，已保留原文）",
        final_zh=seg.text,
    )


def _match_items(
    batch_segments: list[RawSegment], items: list[dict]
) -> list[dict | None]:
    """把 LLM 返回的 items 按句号对位：优先 index 精确匹配，其次按位置兜底。"""
    by_index: dict[int, dict] = {}
    for item in items:
        if isinstance(item, dict):
            try:
                by_index[int(item.get("index"))] = item
            except (TypeError, ValueError):
                continue
    matched: list[dict | None] = []
    for pos, seg in enumerate(batch_segments):
        if seg.index in by_index:
            matched.append(by_index[seg.index])
        elif pos < len(items) and isinstance(items[pos], dict):
            matched.append(items[pos])
        else:
            matched.append(None)
    return matched


async def _translate_batch(
    client,
    meta: BiliMeta,
    glossary: dict,
    batch_segments: list[RawSegment],
    prev_text: str,
) -> list[dict | None]:
    """单批反思翻译：失败自动重试 _JSON_RETRIES 次，最终返回逐句映射（可能为 None）。"""
    messages = [
        {"role": "system", "content": prompts.TRANSLATE_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": prompts.TRANSLATE_USER_TEMPLATE.format(
                meta_context=meta.to_context_string(),
                glossary_json=json.dumps(glossary, ensure_ascii=False),
                prev_context=prev_text,
                numbered_lines=_format_batch_lines(batch_segments),
            ),
        },
    ]
    last_err: Exception | None = None
    for attempt in range(_JSON_RETRIES + 1):
        try:
            content = await config.chat_text(
                client, messages, temperature=0.2, json_mode=True
            )
            data = _extract_json_object(content)
            items = data.get("items")
            if not isinstance(items, list) or not items:
                raise ValueError("items 缺失或不是数组")
            return _match_items(batch_segments, items)
        except Exception as err:  # noqa: BLE001 - 解析失败重试
            last_err = err
            logger.warning(
                "翻译批 [%d-%d] 第 %d 次尝试失败: %s",
                batch_segments[0].index,
                batch_segments[-1].index,
                attempt + 1,
                err,
            )
            await asyncio.sleep(1.0 * (attempt + 1))
    logger.error(
        "翻译批 [%d-%d] 重试 %d 次仍失败(%s)，整批降级保留原文",
        batch_segments[0].index,
        batch_segments[-1].index,
        _JSON_RETRIES,
        last_err,
    )
    return [None] * len(batch_segments)


def _item_to_fields(item: dict | None) -> tuple[str, str]:
    """从 item 提取 (reflection_note, final_zh)，非法/空值由调用方兜底。"""
    if not item:
        return "", ""
    reflection = str(item.get("reflection") or "").strip()
    final_zh = str(item.get("final_zh") or "").strip()
    return reflection, final_zh


async def translate_segments(
    segments: list[RawSegment],
    meta: BiliMeta,
    glossary: dict,
    client=None,
    batch_size: int = BATCH_SIZE,
    max_concurrency: int = MAX_CONCURRENCY,
) -> list[TranslatedLine]:
    """全集并发反思翻译，返回与 segments 等长、按 index 排序的 TranslatedLine 列表。"""
    if not segments:
        return []
    if client is None:
        client = config.require_llm_client()

    semaphore = asyncio.Semaphore(max_concurrency)
    results: dict[int, TranslatedLine] = {}

    async def run_batch(batch: list[RawSegment]) -> None:
        async with semaphore:
            prev_text = _prev_context(segments, batch[0].index - 1)
            logger.info("翻译批 [%d-%d] 开始", batch[0].index, batch[-1].index)
            matched = await _translate_batch(client, meta, glossary, batch, prev_text)

        for seg, item in zip(batch, matched):
            reflection, final_zh = _item_to_fields(item)
            if not final_zh:
                results[seg.index] = _fallback_line(seg)
            else:
                results[seg.index] = TranslatedLine(
                    index=seg.index,
                    start_ms=seg.start_ms,
                    end_ms=seg.end_ms,
                    time_range_str=f"{ms_to_mmss(seg.start_ms)} - {ms_to_mmss(seg.end_ms)}",
                    original_text=seg.text,
                    reflection_note=reflection,
                    final_zh=final_zh,
                )
        logger.info("翻译批 [%d-%d] 完成", batch[0].index, batch[-1].index)

    batches = [
        segments[i : i + batch_size] for i in range(0, len(segments), batch_size)
    ]
    await asyncio.gather(*(run_batch(batch) for batch in batches))

    return [results[seg.index] for seg in sorted(segments, key=lambda s: s.index)]
