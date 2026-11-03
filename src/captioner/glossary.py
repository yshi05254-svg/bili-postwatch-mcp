"""全局术语锁定表提取：前 100 句 + 后 50 句原文样本 + 视频元数据 -> JSON 术语表。"""

import json
import logging
import re

from src import config
from src.captioner import prompts
from src.schemas import BiliMeta, RawSegment

logger = logging.getLogger("bili-postwatch.glossary")

_HEAD_LINES = 100
_TAIL_LINES = 50

EMPTY_GLOSSARY: dict = {"work_title": "", "character_and_term_glossary": {}}


def _extract_json_object(content: str) -> dict:
    """宽容地解析 LLM 输出中的 JSON 对象（容忍代码块围栏与前后杂文字）。"""
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
        text = re.sub(r"```\s*$", "", text)
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end <= start:
        raise ValueError(f"输出中未找到 JSON 对象: {text[:200]}")
    data = json.loads(text[start : end + 1])
    if not isinstance(data, dict):
        raise ValueError("JSON 顶层不是对象")
    return data


def build_sample_text(segments: list[RawSegment]) -> str:
    """取全集前 100 句与后 50 句原文样本（全集较短时自然取全集）。"""
    texts = [seg.text for seg in segments]
    if len(texts) <= _HEAD_LINES + _TAIL_LINES:
        chosen = texts
    else:
        chosen = texts[:_HEAD_LINES] + texts[-_TAIL_LINES:]
    return "\n".join(chosen)


async def build_glossary(
    segments: list[RawSegment],
    meta: BiliMeta,
    client=None,
) -> dict:
    """生成 {"work_title": ..., "character_and_term_glossary": {...}} 形式的术语表。

    LLM 失败或输出不合规时返回空骨架表，保证主流程不中断。
    """
    if not segments:
        return dict(EMPTY_GLOSSARY)
    if client is None:
        client = config.require_llm_client()

    messages = [
        {"role": "system", "content": prompts.GLOSSARY_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": prompts.GLOSSARY_USER_TEMPLATE.format(
                meta_context=meta.to_context_string(),
                sample_text=build_sample_text(segments),
            ),
        },
    ]
    try:
        content = await config.chat_text(client, messages, temperature=0.1, json_mode=True)
        data = _extract_json_object(content)
    except Exception as err:  # noqa: BLE001 - 术语表失败可降级
        logger.warning("术语表提取失败(使用空表降级): %s", err)
        return dict(EMPTY_GLOSSARY)

    glossary = {
        "work_title": str(data.get("work_title") or meta.title),
        "character_and_term_glossary": {},
    }
    terms = data.get("character_and_term_glossary")
    if isinstance(terms, dict):
        glossary["character_and_term_glossary"] = {
            str(k): str(v) for k, v in terms.items() if k and v
        }
    logger.info(
        "术语表提取完成: %s（%d 个术语）",
        glossary["work_title"],
        len(glossary["character_and_term_glossary"]),
    )
    return glossary
