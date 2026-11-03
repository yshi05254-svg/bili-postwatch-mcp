"""全景陪看记忆浓缩器：全集台词 -> 800~1200 字 Markdown《全集陪看记忆档案》。"""

import asyncio
import json
import logging

from src import config
from src.captioner import prompts
from src.schemas import BiliMeta, TranslatedLine
from src.storage.episode_db import ms_to_mmss

logger = logging.getLogger("bili-postwatch.digest")

_DIGEST_RETRIES = 2
_FALLBACK_SECTIONS = (
    "## 一、 核心设定与人物速览\n\n"
    "（档案生成失败，以下为元数据兜底信息。）\n\n"
    "## 二、 全集分场剧情脉络\n\n"
    "（未生成。）\n\n"
    "## 三、 名场面与高光台词原话\n\n"
    "（未生成。）\n\n"
    "## 四、 映后互动话题钩子\n\n"
    "（未生成。）\n"
)


def format_lines_for_digest(lines: list[TranslatedLine]) -> str:
    """全集台词格式化为 "[MM:SS] 终稿台词"。"""
    return "\n".join(f"[{ms_to_mmss(line.start_ms)}] {line.final_zh}" for line in lines)


def _fallback_digest(meta: BiliMeta) -> str:
    header = (
        f"# 《{meta.title}》陪看记忆档案\n\n"
        f"> 自动生成失败，以下为基础元数据备份。\n\n"
    )
    return header + _FALLBACK_SECTIONS


async def build_digest(
    lines: list[TranslatedLine],
    meta: BiliMeta,
    glossary: dict,
    client=None,
) -> str:
    """生成《全集陪看记忆档案》Markdown；LLM 彻底失败时返回元数据兜底档案。"""
    if not lines:
        return _fallback_digest(meta)
    if client is None:
        client = config.require_llm_client()

    messages = [
        {"role": "system", "content": prompts.DIGEST_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": prompts.DIGEST_USER_TEMPLATE.format(
                meta_context=meta.to_context_string(),
                glossary_json=json.dumps(glossary, ensure_ascii=False),
                lines_text=format_lines_for_digest(lines),
            ),
        },
    ]
    last_err: Exception | None = None
    for attempt in range(_DIGEST_RETRIES + 1):
        try:
            content = await config.chat_text(
                client, messages, temperature=0.3, max_retries=1
            )
            text = content.strip()
            if len(text) < 200:
                raise ValueError(f"档案过短({len(text)} 字)，疑似未按章节生成")
            logger.info("全景记忆档案生成完成: %d 字", len(text))
            return text
        except Exception as err:  # noqa: BLE001
            last_err = err
            logger.warning("档案生成第 %d 次尝试失败: %s", attempt + 1, err)
            await asyncio.sleep(1.0 * (attempt + 1))

    logger.error("档案生成彻底失败(%s)，返回元数据兜底档案", last_err)
    return _fallback_digest(meta)
