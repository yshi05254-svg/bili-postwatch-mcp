"""腾讯云录音文件识别（ASR）并发封装与词级时间戳缝合。

- 单分片：读取 MP3 -> Base64 -> CreateRecTask(SourceType=1) -> 每 2s 轮询 DescribeTaskStatus（上限 180s）
- 统一优先 ResTextFormat=2（词级时间戳+标点）。官方文档称 16k_ja/16k_ko 等
  小语种仅支持 0，但实测 16k_ja 传 2 可正常返回 Words；若个别引擎拒绝则自动降级为 0。
- format 0 不返回 ResultDetail，改用 Result 中的句级时间码 [m:ss.mmm,…] 重建时间轴
- 官方 SDK 为同步阻塞 IO，一律用 asyncio.to_thread 包装
- 跨切片时间戳：word_start = sentence.StartMs + word.OffsetStartMs + chunk_idx * 600_000
- 句级 Words 缺失时，按「西文整词 / CJK 单字」在句区间内均匀分配生成伪 WordItem
"""

import asyncio
import base64
import logging
import re
import time
from pathlib import Path

from src import config
from src.schemas import WordItem

logger = logging.getLogger("bili-postwatch.tencent_asr")

POLL_INTERVAL_SEC = 2.0
POLL_TIMEOUT_SEC = 180
CHUNK_OFFSET_MS = 600_000  # 与 audio_chunker.CHUNK_SECONDS 对应的毫秒偏移步长
ASR_CONCURRENCY = 3        # 限制同时上传识别的分片数，避免触发腾讯云并发/QPS 限流

# format 0 的 Result 形如 "[0:1.240,0:47.200]  文本[0:49.140,1:49.620]  文本…"，
# 句级时间码保留为 ResultDetail 缺失时的降级数据源。
_RESULT_SENTENCE_RE = re.compile(
    r"\[(\d+(?::\d+){1,2}(?:\.\d+)?),(\d+(?::\d+){1,2}(?:\.\d+)?)\]\s*"
    r"(.*?)(?=\[\d+(?::\d+){1,2}(?:\.\d+)?,|\Z)",
    re.DOTALL,
)


def _time_token_to_ms(token: str) -> int:
    """把 "0:1.240" / "1:49.620" / "1:02:03.5" 解析为毫秒。"""
    total = 0.0
    for part in token.split(":"):
        total = total * 60 + float(part)
    return int(total * 1000)


class TencentASRError(RuntimeError):
    """腾讯云 ASR 配置或识别流程错误。"""


def _ensure_credentials() -> tuple[str, str, str]:
    secret_id = config.TENCENT_SECRET_ID
    secret_key = config.TENCENT_SECRET_KEY
    if not secret_id or not secret_key:
        raise TencentASRError(
            "未配置腾讯云密钥。请在 .env 中填写 TENCENT_SECRET_ID / TENCENT_SECRET_KEY。"
        )
    return secret_id, secret_key, config.TENCENT_ASR_REGION


def _build_client():
    from tencentcloud.asr.v20190614 import asr_client
    from tencentcloud.common import credential

    secret_id, secret_key, region = _ensure_credentials()
    cred = credential.Credential(secret_id, secret_key)
    return asr_client.AsrClient(cred, region)


def _create_task_sync(client, base64_str: str, data_len: int, engine_model_type: str) -> int:
    from tencentcloud.asr.v20190614 import models
    from tencentcloud.common.exception.tencent_cloud_sdk_exception import (
        TencentCloudSDKException,
    )

    def _request(res_text_format: int) -> int:
        req = models.CreateRecTaskRequest()
        req.EngineModelType = engine_model_type
        req.ChannelNum = 1
        req.ResTextFormat = res_text_format  # 2 = 词级时间戳+标点
        req.SourceType = 1                    # Base64 直传
        req.Data = base64_str
        req.DataLen = data_len
        resp = client.CreateRecTask(req)
        task_id = getattr(resp.Data, "TaskId", None) if resp.Data else None
        if not task_id:
            raise TencentASRError("CreateRecTask 未返回 TaskId")
        return int(task_id)

    try:
        return _request(2)
    except TencentCloudSDKException as err:
        # 官方文档称 16k_ja/16k_ko 等小语种仅支持 0（实测该账号下 2 可用），
        # 若某引擎确实拒绝则降级为 0，改由 Result 的句级时间码兜底。
        if "ResTextFormat" in str(err) or "InvalidParameter" in err.code:
            logger.warning(
                "引擎 %s 拒绝 ResTextFormat=2（%s），降级为 0 重试（改走句级时间码兜底）",
                engine_model_type, err,
            )
            return _request(0)
        raise


def _describe_task_sync(client, task_id: int):
    from tencentcloud.asr.v20190614 import models

    req = models.DescribeTaskStatusRequest()
    req.TaskId = task_id
    return client.DescribeTaskStatus(req)


def _sentence_text(sentence) -> str:
    """SentenceDetail 的文本字段按优先级取值。"""
    for attr in ("FinalSentence", "SliceSentence", "WrittenText", "Text"):
        value = getattr(sentence, attr, None)
        if value:
            return value
    return ""


def _pseudo_words_from_text(text: str, start_ms: int, end_ms: int) -> list[WordItem]:
    """把一段文本按「西文整词 / CJK 单字」切分，并在 [start_ms, end_ms] 内均匀分配时间。

    西文连续字母/数字/撇号保持整词，避免英文 "Hello" 被逐字母切开后
    在 splitter 中被插入空格变成 "H e l l o"。
    """
    tokens = re.findall(r"[A-Za-z0-9']+|\S", text) or [" "]
    total_chars = sum(len(t) for t in tokens) or 1
    span = max(0, end_ms - start_ms)

    items: list[WordItem] = []
    acc_chars = 0
    for token in tokens:
        t_start = start_ms + int(span * (acc_chars / total_chars))
        acc_chars += len(token)
        t_end = start_ms + int(span * (acc_chars / total_chars))
        items.append(WordItem(word=token, start_ms=t_start, end_ms=t_end))
    return items


def _extract_words_from_sentence(sentence, chunk_offset_ms: int) -> list[WordItem]:
    """从一句识别结果提取词级时间戳；无 Words 时按该句区间生成伪词兜底。

    兼容不同 SDK/服务端版本的字段名（Words / WordsSnapshot）。
    """
    sent_start = int(sentence.StartMs)
    sent_end = int(sentence.EndMs)
    words = getattr(sentence, "Words", None) or getattr(sentence, "WordsSnapshot", None)

    items: list[WordItem] = []
    if words:
        for w in words:
            items.append(
                WordItem(
                    word=getattr(w, "Word", "") or "",
                    start_ms=sent_start + int(w.OffsetStartMs) + chunk_offset_ms,
                    end_ms=sent_start + int(w.OffsetEndMs) + chunk_offset_ms,
                )
            )
        return items

    return [
        WordItem(
            word=w.word,
            start_ms=w.start_ms + chunk_offset_ms,
            end_ms=w.end_ms + chunk_offset_ms,
        )
        for w in _pseudo_words_from_text(_sentence_text(sentence), sent_start, sent_end)
    ]


def _words_from_result_text(result: str, chunk_offset_ms: int) -> list[WordItem]:
    """ResultDetail 为空时（ResTextFormat=0），从 Result 的句级时间码重建伪词时间轴。"""
    items: list[WordItem] = []
    for match in _RESULT_SENTENCE_RE.finditer(result or ""):
        sent_start = _time_token_to_ms(match.group(1))
        sent_end = _time_token_to_ms(match.group(2))
        text = (match.group(3) or "").strip()
        if not text:
            continue
        for w in _pseudo_words_from_text(text, sent_start, sent_end):
            items.append(
                WordItem(
                    word=w.word,
                    start_ms=w.start_ms + chunk_offset_ms,
                    end_ms=w.end_ms + chunk_offset_ms,
                )
            )
    return items


async def _transcribe_one_chunk(
    client,
    chunk_path: Path,
    chunk_idx: int,
    engine_model_type: str,
) -> list[WordItem]:
    """识别单个分片并加上跨切片毫秒偏移。"""
    raw_bytes = await asyncio.to_thread(chunk_path.read_bytes)
    base64_str = base64.b64encode(raw_bytes).decode("ascii")
    data_len = len(raw_bytes)
    logger.info(
        "分片 %02d 提交 ASR: %s (%.2f MB)",
        chunk_idx, chunk_path.name, data_len / 1024 / 1024,
    )

    task_id = await asyncio.to_thread(
        _create_task_sync, client, base64_str, data_len, engine_model_type
    )
    logger.info("分片 %02d ASR TaskId=%s", chunk_idx, task_id)

    deadline = time.monotonic() + POLL_TIMEOUT_SEC
    while True:
        resp = await asyncio.to_thread(_describe_task_sync, client, task_id)
        data = resp.Data
        status = int(getattr(data, "Status", -1))
        if status == 2:
            break
        if status == 3:
            err_msg = getattr(data, "ErrorMsg", "") or getattr(data, "StatusStr", "")
            raise TencentASRError(
                f"分片 {chunk_idx} (TaskId={task_id}) 识别失败: {err_msg}"
            )
        if time.monotonic() > deadline:
            raise TencentASRError(
                f"分片 {chunk_idx} (TaskId={task_id}) 轮询超过 {POLL_TIMEOUT_SEC}s 未完成"
            )
        await asyncio.sleep(POLL_INTERVAL_SEC)

    sentences = getattr(data, "ResultDetail", None) or []
    words: list[WordItem] = []
    if sentences:
        for sentence in sentences:
            words.extend(_extract_words_from_sentence(sentence, chunk_idx * CHUNK_OFFSET_MS))
    else:
        # ResTextFormat=0 的降级路径：ResultDetail 为空，用 Result 的句级时间码重建
        words = _words_from_result_text(
            getattr(data, "Result", "") or "", chunk_idx * CHUNK_OFFSET_MS
        )
        if words:
            logger.info("分片 %02d 无 ResultDetail，已从 Result 句级时间码重建时间轴", chunk_idx)
    logger.info("分片 %02d 识别完成: %d 句 -> %d 词", chunk_idx, len(sentences), len(words))
    return words


class TencentBatchASR:
    """并发识别所有 600s 分片并按 chunk_idx 顺序缝合为全局词级时间轴。"""

    def __init__(self, engine_model_type: str = "16k_ja"):
        self.engine_model_type = engine_model_type

    async def transcribe_chunks(self, chunk_paths: list[Path]) -> list[WordItem]:
        if not chunk_paths:
            raise TencentASRError("没有可识别的音频分片")
        client = await asyncio.to_thread(_build_client)
        semaphore = asyncio.Semaphore(ASR_CONCURRENCY)

        async def _guarded(idx: int, path: Path) -> list[WordItem]:
            async with semaphore:
                return await _transcribe_one_chunk(
                    client, path, idx, self.engine_model_type
                )

        try:
            results = await asyncio.gather(
                *(_guarded(idx, path) for idx, path in enumerate(chunk_paths))
            )
        finally:
            # 无论成败都清理已提交的临时分片，防止磁盘堆积
            for path in chunk_paths:
                try:
                    await asyncio.to_thread(path.unlink)
                except FileNotFoundError:
                    pass
                except OSError as err:
                    logger.warning("删除分片 %s 失败(忽略): %s", path.name, err)

        merged: list[WordItem] = []
        for chunk_words in results:  # gather 保持提交顺序，即 chunk_idx 顺序
            merged.extend(chunk_words)
        if not merged:
            raise TencentASRError("ASR 完成但未识别到任何词条（音频可能是纯音乐/静音）")
        return merged
