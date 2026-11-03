"""环境变量加载、路径配置、LLM 客户端与外部二进制解析。"""

import logging
import os
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent

# .env 放在项目根目录；不存在时静默跳过（所有配置回退到默认值）
load_dotenv(BASE_DIR / ".env")

# ---------------------------------------------------------------------------
# 路径配置
# ---------------------------------------------------------------------------
DATA_DIR = BASE_DIR / "data"
TEMP_AUDIO_DIR = BASE_DIR / "temp_audio"
DATA_DIR.mkdir(parents=True, exist_ok=True)
TEMP_AUDIO_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "episodes.db"

# ---------------------------------------------------------------------------
# 腾讯云 ASR
# ---------------------------------------------------------------------------
TENCENT_SECRET_ID = os.getenv("TENCENT_SECRET_ID", "")
TENCENT_SECRET_KEY = os.getenv("TENCENT_SECRET_KEY", "")
TENCENT_ASR_REGION = os.getenv("TENCENT_ASR_REGION", "ap-shanghai")

# ---------------------------------------------------------------------------
# LLM（OpenAI 兼容接口：腾讯混元 / DeepSeek / 任意中转）
# ---------------------------------------------------------------------------
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.hunyuan.cloud.tencent.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "hunyuan-turbos-latest")

# ---------------------------------------------------------------------------
# B 站下载
# ---------------------------------------------------------------------------
BILI_COOKIES_FILE = os.getenv("BILI_COOKIES_FILE", "")

# 扫码登录凭证的本地持久化文件（长期保存，data/ 已被 .gitignore 排除）。
# 结构与保存时机见 src/extractor/bili_auth.py
BILI_CREDENTIALS_FILE = Path(
    os.getenv("BILI_CREDENTIALS_FILE", str(DATA_DIR / "bili_credentials.json"))
)

# CreateRecTask SourceType=1 (Base64 直传) 的单分片硬上限
MAX_CHUNK_BYTES = 5 * 1024 * 1024

logger = logging.getLogger("bili-postwatch")

# ---------------------------------------------------------------------------
# 外部二进制解析：ffmpeg / yt-dlp
# 优先级：FFMPEG_PATH 环境变量 > 系统 PATH > 项目 tools/ffmpeg/bin 兜底
# ---------------------------------------------------------------------------
TOOLS_FFMPEG = BASE_DIR / "tools" / "ffmpeg" / "bin" / "ffmpeg.exe"


def resolve_ffmpeg_path() -> str:
    """返回可用的 ffmpeg 可执行文件路径，找不到时抛出带安装指引的异常。"""
    override = os.getenv("FFMPEG_PATH", "")
    candidates = [override] if override else []
    which = shutil.which("ffmpeg")
    if which:
        candidates.append(which)
    candidates.append(str(TOOLS_FFMPEG))
    for cand in candidates:
        if cand and Path(cand).is_file():
            return str(Path(cand).resolve())
    raise RuntimeError(
        "未找到 ffmpeg。请将其安装到系统 PATH，或通过环境变量 FFMPEG_PATH 指定可执行文件路径。"
    )


def resolve_ytdlp_cmd() -> list[str]:
    """返回 yt-dlp 启动命令。

    系统 PATH 中有 yt-dlp 可执行文件则直接使用；
    否则回退到 `python -m yt_dlp`（规约允许通过 Python 库方式调用）。
    """
    which = shutil.which("yt-dlp")
    if which:
        return [which]
    return [sys.executable, "-m", "yt_dlp"]


# ---------------------------------------------------------------------------
# LLM 客户端（AsyncOpenAI，兼容所有 OpenAI 协议端点）
# ---------------------------------------------------------------------------
_llm_client = None


def get_llm_client():
    """返回缓存的 AsyncOpenAI 客户端；未配置 LLM_API_KEY 时返回 None。"""
    global _llm_client
    if not LLM_API_KEY:
        return None
    if _llm_client is None:
        from openai import AsyncOpenAI

        _llm_client = AsyncOpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL or None)
    return _llm_client


def require_llm_client():
    """返回 AsyncOpenAI 客户端；未配置密钥时抛出带配置指引的异常。"""
    client = get_llm_client()
    if client is None:
        raise RuntimeError(
            "未配置 LLM_API_KEY。请在项目根目录复制 .env.example 为 .env 并填写 LLM_API_KEY（"
            "默认对接腾讯混元 OpenAI 兼容端点，也可换成任意 DeepSeek/Claude 中转地址）。"
        )
    return client


async def chat_text(
    client,
    messages: list[dict],
    temperature: float = 0.1,
    json_mode: bool = False,
    max_retries: int = 1,
) -> str:
    """统一的 LLM 对话封装，返回首条回复文本。

    json_mode=True 时请求 response_format={"type": "json_object"}；
    部分兼容端点不支持该参数（返回 4xx），此时自动去掉该参数重试一次。
    网络/服务端瞬时错误按 max_retries 退避重试。
    """
    import asyncio

    kwargs: dict = {"model": LLM_MODEL, "messages": messages, "temperature": temperature}
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    last_err: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            resp = await client.chat.completions.create(**kwargs)
            return resp.choices[0].message.content or ""
        except Exception as err:  # noqa: BLE001 - 需区分"不支持 response_format"与瞬时错误
            last_err = err
            # 仅当端点明确拒绝 response_format（400 + 参数相关）时才降级，
            # 网络超时等瞬时错误保持 JSON 模式重试，避免误降级。
            msg = str(err).lower()
            status = getattr(err, "status_code", None)
            rejected_format = json_mode and status == 400 and (
                "response_format" in msg or "json_object" in msg
            )
            if rejected_format:
                kwargs.pop("response_format", None)
                json_mode = False
                continue
            if attempt < max_retries:
                await asyncio.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"LLM 调用失败: {last_err}") from last_err


def configure_logging() -> None:
    """MCP stdio 模式下 stdout 是协议通道，日志必须走 stderr。

    同时落盘到 data/server.log（5MB×3 轮转），便于排查"工具调用卡住"这类运行时问题。
    """
    from logging.handlers import RotatingFileHandler

    root = logging.getLogger()
    if root.handlers:
        return
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(fmt)
    root.addHandler(stream)

    file_handler = RotatingFileHandler(
        DATA_DIR / "server.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)
