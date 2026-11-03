"""B 站元数据抓取：BV 号解析（含 b23.tv 短链展开）、标题/简介/标签/热评。"""

import logging
import re

import httpx

from src.schemas import BiliMeta

logger = logging.getLogger("bili-postwatch.bili_meta")

BVID_RE = re.compile(r"BV[0-9A-Za-z]{10}")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bilibili.com/",
}

_TIMEOUT = httpx.Timeout(15.0)


async def _get_json(client: httpx.AsyncClient, url: str) -> dict:
    resp = await client.get(url, headers=_HEADERS)
    resp.raise_for_status()
    return resp.json()


async def extract_bvid(text: str) -> str:
    """从任意输入（URL / 纯 BV 号 / 含短链的文本）中解析出 BV 号。

    b23.tv 短链会先跟随重定向拿到真实 URL 再做正则匹配。
    """
    if not text:
        raise ValueError("输入为空，无法解析 BV 号")
    match = BVID_RE.search(text)
    if match:
        return match.group()

    if "b23.tv" in text:
        url = text if text.startswith("http") else f"https://{text}"
        async with httpx.AsyncClient(follow_redirects=True, timeout=_TIMEOUT) as client:
            resp = await client.get(url, headers=_HEADERS)
            resp.raise_for_status()
            # 依次检查最终 URL、重定向链上每一跳、以及响应体里内嵌的地址
            candidates = [str(resp.url)] + [str(h.url) for h in resp.history] + [resp.text]
            for cand in candidates:
                match = BVID_RE.search(cand)
                if match:
                    return match.group()

    raise ValueError(f"无法从输入中解析 BV 号: {text}")


async def _fetch_tags(client: httpx.AsyncClient, bvid: str) -> list[str]:
    """拉取视频标签；该接口失败不阻塞主流程，返回空列表。"""
    try:
        payload = await _get_json(
            client, f"https://api.bilibili.com/x/tag/archive/tags?bvid={bvid}"
        )
        if payload.get("code") != 0 or not isinstance(payload.get("data"), list):
            return []
        tags = [
            item.get("tag_name", "").strip()
            for item in payload["data"]
            if isinstance(item, dict) and item.get("tag_name", "").strip()
        ]
        return tags[:20]
    except Exception as err:  # noqa: BLE001 - 标签属于锦上添花信息
        logger.warning("拉取标签失败(忽略): %s", err)
        return []


async def _fetch_hot_comments(client: httpx.AsyncClient, aid: int) -> list[str]:
    """拉取前 5 条热评（mode=3 热门排序），供 LLM 识别野生译名与梗。"""
    try:
        payload = await _get_json(
            client,
            f"https://api.bilibili.com/x/v2/reply/main?type=1&oid={aid}&mode=3",
        )
        data = payload.get("data") or {}
        replies = data.get("replies") or []
        comments = []
        for reply in replies:
            message = ((reply.get("content") or {}).get("message") or "").strip()
            if message:
                comments.append(message)
            if len(comments) >= 5:
                break
        return comments
    except Exception as err:  # noqa: BLE001 - 热评属于锦上添花信息
        logger.warning("拉取热评失败(忽略): %s", err)
        return []


async def fetch_bili_meta(url_or_bvid: str) -> BiliMeta:
    """抓取视频标题、简介、标签与热评，组装成 BiliMeta。"""
    bvid = await extract_bvid(url_or_bvid)
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        payload = await _get_json(
            client, f"https://api.bilibili.com/x/web-interface/view?bvid={bvid}"
        )
        if payload.get("code") != 0 or not isinstance(payload.get("data"), dict):
            raise RuntimeError(
                f"B站视频信息接口返回错误: code={payload.get('code')} "
                f"message={payload.get('message')}"
            )
        data = payload["data"]
        aid = data.get("aid")
        title = (data.get("title") or "").strip()
        desc = (data.get("desc") or "").strip()
        tags = await _fetch_tags(client, bvid)
        comments = await _fetch_hot_comments(client, aid) if aid else []

    return BiliMeta(bvid=bvid, title=title, desc=desc, tags=tags, top_comments=comments)
