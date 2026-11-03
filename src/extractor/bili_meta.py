"""B 站元数据抓取：BV 号解析（含 b23.tv 短链展开）、标题/简介/标签/热评。"""

import logging
import re
from urllib.parse import parse_qs, urlparse

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


def _page_from_text(text: str) -> int:
    """从 URL 中解析分 P 序号（?p=2），非 URL 或无该参数时返回 1。"""
    try:
        raw = parse_qs(urlparse(text).query).get("p", ["1"])[0]
        return max(1, int(raw))
    except (TypeError, ValueError):
        return 1


async def resolve_bili_input(text: str) -> tuple[str, int, str]:
    """解析用户输入，返回 (bvid, 分P序号, 可直接下载的完整 URL)。

    支持完整链接（含 ?p=N 分 P 参数）、纯 BV 号、b23.tv 短链。
    """
    if not text:
        raise ValueError("输入为空，无法解析 BV 号")

    match = BVID_RE.search(text)
    if match:
        bvid = match.group()
        page = _page_from_text(text)
        url = f"https://www.bilibili.com/video/{bvid}"
        if page > 1:
            url += f"?p={page}"
        return bvid, page, url

    if "b23.tv" in text:
        url = text if text.startswith("http") else f"https://{text}"
        async with httpx.AsyncClient(follow_redirects=True, timeout=_TIMEOUT) as client:
            resp = await client.get(url, headers=_HEADERS)
            resp.raise_for_status()
            # 优先用最终落地 URL（自带 ?p=N），再检查重定向链与响应体内嵌地址
            final_url = str(resp.url)
            for cand in [final_url] + [str(h.url) for h in resp.history] + [resp.text]:
                match = BVID_RE.search(cand)
                if match:
                    page = _page_from_text(cand)
                    if BVID_RE.search(final_url):
                        download_url = final_url
                    else:
                        download_url = f"https://www.bilibili.com/video/{match.group()}"
                        if page > 1:
                            download_url += f"?p={page}"
                    return match.group(), page, download_url

    raise ValueError(f"无法从输入中解析 BV 号: {text}")


async def extract_bvid(text: str) -> str:
    """从任意输入中解析出 BV 号（兼容既有调用方）。"""
    bvid, _, _ = await resolve_bili_input(text)
    return bvid


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
