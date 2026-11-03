"""音频下载与定长分片（下载逻辑参考 Bili23-Downloader，仅保留音频模式）。

流程对齐参考项目（仅音频 = DownloadType.AUDIO 单独成流，不合并视频轨）：
1. WBI 签名请求 playurl（x/player/wbi/playurl，fnval=4048，参考 parse/parser/base.build_video_info_url），
   凭证来自扫码登录后长期保存在本地的 data/bili_credentials.json（见 bili_auth）；
2. 从 dash.audio 选取音轨：ASR 场景取带宽最小的标准音轨，质量 ID 语义按参考
   download/parse/audio_info.py（30216=64K, 30232=132K, 30280=192K, 30250=杜比, 30251=Hi-Res）；
3. 候选直链按参考 network/cdn.CDN.filter 剔除 mcdn/pcdn 等劣质节点并去重，
   HEAD / Range GET 探测（参考 network/download_url.resolve_download_url）；
4. 下载按参考 downloader.Downloader：4MB 分块 Range 并发（download_thread 默认 4），
   校验响应 Content-Range 起点与总长，单分块自动重试；
5. 直链不可用时回退 yt-dlp（同样只取音轨），凭证自动转 Netscape cookies 注入；
6. 只跑一次 FFmpeg，同时完成 16kHz/单声道/48kbps 转码与 600 秒切片，
   每片约 3.4MB，安全低于腾讯云 SourceType=1 的 5MB 直传上限。
"""

import asyncio
import logging
import re
import shutil
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx

from src import config
from src.extractor import bili_auth

logger = logging.getLogger("bili-postwatch.audio_chunker")

CHUNK_SECONDS = 600
_DOWNLOAD_TIMEOUT_SEC = 180.0   # yt-dlp 兜底上限，避免慢速 PCDN 长时间挂住整次工具调用
_FFMPEG_TIMEOUT_SEC = 180.0

# 参考项目 network/download_url.py
_PROBE_TIMEOUT = 5.0            # 单次探测的连接/读写超时
_PROBE_MIN_FILE_SIZE = 1024     # 探测认定有效的最小文件体积
_PROBE_CONCURRENCY = 4          # 同时在途的探测请求数

# 参考项目 downloader.Downloader：分块体积 4MB，并发分块数 = download_thread 默认 4
_RANGE_CHUNK_SIZE = 4 * 1024 * 1024
_RANGE_CONCURRENCY = 4
_RANGE_MAX_ATTEMPTS = 3

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bilibili.com/",
}

# 参考项目 network/cdn.py CDN.filter：按主机名剔除劣质节点（不整串匹配，避免误杀查询参数）
_CDN_BLACKLIST = [
    "mcdn",
    "pcdn",
    "szbdyd.com",
    "mountaintoys.cn",
    "upos-sz-mirror14b.bilivideo.com",
    "nexusedgeio.com",
    "ahdohpiechei.com",
]

# 参考项目 download/parse/audio_info.py：音轨质量 ID -> 名称（仅用于日志）
_AUDIO_QUALITY_NAMES = {
    30216: "64K",
    30232: "132K",
    30280: "192K",
    30250: "杜比全景声",
    30251: "Hi-Res 无损",
}

# yt-dlp 兜底参数：只要音轨、限低码率、多线程拉分片
_YTDLP_BASE = [
    "--no-playlist",
    "-f", "ba[abr<=64]/ba/worst",
    "--concurrent-fragments", "8",
    "--socket-timeout", "15",
]


def work_dir_for(bvid: str, page: int = 1) -> Path:
    name = bvid if page <= 1 else f"{bvid}_p{page}"
    return config.TEMP_AUDIO_DIR / name


def cleanup_workdir(bvid: str, page: int = 1) -> None:
    """删除该视频（分P）的整个临时目录（含残留分片）。"""
    shutil.rmtree(work_dir_for(bvid, page), ignore_errors=True)


async def _run_command(cmd: list[str], timeout: float) -> None:
    """异步执行外部命令，带超时保护；非零退出码抛出带 stderr 尾部的异常。"""
    logger.info("执行命令: %s", " ".join(str(c) for c in cmd))
    proc = await asyncio.create_subprocess_exec(
        *[str(c) for c in cmd],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"命令超时({timeout:.0f}s): {cmd[0]} ...") from None
    if proc.returncode != 0:
        tail = (stderr or stdout).decode(errors="replace")[-2000:]
        raise RuntimeError(f"命令失败 (exit={proc.returncode}): {cmd[0]} ...\n{tail}")


def _candidate_cdns(audio_stream: dict) -> list[str]:
    """展平音轨的全部直链候选并剔除劣质节点（参考 QueryWorker.get_download_urls + CDN.filter）。

    保留多个候选而不是只挑一个：单个 CDN 节点偶发超时/403 时可以就地换线重试，
    避免整条流水线掉进慢速 yt-dlp 兜底。
    """
    urls: list[str] = []
    for key in ("baseUrl", "base_url", "backupUrl", "backup_url", "url"):
        value = audio_stream.get(key)
        if isinstance(value, list):
            urls.extend(value)
        elif isinstance(value, str):
            urls.append(value)
    urls = list(dict.fromkeys(u for u in urls if u))
    if not urls:
        raise RuntimeError("playurl 未返回可用音频直链")

    filtered = [
        u for u in urls
        if not any(domain in _netloc(u) for domain in _CDN_BLACKLIST)
    ]
    if not filtered:
        # 参考项目 CDN.filter：少数视频只有 PCDN 链接，全部过滤会无链接可试，
        # 退回原始列表，劣质链接也好过没有链接
        logger.warning("音频直链全部命中劣质节点黑名单，退回未过滤列表")
        filtered = urls
    return filtered


def _netloc(url: str) -> str:
    try:
        return urlparse(url).netloc
    except ValueError:
        return ""


def _extract_file_size(headers: httpx.Headers) -> int:
    """从响应头提取文件大小；text/json 响应视为无效（参考 download_url._extract_file_size）。"""
    content_type = headers.get("Content-Type", "").lower()
    if not content_type or "text" in content_type or "json" in content_type:
        return 0
    content_range = headers.get("Content-Range", "")
    range_total = content_range.rpartition("/")[2].strip()
    if range_total.isdigit():
        return int(range_total)
    content_length = headers.get("Content-Length", "")
    if str(content_length).isdigit():
        return int(content_length)
    return 0


async def _probe_url(client: httpx.AsyncClient, url: str) -> int:
    """探测单个候选直链，返回文件大小（无效返回 0）。

    与参考 download_url._probe_url/_probe_with_head/_probe_with_range_get 对齐：
    HEAD 优先；405 或取不到有效大小时补一次 Range GET bytes=0-0。
    CDN 鉴权信息全在查询参数里，探测不带 Cookie（参考 download_url 注释）。
    """
    try:
        resp = await client.head(url, headers=_HEADERS)
        if resp.status_code == 405:
            return await _probe_with_range_get(client, url)
        resp.raise_for_status()
        file_size = _extract_file_size(resp.headers)
        if file_size > _PROBE_MIN_FILE_SIZE:
            return file_size
        return await _probe_with_range_get(client, url)
    except httpx.HTTPStatusError as err:
        logger.warning("探测失败 %s: HTTP %s", _netloc(url), err.response.status_code)
        return 0
    except Exception as err:  # noqa: BLE001 - 单节点失败换下一个候选
        logger.warning("探测失败 %s: %s", _netloc(url), type(err).__name__)
        return 0


async def _probe_with_range_get(client: httpx.AsyncClient, url: str) -> int:
    headers = {**_HEADERS, "Range": "bytes=0-0"}
    async with client.stream("GET", url, headers=headers) as resp:
        resp.raise_for_status()
        return _extract_file_size(resp.headers)


async def _resolve_download_url(
    client: httpx.AsyncClient, candidates: list[str]
) -> tuple[str, int]:
    """并发探测候选直链，按候选顺序取第一个有效者（参考 resolve_download_url 的顺序语义）。"""
    sem = asyncio.Semaphore(_PROBE_CONCURRENCY)

    async def _probe(index: int, url: str) -> tuple[int, str, int]:
        async with sem:
            return index, url, await _probe_url(client, url)

    for index, url, size in await asyncio.gather(
        *(_probe(i, u) for i, u in enumerate(candidates))
    ):
        if size > _PROBE_MIN_FILE_SIZE:
            return url, size

    raise RuntimeError(
        f"全部 {len(candidates)} 个音频直链候选均探测失败"
    )


async def _fetch_cid(client: httpx.AsyncClient, bvid: str, page: int) -> int | None:
    view = await client.get(
        f"https://api.bilibili.com/x/web-interface/view?bvid={bvid}",
        headers=bili_auth.api_headers(),
    )
    payload = view.json()
    if payload.get("code") != 0:
        logger.warning("view 接口返回错误 code=%s: %s", payload.get("code"), payload.get("message"))
        return None
    data = payload["data"]
    pages = data.get("pages") or []
    if 0 < page <= len(pages):
        return int(pages[page - 1]["cid"])
    cid = data.get("cid")
    return int(cid) if cid else None


async def _fetch_dash_audio(client: httpx.AsyncClient, bvid: str, page: int) -> dict | None:
    """WBI 签名请求 playurl，返回带宽最小的标准音轨节点（仅音频模式）。

    请求参数与参考 build_video_info_url 一致（bvid/cid/qn/fnver=0/fnval=4048/fourk=1）；
    fnval=4048 使接口返回 DASH 流，音轨在 dash.audio 数组中。
    """
    cid = await _fetch_cid(client, bvid, page)
    if not cid:
        return None

    wbi = await bili_auth.ensure_wbi_keys(client)
    params = {
        "bvid": bvid,
        "cid": cid,
        "qn": 16,  # 仅音频模式不消费 qn；参考项目同样恒带该参数（取最低档 16）
        "fnver": 0,
        "fnval": 4048,
        "fourk": 1,
    }
    query = bili_auth.enc_wbi(params, wbi["img_key"], wbi["sub_key"])
    play = await client.get(f"{bili_auth.VIDEO_PLAYURL_API}?{query}", headers=bili_auth.api_headers())
    play.raise_for_status()
    payload = play.json()
    if payload.get("code") != 0:
        raise RuntimeError(
            f"playurl 接口返回错误 code={payload.get('code')} message={payload.get('message')}"
        )
    dash = (payload.get("data") or {}).get("dash") or {}

    audios = list(dash.get("audio") or [])
    if not audios:
        return None

    # 音轨质量 ID 语义按参考 AudioInfoParser（flac/dolby 不参与 ASR 选轨，仅记日志）
    for entry in audios:
        quality_id = entry.get("id")
        if quality_id in _AUDIO_QUALITY_NAMES:
            logger.info("可用音轨: %s (%s)", quality_id, _AUDIO_QUALITY_NAMES[quality_id])

    # ASR 场景取带宽最小音轨（64kbps，24 分钟约 11MB，转码素材足够）
    audios.sort(key=lambda a: a.get("bandwidth", 1 << 30))
    return audios[0]


async def _download_range_chunks(
    client: httpx.AsyncClient,
    url: str,
    out_path: Path,
    file_size: int,
) -> None:
    """按 4MB 分块 Range 并发下载（参考 downloader.Downloader 的分块 + Content-Range 校验）。"""
    chunk_bounds = [
        (start, min(start + _RANGE_CHUNK_SIZE, file_size) - 1)
        for start in range(0, file_size, _RANGE_CHUNK_SIZE)
    ]
    sem = asyncio.Semaphore(_RANGE_CONCURRENCY)

    async def _chunk(index: int, start: int, end: int) -> None:
        async with sem:
            headers = {**_HEADERS, "Range": f"bytes={start}-{end}"}
            expect = end - start + 1
            last_err: Exception | None = None
            for attempt in range(1, _RANGE_MAX_ATTEMPTS + 1):
                try:
                    resp = await client.get(url, headers=headers)
                    if resp.status_code != 206:
                        raise RuntimeError(f"期望 206，实际 HTTP {resp.status_code}")
                    # 参考项目 downloader._validate_response：206 必须带有效 Content-Range，
                    # 且区间起点、文件总长都要与请求/探测值一致
                    match = re.match(
                        r"bytes\s+(\d+)-(\d+)/(\d+)", resp.headers.get("Content-Range", "")
                    )
                    if not match:
                        raise RuntimeError(
                            f"响应缺少有效 Content-Range（{resp.headers.get('Content-Range')!r}）"
                        )
                    range_start, _range_end, range_total = (int(g) for g in match.groups())
                    if range_start != start:
                        raise RuntimeError(f"Content-Range 起点不符（请求 {start}，实际 {range_start}）")
                    if range_total != file_size:
                        raise RuntimeError(
                            f"响应文件总长与探测不符（探测 {file_size}，实际 {range_total}）"
                        )
                    data = resp.content
                    if len(data) != expect:
                        raise RuntimeError(f"分块字节数不符（期望 {expect}，实际 {len(data)}）")
                    with open(out_path, "r+b") as fh:
                        fh.seek(start)
                        fh.write(data)
                    logger.debug("分块 %d/%d 完成", index + 1, len(chunk_bounds))
                    return
                except Exception as err:  # noqa: BLE001 - 单分块重试，最终失败交上层换线
                    last_err = err
                    logger.warning(
                        "分块 %d 第 %d/%d 次尝试失败: %s",
                        index + 1, attempt, _RANGE_MAX_ATTEMPTS, err,
                    )
                    if attempt < _RANGE_MAX_ATTEMPTS:
                        await asyncio.sleep(1.0 * attempt)
            raise RuntimeError(f"分块 {index + 1} 下载失败: {last_err}")

    await asyncio.gather(*(_chunk(i, s, e) for i, (s, e) in enumerate(chunk_bounds)))


async def _try_direct_dash_download(url: str, bvid: str, out_path: Path) -> bool:
    """走 WBI 签名 playurl 直链下载纯音轨（仅音频模式，参考项目下载逻辑）。"""
    try:
        page = max(1, int(parse_qs(urlparse(url).query).get("p", ["1"])[0]))
        timeout = httpx.Timeout(_PROBE_TIMEOUT, read=30.0, pool=_PROBE_TIMEOUT)
        async with httpx.AsyncClient(
            headers=_HEADERS, follow_redirects=True, timeout=timeout,
            transport=httpx.AsyncHTTPTransport(retries=0),
        ) as client:
            audio_stream = await _fetch_dash_audio(client, bvid, page)
            if not audio_stream:
                logger.warning("playurl 未返回 DASH 音轨，回退 yt-dlp")
                return False

            quality_id = audio_stream.get("id")
            logger.info(
                "选定音轨 %s (%s)，带宽 %s kbps",
                quality_id,
                _AUDIO_QUALITY_NAMES.get(quality_id, "标准"),
                round((audio_stream.get("bandwidth") or 0) / 1000),
            )
            candidates = _candidate_cdns(audio_stream)
            fast_url, file_size = await _resolve_download_url(client, candidates)
            logger.info("探测命中 %s（%.2f MB）", _netloc(fast_url), file_size / 1024 / 1024)

            # 预建空文件，分块按 Range 起点写入
            with open(out_path, "wb") as fh:
                fh.truncate(file_size)
            await _download_range_chunks(client, fast_url, out_path, file_size)

            actual = out_path.stat().st_size
            if actual != file_size:
                raise RuntimeError(f"下载体积不符（探测 {file_size}，实际 {actual}）")
            logger.info("直链下载完成: %.2f MB", actual / 1024 / 1024)
            return True
    except Exception as err:  # noqa: BLE001 - 直链失败一律回退 yt-dlp
        logger.warning("B 站直链快拉未命中(%s)，回退多线程 yt-dlp", err)
        out_path.unlink(missing_ok=True)
        return False


async def _ytdlp_download(url: str, bvid: str, work_dir: Path) -> Path:
    cmd = config.resolve_ytdlp_cmd() + _YTDLP_BASE + [
        "-o", str(work_dir / f"{bvid}_raw.%(ext)s"),
        url,
    ]
    # 凭证注入优先级：BILI_COOKIES_FILE 显式指定 > 扫码登录凭证自动转换
    cookies = config.BILI_COOKIES_FILE
    if not (cookies and Path(cookies).is_file()):
        cookies = bili_auth.write_ytdlp_cookies() or ""
    if cookies and Path(cookies).is_file():
        cmd.extend(["--cookies", str(Path(cookies).resolve())])
        logger.info("yt-dlp 使用 Cookie 文件: %s", cookies)
    await _run_command(cmd, timeout=_DOWNLOAD_TIMEOUT_SEC)

    candidates = [p for p in work_dir.glob(f"{bvid}_raw.*") if p.is_file()]
    if not candidates:
        raise RuntimeError(f"yt-dlp 未产出音频文件，工作目录: {work_dir}")
    return candidates[0]


async def _slice_audio(ffmpeg_path: str, raw_audio: Path, work_dir: Path, bvid: str) -> None:
    """单次 FFmpeg 完成重采样（16kHz/单声道/48kbps）与 600 秒切片。"""
    await _run_command([
        ffmpeg_path, "-y",
        "-i", str(raw_audio),
        "-vn", "-ar", "16000", "-ac", "1", "-b:a", "48k",
        "-f", "segment",
        "-segment_time", str(CHUNK_SECONDS),
        str(work_dir / f"{bvid}_chunk_%03d.mp3"),
    ], timeout=_FFMPEG_TIMEOUT_SEC)


async def download_and_chunk(url: str, bvid: str, page: int = 1) -> list[Path]:
    """下载 -> 转码 -> 600s 切片，返回按序号排序的分片路径列表。

    每次运行使用独立的 run_<毫秒> 子目录：即使上一次运行的服务进程被中止、
    残留文件被占用删不掉，也不会污染本次的切片集合。
    任一环节失败时清理残留文件后向上抛异常。
    """
    ffmpeg_path = config.resolve_ffmpeg_path()
    base_dir = work_dir_for(bvid, page)
    shutil.rmtree(base_dir, ignore_errors=True)  # 尽力清理历史残留（被占用时忽略）
    work_dir = base_dir / f"run_{int(time.time() * 1000)}"
    work_dir.mkdir(parents=True, exist_ok=True)

    try:
        raw_audio = work_dir / f"{bvid}_raw.m4s"
        from_direct = await _try_direct_dash_download(url, bvid, raw_audio)
        if not from_direct:
            raw_audio = await _ytdlp_download(url, bvid, work_dir)

        try:
            await _slice_audio(ffmpeg_path, raw_audio, work_dir, bvid)
        except RuntimeError:
            if not from_direct:
                raise
            # 直链产物无法解码（偶发 CDN 返回错误页），回退 yt-dlp 重下一次
            logger.warning("直链产物无法解码，回退 yt-dlp 重新下载")
            raw_audio.unlink(missing_ok=True)
            raw_audio = await _ytdlp_download(url, bvid, work_dir)
            await _slice_audio(ffmpeg_path, raw_audio, work_dir, bvid)

        raw_audio.unlink(missing_ok=True)
        chunks = sorted(work_dir.glob(f"{bvid}_chunk_*.mp3"))
        if not chunks:
            raise RuntimeError("FFmpeg 切片失败：未生成任何分片文件")

        for idx, chunk in enumerate(chunks):
            size = chunk.stat().st_size
            if size >= config.MAX_CHUNK_BYTES:
                raise RuntimeError(
                    f"分片 {chunk.name} 体积 {size / 1024 / 1024:.2f}MB 超过腾讯云 5MB 直传上限"
                )
            logger.info("分片 %02d: %s (%.2f MB)", idx, chunk.name, size / 1024 / 1024)
        return chunks
    except Exception:
        cleanup_workdir(bvid, page)
        raise
