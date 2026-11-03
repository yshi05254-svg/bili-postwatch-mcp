"""B 站扫码登录与凭证本地持久化（参考 Bili23-Downloader 的 auth / network 实现）。

流程对齐参考项目 src/util/auth/qrcode.py：
1. GET passport.bilibili.com/x/passport-login/web/qrcode/generate -> data.url + data.qrcode_key
2. 轮询 .../qrcode/poll?qrcode_key=...，data.code：
   86101 等待扫码 / 86090 已扫码待确认 / 0 登录成功（凭证随响应 Set-Cookie 下发）/ 86038 二维码过期
3. 成功后取 SESSDATA / bili_jct / DedeUserID / DedeUserID__ckMd5（参考 cookie_login.LOGIN_COOKIE_KEYS），
   并通过 x/web-interface/nav 校验 isLogin、回写 WBI 签名密钥（参考 auth/base.store_wbi_keys），
   通过 x/frontend/finger/spi 获取 buvid3/buvid4（参考 auth/cookie.get_buvid）。

与参考项目的差异（MCP 服务形态适配，均非流程改动）：
- 凭证不进 GUI 配置文件，而是长期落盘到 data/bili_credentials.json；
- 二维码以半块字符渲染为文本（另存一份 PNG 兜底），替代 QPixmap 弹窗；
- Cookie 以请求头字符串方式附带，替代全局 httpx cookiejar；
- 未移植参考项目的完整匿名指纹栈（bili_ticket/_uuid/ExClimbWuzhi 激活等），仅保留 buvid。
"""

import asyncio
import json
import logging
import time
from hashlib import md5
from pathlib import Path
from urllib.parse import urlencode

import httpx

from src import config

logger = logging.getLogger("bili-postwatch.bili_auth")

# ---------------------------------------------------------------------------
# 常量（均照搬参考项目，来源见各处注释）
# ---------------------------------------------------------------------------

# 参考项目 src/util/common/enum.py QRCodeScanStatus
QR_WAITING_FOR_SCAN = 86101
QR_WAITING_FOR_CONFIRMATION = 86090
QR_SUCCESS = 0
QR_EXPIRED = 86038

# 参考项目 src/util/auth/cookie_login.py LOGIN_COOKIE_KEYS
LOGIN_COOKIE_KEYS = ("SESSDATA", "bili_jct", "DedeUserID", "DedeUserID__ckMd5")

# 参考项目 src/util/parse/parser/base.py
WBI_API = "https://api.bilibili.com/x/web-interface/nav"
VIDEO_PLAYURL_API = "https://api.bilibili.com/x/player/wbi/playurl"
QR_GENERATE_API = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"
QR_POLL_API = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"
BUVID_SPI_API = "https://api.bilibili.com/x/frontend/finger/spi"

mixinKeyEncTab = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49,
    33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40,
    61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11,
    36, 20, 34, 44, 52
]

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bilibili.com/",
}

_TIMEOUT = httpx.Timeout(15.0)

# 轮询间隔：参考项目 QTimer.start(1000)
POLL_INTERVAL_SEC = 1.0
# check 工具单次调用内最多轮询多久（给用户留出「点确认」的时间窗）
POLL_WINDOW_SEC = 20.0

_pending_path = config.DATA_DIR / "bili_login_pending.json"

# WBI 密钥的进程内缓存（参考项目为 config 内存值，不落盘，理由见 store_wbi_keys 注释）
_wbi_keys: dict = {}


# ---------------------------------------------------------------------------
# 凭证存储
# ---------------------------------------------------------------------------

def _credentials_path() -> Path:
    return Path(config.BILI_CREDENTIALS_FILE)


def load_credentials() -> dict:
    """读取本地持久化的登录凭证；不存在或损坏时返回空 dict。"""
    path = _credentials_path()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        logger.warning("凭证文件损坏，忽略: %s", path)
        return {}


def save_credentials(credentials: dict) -> Path:
    """凭证长期落盘。data/ 已被 .gitignore 排除，不会入库。"""
    path = _credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(credentials, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info("登录凭证已保存到 %s（账号: %s）", path, credentials.get("uname", ""))
    return path


def has_credentials() -> bool:
    return bool(load_credentials().get("SESSDATA"))


def _from_jar(jar: httpx.Cookies) -> dict:
    """从响应 cookiejar 中提取登录 Cookie 键（对应参考项目 update_cookies 的快照读取）。"""
    return {key: jar.get(key, "") for key in LOGIN_COOKIE_KEYS}


# ---------------------------------------------------------------------------
# Cookie 头（对应参考项目 network/request.py get_cookies 的子集）
# ---------------------------------------------------------------------------

def build_cookie_header(logged_in: bool = True, extra: dict | None = None) -> str:
    """组装请求用 Cookie 头。登录态缺失时自动退化为匿名 Cookie。"""
    creds = load_credentials() if logged_in else {}
    pairs: dict[str, str] = {
        "buvid3": creds.get("buvid3", ""),
        "buvid4": creds.get("buvid4", ""),
        "CURRENT_FNVAL": "4048",
        "CURRENT_QUALITY": "0",
    }
    if creds.get("SESSDATA"):
        for key in LOGIN_COOKIE_KEYS:
            pairs[key] = creds.get(key, "")
    if extra:
        pairs.update({k: v for k, v in extra.items() if v})
    return "; ".join(f"{k}={v}" for k, v in pairs.items() if v != "")


def api_headers(logged_in: bool = True) -> dict:
    return {**_HEADERS, "Cookie": build_cookie_header(logged_in=logged_in)}


# ---------------------------------------------------------------------------
# WBI 签名（算法照搬参考项目 parse/parser/base.py 的 enc_wbi）
# ---------------------------------------------------------------------------

def _get_mixin_key(orig: str) -> str:
    return "".join(orig[i] for i in mixinKeyEncTab)[:32]


async def ensure_wbi_keys(client: httpx.AsyncClient | None = None) -> dict:
    """确保 WBI 签名密钥就绪：进程缓存 -> 本地凭证文件 -> nav 接口。

    与参考项目一致：密钥会随每次 nav 响应回写（store_wbi_keys），
    但只在内存/凭证文件里带上这份，过期由下一次 nav 请求兜底刷新。
    """
    if len(_wbi_keys.get("img_key", "")) + len(_wbi_keys.get("sub_key", "")) >= 64:
        return _wbi_keys

    creds = load_credentials()
    if len(creds.get("img_key", "")) + len(creds.get("sub_key", "")) >= 64:
        _wbi_keys.update({"img_key": creds["img_key"], "sub_key": creds["sub_key"]})
        return _wbi_keys

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=_TIMEOUT)
    try:
        resp = await client.get(WBI_API, headers=api_headers())
        resp.raise_for_status()
        store_wbi_keys(resp.json().get("data") or {})
    finally:
        if own_client:
            await client.aclose()

    if not _wbi_keys:
        raise RuntimeError("无法获取 WBI 签名密钥（nav 接口未返回 wbi_img）")
    return _wbi_keys


def store_wbi_keys(data: dict) -> bool:
    """从 nav 响应回写 WBI 密钥（参考项目 auth/base.store_wbi_keys 的落盘版）。"""
    wbi_img = (data or {}).get("wbi_img") or {}
    img_key = Path(wbi_img.get("img_url", "")).stem
    sub_key = Path(wbi_img.get("sub_url", "")).stem
    if not img_key or not sub_key:
        return False
    _wbi_keys.update({"img_key": img_key, "sub_key": sub_key})

    creds_path = _credentials_path()
    if creds_path.is_file():
        creds = load_credentials()
        if creds.get("img_key") != img_key or creds.get("sub_key") != sub_key:
            creds.update({"img_key": img_key, "sub_key": sub_key})
            save_credentials(creds)
    return True


def enc_wbi(params: dict, img_key: str, sub_key: str) -> str:
    """对参数做 WBI 签名，返回可直接拼接的 query string。与参考项目实现逐行对齐。"""
    mixin_key = _get_mixin_key(img_key + sub_key)
    params["wts"] = round(time.time())
    params = dict(sorted(params.items()))
    params = {
        k: "".join(filter(lambda chr: chr not in "!'()*", str(v)))
        for k, v in params.items()
    }
    query = urlencode(params)
    w_rid = md5((query + mixin_key).encode()).hexdigest()
    params["w_rid"] = w_rid
    return urlencode(params)


# ---------------------------------------------------------------------------
# buvid（参考项目 auth/cookie.get_buvid：x/frontend/finger/spi）
# ---------------------------------------------------------------------------

async def ensure_buvid(client: httpx.AsyncClient) -> dict:
    """凭证缺失 buvid3/buvid4 时向 spi 接口补取并写回凭证文件。"""
    creds = load_credentials()
    if creds.get("buvid3") and creds.get("buvid4"):
        return creds

    resp = await client.get(BUVID_SPI_API, headers=api_headers())
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("code") != 0:
        raise RuntimeError(f"获取 buvid 失败: {payload.get('message')}")
    data = payload.get("data") or {}
    creds["buvid3"] = data.get("b_3", "")
    creds["buvid4"] = data.get("b_4", "")
    if creds.get("SESSDATA"):
        save_credentials(creds)
    logger.info("已获取 buvid3/buvid4")
    return creds


# ---------------------------------------------------------------------------
# 扫码登录
# ---------------------------------------------------------------------------

def _render_qrcode_ascii(text: str) -> str:
    """把二维码渲染为半块字符文本（参考项目 _build_qrcode_pixmap 的矩阵逻辑改为字符输出）。

    半块字符按终端/聊天主题不同可能呈反相，故二维码另存 PNG 兜底（见 _save_qrcode_png）。
    """
    import qrcode

    qr = qrcode.QRCode(border=4)
    qr.add_data(text)
    qr.make(fit=True)
    matrix = qr.get_matrix()

    fill = "██"
    blank = "  "
    lines = []
    # 顶部留白（奇偶对齐：矩阵行数可能为奇数，最后一行按半块处理）
    lines.append(blank * (len(matrix[0]) + 2))
    for row in range(0, len(matrix), 2):
        line = []
        for col in range(len(matrix[row])):
            top = matrix[row][col]
            bottom = matrix[row + 1][col] if row + 1 < len(matrix) else False
            if top and bottom:
                line.append(fill)
            elif top:
                line.append("▀▀")
            elif bottom:
                line.append("▄▄")
            else:
                line.append(blank)
        lines.append(blank + "".join(line) + blank)
    lines.append(blank * (len(matrix[0]) + 2))
    return "\n".join(lines)


def _save_qrcode_png(text: str) -> Path:
    """二维码 PNG 兜底：白底黑块（与参考项目 QPixmap 白底黑刷一致），颜色不受主题影响。"""
    import qrcode

    qr = qrcode.QRCode(border=4, box_size=10)
    qr.add_data(text)
    qr.make(fit=True)
    out = config.DATA_DIR / "bili_login_qrcode.png"
    img = qr.make_image(fill_color="black", back_color="white")
    img.save(out)
    return out


def _read_pending() -> dict | None:
    if not _pending_path.is_file():
        return None
    try:
        return json.loads(_pending_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _write_pending(qrcode_key: str) -> None:
    _pending_path.parent.mkdir(parents=True, exist_ok=True)
    _pending_path.write_text(
        json.dumps({"qrcode_key": qrcode_key, "generated_at": int(time.time())}),
        encoding="utf-8",
    )


async def start_qr_login() -> dict:
    """生成登录二维码并保存待验证状态，返回 {qrcode_key, qr_text, qr_png, qr_url}。"""
    # 参考项目 qrcode.generate() 的 query 参数
    params = {
        "source": "main-fe-header",
        "go_url": "https://www.bilibili.com/",
        "web_location": "333.1007",
    }
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(QR_GENERATE_API, params=params, headers=_HEADERS)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("code") != 0:
            raise RuntimeError(f"生成二维码失败: {payload.get('message')}")

    data = payload["data"]
    qrcode_key = data["qrcode_key"]
    qr_url = data["url"]
    _write_pending(qrcode_key)

    return {
        "qrcode_key": qrcode_key,
        "qr_url": qr_url,
        "qr_text": _render_qrcode_ascii(qr_url),
        "qr_png": _save_qrcode_png(qr_url),
    }


async def poll_qr_once(client: httpx.AsyncClient, qrcode_key: str) -> dict:
    """轮询一次扫码状态，返回 B 站原始响应 data 节点。"""
    resp = await client.get(QR_POLL_API, params={"qrcode_key": qrcode_key}, headers=_HEADERS)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("code") != 0:
        raise RuntimeError(f"轮询扫码状态失败: {payload.get('message')}")
    return payload.get("data") or {}


async def _finalize_login(client: httpx.AsyncClient, poll_data: dict) -> dict:
    """登录成功后的收尾：提取凭证 -> 补 buvid -> nav 校验并回写 WBI -> 落盘。"""
    creds = _from_jar(client.cookies)
    missing = [k for k in LOGIN_COOKIE_KEYS if not creds.get(k)]
    if missing:
        raise RuntimeError(f"登录响应缺少凭证字段: {', '.join(missing)}")

    creds["logged_in_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_credentials(creds)  # 先落盘，后续 ensure_buvid/nav 才能带登录态请求

    creds = await ensure_buvid(client)

    resp = await client.get(WBI_API, headers=api_headers())
    resp.raise_for_status()
    nav = resp.json()
    if nav.get("code") != 0:
        raise RuntimeError(f"校验登录态失败: {nav.get('message')}")
    nav_data = nav.get("data") or {}
    if not nav_data.get("isLogin"):
        raise RuntimeError("B 站返回未登录（isLogin=false），凭证未保存")
    store_wbi_keys(nav_data)

    # WBI 密钥与登录键平级存放（ensure_wbi_keys 从顶层读取），
    # 一次性落盘，避免中途保存覆盖掉 store_wbi_keys 的写入
    creds.update({
        "img_key": _wbi_keys["img_key"],
        "sub_key": _wbi_keys["sub_key"],
        "uname": nav_data.get("uname", ""),
        "mid": nav_data.get("mid"),
    })
    save_credentials(creds)
    _pending_path.unlink(missing_ok=True)
    logger.info("扫码登录成功并已本地保存: %s (uid=%s)", creds.get("uname"), creds.get("mid"))
    return creds


async def check_qr_login(wait_sec: float = POLL_WINDOW_SEC) -> dict:
    """在 wait_sec 时间窗内轮询扫码结果。返回 {status, message, creds?}。"""
    pending = _read_pending()
    if not pending or not pending.get("qrcode_key"):
        return {"status": "no_pending", "message": "尚未生成二维码，请先调用 bili_login_qrcode"}

    deadline = time.monotonic() + wait_sec
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        while True:
            poll_data = await poll_qr_once(client, pending["qrcode_key"])
            code = poll_data.get("code")

            if code == QR_SUCCESS:
                creds = await _finalize_login(client, poll_data)
                return {
                    "status": "success",
                    "message": f"登录成功，凭证已长期保存到 {_credentials_path()}",
                    "creds": creds,
                }
            if code == QR_EXPIRED:
                _pending_path.unlink(missing_ok=True)
                return {
                    "status": "expired",
                    "message": "二维码已过期，请重新调用 bili_login_qrcode 生成新码",
                }
            if code == QR_WAITING_FOR_CONFIRMATION:
                return {"status": "scanned", "message": "已扫码，请在手机上确认登录后再调用本工具"}
            # QR_WAITING_FOR_SCAN 或未知状态：窗口未到就继续等
            if time.monotonic() >= deadline:
                return {
                    "status": "waiting",
                    "message": "等待扫码中（请在手机 B 站 App 扫描二维码，然后再次调用本工具确认）",
                }
            await asyncio.sleep(POLL_INTERVAL_SEC)


async def verify_login() -> dict:
    """用本地凭证请求 nav，返回 {logged_in, uname, mid, credentials_path}。"""
    creds = load_credentials()
    if not creds.get("SESSDATA"):
        return {
            "logged_in": False,
            "message": f"本地没有登录凭证（{_credentials_path()}），请先调用 bili_login_qrcode 扫码登录",
        }
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(WBI_API, headers=api_headers())
        resp.raise_for_status()
        payload = resp.json()
    data = payload.get("data") or {}
    logged_in = bool(data.get("isLogin"))
    if logged_in:
        store_wbi_keys(data)
    return {
        "logged_in": logged_in,
        "uname": data.get("uname", ""),
        "mid": data.get("mid"),
        "credentials_path": str(_credentials_path()),
        "saved_at": creds.get("logged_in_at", ""),
        "message": (
            f"凭证有效（账号: {data.get('uname', '')}）"
            if logged_in
            else "本地凭证已失效（SESSDATA 过期），请重新扫码登录"
        ),
    }


def write_ytdlp_cookies() -> str | None:
    """把保存的凭证转成 Netscape cookies.txt，供 yt-dlp 兜底下载使用。

    返回文件路径；本地无登录凭证时返回 None（yt-dlp 匿名下载）。
    """
    creds = load_credentials()
    if not creds.get("SESSDATA"):
        return None
    out = config.DATA_DIR / "bili_cookies_ytdlp.txt"
    lines = ["# Netscape HTTP Cookie File", "# 由 bili_auth.write_ytdlp_cookies 生成"]
    for key in LOGIN_COOKIE_KEYS:
        value = creds.get(key, "")
        if not value:
            continue
        lines.append(
            f".bilibili.com\tTRUE\t/\tFALSE\t0\t{key}\t{value}"
        )
    for key in ("buvid3", "buvid4"):
        value = creds.get(key, "")
        if value:
            lines.append(f".bilibili.com\tTRUE\t/\tFALSE\t0\t{key}\t{value}")
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(out)
