"""扫码登录运行器：生成二维码并轮询直到登录成功/二维码过期。

用法: python tools/qr_login_runner.py
成功时凭证长期保存到 data/bili_credentials.json。
"""

import asyncio
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src import config  # noqa: E402
from src.extractor import bili_auth  # noqa: E402


async def main() -> None:
    config.configure_logging()

    qr = await bili_auth.start_qr_login()
    print("===QR_TEXT_BEGIN===")
    print(qr["qr_text"])
    print("===QR_TEXT_END===")
    print(f"QR_PNG: {qr['qr_png']}")
    print(f"QR_URL: {qr['qr_url']}", flush=True)

    deadline = time.monotonic() + 170  # B 站二维码有效期约 180 秒
    last_status = ""
    while time.monotonic() < deadline:
        result = await bili_auth.check_qr_login(wait_sec=20)
        status = result["status"]
        if status != last_status:
            print(f"STATUS: {status} — {result['message']}", flush=True)
            last_status = status
        if status == "success":
            creds = result.get("creds") or {}
            print(f"LOGIN_OK: {creds.get('uname', '')} (uid={creds.get('mid', '')})")
            print(f"CREDENTIALS: {bili_auth._credentials_path()}")
            return
        if status == "expired":
            print("LOGIN_EXPIRED")
            return
        # waiting / scanned：继续轮询直到成功或过期
    print("LOGIN_TIMEOUT")


if __name__ == "__main__":
    asyncio.run(main())
