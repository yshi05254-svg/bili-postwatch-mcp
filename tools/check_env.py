"""配置自检：验证 .env 中腾讯云密钥与 LLM 密钥是否真实可用。

- 腾讯云：用 DescribeTaskStatus 探测一个不存在的任务 ID——认证失败会返回
  AuthFailure 系错误；返回业务类错误（任务不存在等）即证明签名认证通过。
  全程不创建识别任务，零计费。
- LLM：发送一条几 token 的测试消息验证连通性。
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

from src import config


def mask(s: str) -> str:
    return f"{s[:6]}...({len(s)}位)" if s else "(未配置)"


def check_tencent() -> tuple[bool, str]:
    from tencentcloud.common.exception.tencent_cloud_sdk_exception import (
        TencentCloudSDKException,
    )

    from src.asr.tencent_batch_asr import _build_client, _describe_task_sync

    if not config.TENCENT_SECRET_ID or not config.TENCENT_SECRET_KEY:
        return False, "TENCENT_SECRET_ID / TENCENT_SECRET_KEY 未配置"
    try:
        client = _build_client()
        resp = _describe_task_sync(client, 123456789)
        data = resp.Data
        return True, (
            f"签名认证通过（探活任务返回 Status={getattr(data, 'Status', '?')}，"
            f"属预期业务响应）"
        )
    except TencentCloudSDKException as err:
        code = str(err.code or "")
        if code.startswith("AuthFailure") or "Credential" in code or "Signature" in code:
            return False, f"认证失败 [{code}]：SecretId/SecretKey 不正确"
        if "LimitExceeded" in code:
            return False, f"额度/开通问题 [{code}]：{err}"
        # 任务不存在等业务错误 = 认证已通过
        return True, f"签名认证通过（API 返回预期业务错误: [{code}] {str(err)[:100]}）"
    except Exception as err:  # noqa: BLE001
        return False, f"未知错误: {err}"


async def check_llm() -> tuple[bool, str]:
    client = config.get_llm_client()
    if client is None:
        return False, "LLM_API_KEY 未配置"
    try:
        text = await config.chat_text(
            client,
            [{"role": "user", "content": "请只回复两个字：正常"}],
            temperature=0,
            max_retries=0,
        )
        return True, f"连通正常，模型回复: {text.strip()[:20]!r}"
    except Exception as err:  # noqa: BLE001
        msg = str(err)
        if "401" in msg or "403" in msg or "invalid" in msg.lower() and "key" in msg.lower():
            return False, f"密钥被拒绝（鉴权失败）：{msg[:200]}"
        return False, f"调用失败: {msg[:200]}"


async def main() -> None:
    print("=== bili-postwatch-mcp 配置自检 ===")
    print(f"TENCENT_SECRET_ID : {mask(config.TENCENT_SECRET_ID)}")
    print(f"TENCENT_SECRET_KEY: {mask(config.TENCENT_SECRET_KEY)}")
    print(f"ASR 区域          : {config.TENCENT_ASR_REGION}")
    print(f"LLM_BASE_URL      : {config.LLM_BASE_URL}")
    print(f"LLM_MODEL         : {config.LLM_MODEL}")
    print(f"LLM_API_KEY       : {mask(config.LLM_API_KEY)}")

    ok1, msg1 = check_tencent()
    print(f"\n[腾讯云 ASR] {'PASS' if ok1 else 'FAIL'} - {msg1}")
    ok2, msg2 = await check_llm()
    print(f"[LLM 接口  ] {'PASS' if ok2 else 'FAIL'} - {msg2}")

    print("\n结论:", "全部可用，可以跑完整流水线" if ok1 and ok2 else "存在未通过项，见上方信息")
    sys.exit(0 if ok1 and ok2 else 1)


asyncio.run(main())
