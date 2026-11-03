"""验证脚本：改动后的下载/登录逻辑与参考项目 Bili23-Downloader 对齐。

1. WBI 签名：直接从参考项目源文件提取 enc_wbi/mixinKeyEncTab 原文执行，
   与 src/extractor/bili_auth.enc_wbi 在随机参数上比对 —— 逐字节一致才通过。
2. CDN 候选：黑名单过滤 / 去重 / 全黑名单回退，对齐参考 CDN.filter 语义。
3. 文件大小提取：Content-Range / Content-Length / text 响应判定。
4. 模块导入烟测。
"""

import asyncio
import os
import random
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 参考项目 Bili23-Downloader 的 base.py 路径：
# 可用环境变量 BILI23_REF_BASE 指定，默认取与本项目同级的 _ref 目录。
REF_BASE = Path(
    os.getenv("BILI23_REF_BASE")
    or (PROJECT_ROOT.parent / "_ref" / "Bili23-Downloader" / "src" / "util" / "parse" / "parser" / "base.py")
)
if not REF_BASE.is_file():
    raise SystemExit(
        f"未找到参考项目源文件: {REF_BASE}\n"
        "请把 Bili23-Downloader 放到同级 _ref 目录，或用环境变量 BILI23_REF_BASE 指定 base.py 路径。"
    )

failures = []


def check(name: str, ok: bool, detail: str = ""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))
    if not ok:
        failures.append(name)


# ---------------------------------------------------------------------------
# 1. WBI 签名等价性：从参考源文件原样提取后执行
# ---------------------------------------------------------------------------
ref_src = REF_BASE.read_text(encoding="utf-8")
tab_block = re.search(r"mixinKeyEncTab = \[.*?\]", ref_src, re.S).group()
enc_block = re.search(r"def enc_wbi\(params: dict\):.*?return urllib\.parse\.urlencode\(params\)", ref_src, re.S).group()

ns = {}
IMG_KEY = "7cd084941338484aae1ad9425b84077c"
SUB_KEY = "4932caff0ff746eab6f01bf08b70ac45"


class _FakeConfig:
    """替代参考项目 config：enc_wbi 内部读取 config.get(config.img_key/sub_key)。"""

    img_key = "img_key_sentinel"
    sub_key = "sub_key_sentinel"

    @classmethod
    def get(cls, item):
        return IMG_KEY if item is cls.img_key else SUB_KEY


exec(  # noqa: S102 - 参考源码片段，仅测试用
    "import urllib.parse\nimport time\nfrom hashlib import md5\nfrom functools import reduce\n"
    + tab_block + "\n" + enc_block,
    ns,
)
ns["config"] = _FakeConfig
ns["ensure_wbi_keys"] = lambda: None
ref_enc_wbi = ns["enc_wbi"]

from src.extractor import bili_auth  # noqa: E402

from urllib.parse import parse_qs, urlencode  # noqa: E402

random.seed(20261104)
all_match = True
for case in range(50):
    params = {
        "bvid": "BV1abcDEfghi",
        "cid": random.randint(1, 10**9),
        "qn": 16,
        "fnver": 0,
        "fnval": 4048,
        "fourk": 1,
    }
    # 随机附加参数（含需要过滤的字符）
    if case % 2:
        params["foo"] = "1_()'!*22"
        params["zab"] = random.randint(1, 999)

    mine = bili_auth.enc_wbi(dict(params), IMG_KEY, SUB_KEY)
    theirs = ref_enc_wbi(dict(params))

    def strip_wrid(query: str) -> dict:
        q = {k: v[0] for k, v in parse_qs(query).items()}
        q.pop("w_rid", None)
        return q

    mine_q, ref_q = strip_wrid(mine), strip_wrid(theirs)
    if mine_q != ref_q:
        # 罕见情况：两次调用跨过整秒，wts 不同。重跑这一组
        mine = bili_auth.enc_wbi(dict(params), IMG_KEY, SUB_KEY)
        theirs = ref_enc_wbi(dict(params))
        mine_q, ref_q = strip_wrid(mine), strip_wrid(theirs)
        if mine_q != ref_q:
            all_match = False
            break

    # 以参考实现的 mixin 表 + 相同 wts 复算 w_rid，比对签名输出
    mixin = ns["getMixinKey"](IMG_KEY + SUB_KEY) if "getMixinKey" in ns else None
    if mixin is None:
        # 参考源码中 getMixinKey 是 enc_wbi 的内嵌函数，这里按其原文逻辑重建
        from functools import reduce
        mixin = reduce(lambda s, i: s + (IMG_KEY + SUB_KEY)[i], ns["mixinKeyEncTab"], "")[:32]
    merged = dict(sorted({**params, "wts": int(mine_q["wts"])}.items()))
    merged = {k: "".join(filter(lambda c: c not in "!'()*", str(v))) for k, v in merged.items()}
    expected_rid = ns["md5"]((urlencode(merged) + mixin).encode()).hexdigest()
    if parse_qs(mine)["w_rid"][0] != expected_rid:
        all_match = False
        break

check("WBI 签名与参考实现等价（50 组随机参数）", all_match)
check("getMixinKey 长度为 32", len(bili_auth._get_mixin_key("a" * 64)) == 32)

# ---------------------------------------------------------------------------
# 2. CDN 候选：过滤 / 去重 / 全黑名单回退
# ---------------------------------------------------------------------------
from src.extractor.audio_chunker import _candidate_cdns, _extract_file_size  # noqa: E402

stream = {
    "baseUrl": "https://upos-sz-mirror08c.bilivideo.com/xa.mp4?x=1",
    "base_url": "https://upos-sz-mirror08c.bilivideo.com/xa.mp4?x=1",
    "backupUrl": [
        "https://mcdn.bilivideo.com:4483/x.mp4?x=2",
        "https://upos-sz-eastx22.bilivideo.com/xa.mp4?x=3",
    ],
}
cands = _candidate_cdns(stream)
check(
    "CDN 候选去重 + 黑名单剔除",
    cands == [
        "https://upos-sz-mirror08c.bilivideo.com/xa.mp4?x=1",
        "https://upos-sz-eastx22.bilivideo.com/xa.mp4?x=3",
    ],
    str(cands),
)
only_bad = _candidate_cdns({"baseUrl": "https://mcdn.bilivideo.com:4483/x.mp4?x=2"})
check("全黑名单时回退原始列表", only_bad == ["https://mcdn.bilivideo.com:4483/x.mp4?x=2"], str(only_bad))
query_param = _candidate_cdns({"baseUrl": "https://upos.example.com/a?mcdn=not_a_host"})
check("黑名单只按主机名匹配，不误杀查询参数", len(query_param) == 1, str(query_param))

# ---------------------------------------------------------------------------
# 3. 文件大小提取（参考 download_url._extract_file_size 语义）
# ---------------------------------------------------------------------------
class FakeHeaders(dict):
    def get(self, key, default=""):
        return super().get(key, default)


check(
    "Content-Range 提取总长",
    _extract_file_size(FakeHeaders({"Content-Range": "bytes 0-0/11529448", "Content-Type": "audio/mp4"})) == 11529448,
)
check(
    "Content-Length 兜底",
    _extract_file_size(FakeHeaders({"Content-Length": "123456", "Content-Type": "audio/mp4"})) == 123456,
)
check(
    "text/json 响应判无效",
    _extract_file_size(FakeHeaders({"Content-Length": "123", "Content-Type": "application/json"})) == 0,
)

# ---------------------------------------------------------------------------
# 4. 模块导入烟测 + Netscape cookie 写出
# ---------------------------------------------------------------------------
import src.server  # noqa: E402, F401
from src.extractor import audio_chunker  # noqa: E402, F401

fake_creds = {
    "SESSDATA": "test-sessdata",
    "bili_jct": "test-jct",
    "DedeUserID": "123",
    "DedeUserID__ckMd5": "abc",
    "buvid3": "b3",
    "buvid4": "b4",
}
bili_auth.save_credentials(fake_creds)
cookie_header = bili_auth.build_cookie_header()
check(
    "Cookie 头包含登录键与 buvid",
    all(k in cookie_header for k in ("SESSDATA=test-sessdata", "bili_jct=test-jct", "buvid3=b3", "CURRENT_FNVAL=4048")),
    cookie_header[:80] + "...",
)
netscape = bili_auth.write_ytdlp_cookies()
check("Netscape cookies 已生成", bool(netscape) and Path(netscape).is_file(), str(netscape))

# 清理测试凭证，避免污染真实登录状态
bili_auth._credentials_path().unlink(missing_ok=True)
bili_auth.write_ytdlp_cookies.__globals__["config"].DATA_DIR.joinpath("bili_cookies_ytdlp.txt").unlink(missing_ok=True)

print()
if failures:
    print(f"共 {len(failures)} 项失败: {failures}")
    sys.exit(1)
print("全部通过")
