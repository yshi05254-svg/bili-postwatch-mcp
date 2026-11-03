"""轻量级 CLI 验证脚本。

用法（在项目根目录，且已安装 requirements.txt 依赖）：

    # 1) 离线 Mock 闭环自测：断句对齐 + 反思翻译(含降级) + SQLite 读写（无需任何密钥/网络）
    python test_pipeline.py

    # 2) 仅验证 B 站元数据抓取（需要网络）
    python test_pipeline.py --meta https://www.bilibili.com/video/BVxxxxxxxx/

    # 3) 全量真实流水线（需要网络 + 腾讯云密钥 + LLM 密钥）
    python test_pipeline.py --url https://www.bilibili.com/video/BVxxxxxxxx/ --language 16k_ja
"""

import argparse
import asyncio
import json
import re
import sys
import tempfile
import types
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Windows 控制台输出中文/日文
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

from src.captioner.digest_builder import build_digest
from src.captioner.glossary import build_glossary
from src.captioner.splitter import align_split_sentences, clean_str, split_words_into_segments
from src.captioner.translator import translate_segments
from src.schemas import BiliMeta, WordItem
from src.storage.episode_db import EpisodeDB, ms_to_mmss, parse_time_to_ms

PASS_COUNT = 0
FAIL_COUNT = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS_COUNT, FAIL_COUNT
    if cond:
        PASS_COUNT += 1
        print(f"  [PASS] {name}")
    else:
        FAIL_COUNT += 1
        print(f"  [FAIL] {name}  {detail}")


# ---------------------------------------------------------------------------
# Mock 依赖：仿 AsyncOpenAI 客户端 + 假 ASR 词级数据
# ---------------------------------------------------------------------------
_SAMPLE_JA_TEXT = (
    "はい、みなさんこんにちは。今日は新作アニメの感想会を始めましょう。"
    "まずは第一話の冒頭シーンについて話したいと思います。"
    "主人公が学校に向かう途中、不思議な少女に出会いましたよね。"
    "あのシーンの演出は本当に素晴らしかったです。"
    "ところで、彼女の正体は最後まで明かされませんでしたね。"
    "それは作家の意図的な仕掛けだと思います。"
    "第二話では新しい敵キャラが登場します。"
    "戦闘シーンの作画は映画並みのクオリティでした。"
    "みなさんの推しポイントはどこですか。"
    "私としては雨の日の屋上のシーンが一番心に残りました。"
) * 6  # 加长到约 1500 字，保证产生多个 150 词分块

_GLOSSARY_JSON = json.dumps(
    {
        "work_title": "架空学园动画《星降の夜に》",
        "character_and_term_glossary": {
            "主人公": "星野湊（男主人公，天文部部员）",
            "少女": "ルナ（谜之少女，实为陨石之精）",
        },
    },
    ensure_ascii=False,
)

_DIGEST_MARKDOWN = """## 一、 核心设定与人物速览
本集为学园动画开篇，主人公星野湊是天文部部员，上学途中与谜之少女ルナ相遇。
ルナ的真实身份被刻意隐藏，成为全集最大悬念。
## 二、 全集分场剧情脉络
- [00:00] 通学路相遇：湊在上学途中邂逅ルナ，悬念铺开。
- [10:00] 屋上对话：雨天的屋上，两人关于星星的对话揭示主题。
## 三、 名场面与高光台词原话
* [01:10 - 01:15] 屋上：「雨の日こそ、星は泣いている」（雨の日の屋上のシーン）
* [10:30 - 10:35] ルナ：「私は星から来たの」（最終話への伏線）
## 四、 映后互动话题钩子
1. ルナ的真实身份到底是什么？
2. 雨中屋上场景的演出意图是什么？
"""


class GoodFakeLLM:
    """按请求类型返回可解析结果，并在断句文本中注入 1 个字符漂移以测试游标容错。"""

    def __init__(self):
        self.chat = types.SimpleNamespace(
            completions=types.SimpleNamespace(create=self._create)
        )
        self.drift_injected = False

    async def _create(self, **kwargs):
        system = kwargs["messages"][0]["content"]
        user = kwargs["messages"][-1]["content"]
        if "断句师" in system:
            m = re.search(r"<<<\n?(.*?)\n?>>>", user, re.DOTALL)
            text = m.group(1) if m else user
            sentences = re.split(r"(?<=[。！？])", text)
            out = "||".join(s for s in sentences if s)
            if not self.drift_injected and "||" in out:
                # 模拟 LLM 多吐一个字符（在第一个断句符后）
                out = out.replace("||", "ー||", 1)
                self.drift_injected = True
            content = out
        elif "术语审校" in system:
            content = _GLOSSARY_JSON
        elif "反思翻译" in system:
            numbered = re.findall(r"^\s*(\d+)\.\s*\[[^\]]+\]\s+(.+)$", user, re.MULTILINE)
            items = [
                {
                    "index": int(idx),
                    "draft": f"直译:{text[:12]}",
                    "reflection": f"术语核对:{text[:6]}",
                    "final_zh": f"（微笑）这是第{idx}句的中文终稿：{text[:10]}",
                }
                for idx, text in numbered
            ]
            content = json.dumps({"items": items}, ensure_ascii=False)
        elif "陪看伙伴" in system:
            content = _DIGEST_MARKDOWN
        else:
            raise AssertionError(f"未知请求类型: {system[:50]}")
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=types.SimpleNamespace(content=content))]
        )


class BadFakeLLM(GoodFakeLLM):
    """始终返回非法 JSON，用于测试翻译重试与原文兜底。"""

    async def _create(self, **kwargs):
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=types.SimpleNamespace(content="抱歉，我做不到"))]
        )


def build_mock_words() -> list[WordItem]:
    """把样本日文文本切成 1~3 字符的 ASR 风格伪词，并铺上单调递增时间戳。"""
    words: list[WordItem] = []
    cursor_ms = 500
    i = 0
    while i < len(_SAMPLE_JA_TEXT):
        step = 1 + (i % 3)  # 1~3 字符
        token = _SAMPLE_JA_TEXT[i : i + step]
        dur = 120 + 60 * len(token)
        words.append(WordItem(word=token, start_ms=cursor_ms, end_ms=cursor_ms + dur))
        cursor_ms += dur + 40
        i += step
    return words


# ---------------------------------------------------------------------------
# 测试用例
# ---------------------------------------------------------------------------
def test_time_utils() -> None:
    print("\n== 时间工具函数 ==")
    check("ms_to_mmss(754000) == 12:34", ms_to_mmss(754000) == "12:34")
    check("parse_time_to_ms('12:00') == 720000", parse_time_to_ms("12:00") == 720_000)
    check(
        "parse_time_to_ms('1:12:00') == 4320000",
        parse_time_to_ms("1:12:00") == 4_320_000,
    )
    check("parse_time_to_ms('abc') is None", parse_time_to_ms("abc") is None)


def test_align_with_drift() -> None:
    print("\n== 滑动游标对齐（含 LLM 字符漂移） ==")
    text = "こんにちは、今日はいい天気ですね。明日は雨かもしれません。"
    words = []
    cursor = 100
    for i in range(0, len(text), 2):
        token = text[i : i + 2]
        words.append(WordItem(word=token, start_ms=cursor, end_ms=cursor + 200))
        cursor += 250
    # LLM 输出多吐一个标点：句尾多了一个句号
    llm_text = "こんにちは、今日はいい天気ですね。。||明日は雨かもしれません。"
    segments = align_split_sentences(words, llm_text)
    check("漂移下仍切成 2 句", len(segments) == 2, f"实际 {len(segments)} 句")
    check("首句起点 = 首词起点", segments[0].start_ms == words[0].start_ms)
    check("末句终点 = 末词终点", segments[-1].end_ms == words[-1].end_ms)
    check("index 连续", [s.index for s in segments] == [1, 2])


async def test_mock_closed_loop() -> None:
    print("\n== Mock 全链路闭环（无需密钥） ==")
    words = build_mock_words()
    check(f"构建伪 ASR 词 {len(words)} 个", len(words) >= 300)

    # --- 语义断句 ---
    good = GoodFakeLLM()
    segments = await split_words_into_segments(words, client=good)
    check("断句产出非空", len(segments) > 10, f"实际 {len(segments)}")
    check(
        "index 1..N 连续",
        [s.index for s in segments] == list(range(1, len(segments) + 1)),
    )
    check("时间单调递增", all(
        segments[i].start_ms <= segments[i + 1].start_ms for i in range(len(segments) - 1)
    ))
    check("全覆盖到末词", segments[-1].end_ms == words[-1].end_ms)
    clean_words = sum(len(clean_str(w.word)) for w in words)
    clean_segs = sum(len(clean_str(s.text)) for s in segments)
    ratio = clean_segs / max(1, clean_words)
    check(f"字符守恒率 {ratio:.2%} >= 85%", ratio >= 0.85, f"实际 {ratio:.2%}")

    # --- 术语表 ---
    meta = BiliMeta(
        bvid="BV1xx411c7mD",
        title="【生肉】星降の夜に 第1話",
        desc="无字幕生肉，欢迎一起看",
        tags=["动画", "生肉"],
        top_comments=["ルナ太可爱了", "野生字幕君辛苦"],
    )
    glossary = await build_glossary(segments, meta, client=good)
    check("work_title 非空", bool(glossary.get("work_title")))
    check("术语表含条目", len(glossary.get("character_and_term_glossary", {})) >= 1)

    # --- 反思翻译（成功路径） ---
    lines = await translate_segments(segments, meta, glossary, client=good)
    check("台词数 == 句子数", len(lines) == len(segments))
    check("final_zh 全部非空", all(l.final_zh for l in lines))
    check("index 对齐", [l.index for l in lines] == [s.index for s in segments])
    check(
        "time_range_str 格式正确",
        all(re.match(r"^\d{2,}:\d{2} - \d{2,}:\d{2}$", l.time_range_str) for l in lines),
    )
    print("  样例:", lines[0].time_range_str, lines[0].final_zh)

    # --- 反思翻译（JSON 彻底失败 -> 原文兜底） ---
    bad_lines = await translate_segments(segments[:10], meta, glossary, client=BadFakeLLM())
    check("降级后仍返回 10 句", len(bad_lines) == 10)
    check("降级句保留原文", all(l.final_zh == l.original_text for l in bad_lines))
    check("降级备注已标注", all("失败" in l.reflection_note for l in bad_lines))

    # --- 全景档案 ---
    digest = await build_digest(lines, meta, glossary, client=good)
    for header in ("## 一、", "## 二、", "## 三、", "## 四、"):
        check(f"档案含章节 {header}", header in digest)

    # --- SQLite 读写闭环 ---
    print("\n== SQLite 存储闭环 ==")
    with tempfile.TemporaryDirectory() as tmp:
        db = EpisodeDB(Path(tmp) / "episodes.db")
        await db.save_episode_data(meta, glossary, lines, digest)
        cached = await db.get_episode_digest(meta.bvid)
        check("digest 缓存读取一致", cached == digest)
        first, last = lines[0], lines[-1]
        by_time = await db.query_lines(
            meta.bvid, start_ms=first.start_ms, end_ms=last.end_ms, limit=len(lines) + 10
        )
        check("全区间查询命中全部", len(by_time) == len(lines), f"实际 {len(by_time)}")
        kw = re.search(r"第(\d+)句的中文终稿", lines[3].final_zh)
        kw_lines = await db.query_lines(meta.bvid, keyword=kw.group(0))
        check("关键词命中中文台词", len(kw_lines) >= 1)
        note_kw = "纠错特训词"
        lines[5].reflection_note = note_kw
        await db.save_episode_data(meta, glossary, lines, digest)
        hit_note = await db.query_lines(meta.bvid, keyword=note_kw)
        check("关键词命中纠错注释", len(hit_note) == 1 and hit_note[0].index == lines[5].index)
        miss = await db.query_lines(meta.bvid, keyword="绝不存在的词xyz")
        check("未命中返回空", miss == [])
        gloss_cached = await db.get_episode_glossary(meta.bvid)
        check("术语表缓存读取一致", gloss_cached == glossary)

    print("\n" + "=" * 50)
    print(f"Mock 自测结果: PASS={PASS_COUNT} FAIL={FAIL_COUNT}")
    if FAIL_COUNT:
        sys.exit(1)


async def run_meta_only(url: str) -> None:
    from src.extractor.bili_meta import fetch_bili_meta

    meta = await fetch_bili_meta(url)
    print(meta.to_context_string())


async def run_full_pipeline(url: str, language: str) -> None:
    from src.server import process_episode

    bvid, digest = await process_episode(url, language=language)
    print(f"\n===== {bvid} 全景陪看记忆档案 =====\n")
    print(digest)


def main() -> None:
    parser = argparse.ArgumentParser(description="bili-postwatch-mcp 验证脚本")
    parser.add_argument("--meta", metavar="URL_OR_BVID", help="仅测试 B 站元数据抓取")
    parser.add_argument("--url", metavar="URL_OR_BVID", help="全量真实流水线")
    parser.add_argument("--language", default="16k_ja", help="ASR 引擎，默认 16k_ja")
    args = parser.parse_args()

    if args.meta:
        asyncio.run(run_meta_only(args.meta))
        return
    if args.url:
        asyncio.run(run_full_pipeline(args.url, args.language))
        return

    test_time_utils()
    test_align_with_drift()
    asyncio.run(test_mock_closed_loop())


if __name__ == "__main__":
    main()
