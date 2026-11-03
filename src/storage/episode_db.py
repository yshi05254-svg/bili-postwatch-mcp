"""SQLite 异步持久化层（aiosqlite）：episodes 全景档案 + subtitle_lines 逐句台本。"""

import asyncio
import json
from pathlib import Path

import aiosqlite

from src.config import DB_PATH
from src.schemas import BiliMeta, TranslatedLine

_SCHEMA = """
CREATE TABLE IF NOT EXISTS episodes (
    bvid TEXT PRIMARY KEY,
    title TEXT,
    meta_json TEXT,
    glossary_json TEXT,
    digest_markdown TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS subtitle_lines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bvid TEXT,
    line_index INTEGER,
    start_ms INTEGER,
    end_ms INTEGER,
    time_range_str TEXT,
    original_text TEXT,
    reflection_note TEXT,
    final_zh TEXT
);
CREATE INDEX IF NOT EXISTS idx_bvid_time ON subtitle_lines(bvid, start_ms);
"""


def ms_to_mmss(ms: int) -> str:
    """毫秒 -> "MM:SS"（超过 59 分钟时分钟位自然增长，如 75:03）。"""
    total_sec = max(0, int(ms) // 1000)
    minutes, seconds = divmod(total_sec, 60)
    return f"{minutes:02d}:{seconds:02d}"


def parse_time_to_ms(time_str: str) -> int | None:
    """把 "MM:SS" / "HH:MM:SS" / 纯秒数 解析为毫秒；非法输入返回 None。"""
    if time_str is None:
        return None
    text = str(time_str).strip()
    if not text:
        return None
    parts = text.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None
    if any(n < 0 for n in nums) or len(nums) > 3:
        return None
    total_sec = 0
    for n in nums:
        total_sec = total_sec * 60 + n
    return total_sec * 1000


class EpisodeDB:
    """每方法独立短连接，避免跨事件循环/进程生命周期管理连接的坑。"""

    def __init__(self, db_path: Path | None = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_lock = asyncio.Lock()
        self._initialized = False

    async def init_db(self) -> None:
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.executescript(_SCHEMA)
                await conn.commit()
            self._initialized = True

    async def get_episode_digest(self, bvid: str) -> str | None:
        await self.init_db()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                "SELECT digest_markdown FROM episodes WHERE bvid = ?", (bvid,)
            ) as cursor:
                row = await cursor.fetchone()
        return row[0] if row else None

    async def get_episode_meta(self, bvid: str) -> BiliMeta | None:
        """读取已建档视频的元数据（用于字幕输出时带标题）。"""
        await self.init_db()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                "SELECT meta_json FROM episodes WHERE bvid = ?", (bvid,)
            ) as cursor:
                row = await cursor.fetchone()
        if not row or not row[0]:
            return None
        try:
            return BiliMeta.model_validate_json(row[0])
        except Exception:  # noqa: BLE001 - 元数据损坏时降级为无标题
            return None

    async def get_episode_glossary(self, bvid: str) -> dict | None:
        """读取全局术语表（便于复盘过程中复用，避免重复提取）。"""
        await self.init_db()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                "SELECT glossary_json FROM episodes WHERE bvid = ?", (bvid,)
            ) as cursor:
                row = await cursor.fetchone()
        if not row or not row[0]:
            return None
        try:
            return json.loads(row[0])
        except (TypeError, json.JSONDecodeError):
            return None

    async def save_episode_data(
        self,
        meta: BiliMeta,
        glossary: dict,
        lines: list[TranslatedLine],
        digest: str,
    ) -> None:
        await self.init_db()
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("BEGIN")
            await conn.execute(
                "INSERT OR REPLACE INTO episodes "
                "(bvid, title, meta_json, glossary_json, digest_markdown) VALUES (?, ?, ?, ?, ?)",
                (
                    meta.bvid,
                    meta.title,
                    meta.model_dump_json(),
                    json.dumps(glossary, ensure_ascii=False),
                    digest,
                ),
            )
            # 同一 bvid 重刷时先清空旧台本，保证 line_index 与新流水线一致
            await conn.execute("DELETE FROM subtitle_lines WHERE bvid = ?", (meta.bvid,))
            await conn.executemany(
                "INSERT INTO subtitle_lines "
                "(bvid, line_index, start_ms, end_ms, time_range_str, original_text, "
                " reflection_note, final_zh) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        meta.bvid,
                        line.index,
                        line.start_ms,
                        line.end_ms,
                        line.time_range_str,
                        line.original_text,
                        line.reflection_note,
                        line.final_zh,
                    )
                    for line in lines
                ],
            )
            await conn.commit()

    async def query_lines(
        self,
        bvid: str,
        start_ms: int | None = None,
        end_ms: int | None = None,
        keyword: str | None = None,
        limit: int = 40,
    ) -> list[TranslatedLine]:
        """按时间区间 [start_ms, end_ms]（按台词与区间有重叠计）和/或关键词过滤逐句台本。

        关键词对 final_zh / original_text / reflection_note 三列做 LIKE %kw% 匹配。
        """
        await self.init_db()
        where = ["bvid = ?"]
        params: list = [bvid]
        if start_ms is not None:
            where.append("end_ms >= ?")
            params.append(start_ms)
        if end_ms is not None:
            where.append("start_ms <= ?")
            params.append(end_ms)
        if keyword:
            like = f"%{keyword}%"
            where.append("(final_zh LIKE ? OR original_text LIKE ? OR reflection_note LIKE ?)")
            params.extend([like, like, like])
        sql = (
            "SELECT line_index, start_ms, end_ms, time_range_str, original_text, "
            "reflection_note, final_zh FROM subtitle_lines WHERE "
            + " AND ".join(where)
            + " ORDER BY start_ms ASC LIMIT ?"
        )
        params.append(max(1, int(limit)))
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(sql, params) as cursor:
                rows = await cursor.fetchall()
        return [
            TranslatedLine(
                index=row[0],
                start_ms=row[1],
                end_ms=row[2],
                time_range_str=row[3],
                original_text=row[4],
                reflection_note=row[5],
                final_zh=row[6],
            )
            for row in rows
        ]
