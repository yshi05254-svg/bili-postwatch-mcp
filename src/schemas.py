"""全局 Pydantic 数据契约：模块间通信一律使用以下模型。"""

from pydantic import BaseModel, Field


class BiliMeta(BaseModel):
    bvid: str
    title: str
    desc: str
    tags: list[str] = Field(default_factory=list)
    top_comments: list[str] = Field(default_factory=list)

    def to_context_string(self) -> str:
        tags_str = ", ".join(self.tags)
        comments_str = " | ".join(self.top_comments[:5])
        return f"标题: {self.title}\n简介: {self.desc}\n标签: {tags_str}\n高赞评论参考: {comments_str}"


class WordItem(BaseModel):
    word: str
    start_ms: int
    end_ms: int


class RawSegment(BaseModel):
    index: int
    start_ms: int
    end_ms: int
    text: str


class TranslatedLine(BaseModel):
    index: int
    start_ms: int
    end_ms: int
    time_range_str: str      # 格式: "MM:SS - MM:SS"
    original_text: str
    reflection_note: str     # 专有名词纠错与语境分析备注
    final_zh: str            # 含（语气/动作）括号标注的中文台词
