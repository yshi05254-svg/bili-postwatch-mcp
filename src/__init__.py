"""bili-postwatch-mcp: Bilibili 生肉映后复盘 MCP Server。

拉取 B 站无字幕外语视频音频 -> FFmpeg 600 秒定长切片 -> 腾讯云 ASR 词级时间戳
-> 卡卡字幕助手管线（全局术语锁定 + LLM 语义重断句 + 反思翻译）
-> 双层记忆（全景剧情档案 + 逐句台本检索），通过 FastMCP 暴露给聊天 Bot。

运行方式（在项目根目录下）：
    python -m src.server          # 以 stdio 方式启动 MCP Server
"""
