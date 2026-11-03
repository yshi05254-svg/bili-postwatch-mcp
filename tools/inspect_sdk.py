"""检查 tencentcloud-sdk 中 ASR 相关 Model 类包含哪些字段。

模型文件路径优先取命令行参数，否则在项目根目录的 .venv 中自动定位。
用法: python tools/inspect_sdk.py [models.py 路径]
"""

import re
import sys
from pathlib import Path

REL = Path(".venv/Lib/site-packages/tencentcloud/asr/v20190614/models.py")

if len(sys.argv) > 1:
    models_path = Path(sys.argv[1])
else:
    base = Path(__file__).resolve().parent.parent
    candidates = [base / REL, base / ".venv/lib/python3/site-packages/tencentcloud/asr/v20190614/models.py"]
    models_path = next((p for p in candidates if p.is_file()), candidates[0])

if not models_path.is_file():
    sys.exit(f"未找到 tencentcloud models.py: {models_path}（可作为参数传入）")

src = models_path.read_text(encoding="utf-8")

lines = src.splitlines()
targets = ["class Task(AbstractModel)", "class TaskStatus(AbstractModel)", "class SentenceDetail(AbstractModel)", "class SentenceWord(AbstractModel)", "class SentenceWords(AbstractModel)"]
for target in targets:
    start = next(i for i, l in enumerate(lines) if l.startswith(target))
    fields = []
    for l in lines[start:]:
        if l.startswith("class ") and not l.startswith(target):
            break
        m = re.match(r"\s*self\.(_\w+) = None", l)
        if m:
            fields.append(m.group(1))
    print(target, "->", sorted(set(fields)))
