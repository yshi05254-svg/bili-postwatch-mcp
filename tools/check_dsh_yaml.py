"""校验 DSH cordis.patch.yml 的 YAML 语法与 bili_postwatch 条目。

配置文件路径通过环境变量 DSH_CORDIS_PATCH 指定，
未指定时回退到当前用户的 ~/.dsh/profiles/desktop/cordis.patch.yml。
"""

import os
import sys
from pathlib import Path

import yaml

DEFAULT_REL = Path(".dsh") / "profiles" / "desktop" / "cordis.patch.yml"
PATH = Path(os.getenv("DSH_CORDIS_PATCH") or (Path.home() / DEFAULT_REL))

if not PATH.is_file():
    sys.exit(f"未找到配置文件: {PATH}（可用环境变量 DSH_CORDIS_PATCH 指定）")

data = yaml.safe_load(open(PATH, encoding="utf-8"))
servers = []
for entry in data:
    if isinstance(entry, dict) and "insert" in entry:
        for sub in entry["insert"]:
            cfg = sub.get("config", {})
            if "serverName" in cfg:
                servers.append(cfg["serverName"])
assert isinstance(data, list) and len(data) >= 10, f"条目异常: {len(data)}"
assert "bili_postwatch" in servers, f"bili_postwatch 未写入: {servers}"
print(f"YAML_PARSE_OK total_entries={len(data)} mcp_servers={servers}")
