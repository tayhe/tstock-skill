"""统一快照加载：从文件读取或通过 data_source.py 获取。"""

import json
import subprocess
import sys
from typing import Optional

from tstock.paths import DATA_SOURCE_SCRIPT


def load_snapshot(
    code: Optional[str] = None,
    snapshot_path: Optional[str] = None,
    data_type: str = "all",
) -> dict:
    """加载股票快照。

    优先从 snapshot_path 读取 JSON 文件；
    否则调用 data_source.py 获取新快照。

    Args:
        code: 股票代码（如 "300308"）
        snapshot_path: 已有快照 JSON 文件路径
        data_type: 数据类型，"all" / "core" / "financial"
    """
    if snapshot_path:
        with open(snapshot_path, "r", encoding="utf-8") as f:
            return json.load(f)

    if not code:
        raise ValueError("必须提供 --code 或 --snapshot")

    import tempfile
    with tempfile.NamedTemporaryFile(
        suffix=".json", prefix=f"{code}_", delete=False
    ) as tmp:
        tmp_path = tmp.name

    cmd = [
        sys.executable,
        str(DATA_SOURCE_SCRIPT),
        "--code", code,
        "--data-type", data_type,
        "--output", tmp_path,
    ]
    subprocess.run(cmd, check=True)
    with open(tmp_path, "r", encoding="utf-8") as f:
        return json.load(f)
