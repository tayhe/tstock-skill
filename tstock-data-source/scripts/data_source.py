#!/usr/bin/env python3
"""A股统一数据源 — 兼容入口（实际逻辑在 tstock_data_source/ 包中）。"""

import sys
from pathlib import Path

# 将 scripts/ 目录加入 path，以便导入同级的 tstock_data_source 包
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))  # project root: tstock/, config
sys.path.insert(0, str(Path(__file__).resolve().parent))  # scripts/: tstock_data_source

from tstock_data_source.cli import main

if __name__ == "__main__":
    main()
