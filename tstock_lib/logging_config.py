"""日志配置。"""

import logging
import sys


def setup_logging(level: str = "WARNING") -> None:
    """配置全局日志。level: DEBUG / INFO / WARNING / ERROR。"""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.WARNING),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        stream=sys.stderr,
    )
