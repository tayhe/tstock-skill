"""项目路径常量。"""

from pathlib import Path

# 项目根目录（tstock-skills/）
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

# 各 skill 的 CLI 脚本路径
DATA_SOURCE_SCRIPT: Path = PROJECT_ROOT / "tstock-data-source" / "scripts" / "data_source.py"
FUNDAMENTAL_SCRIPT: Path = PROJECT_ROOT / "tstock-fundamental_analyzer" / "scripts" / "fundamental_analyzer.py"
TECHNICAL_SCRIPT: Path = PROJECT_ROOT / "tstock-technical_analyzer" / "scripts" / "technical_analyzer.py"
RISK_SCRIPT: Path = PROJECT_ROOT / "tstock-risk_analyzer" / "scripts" / "risk_evaluator.py"
STRATEGY_SCRIPT: Path = PROJECT_ROOT / "tstock-portfolio" / "scripts" / "strategy_planner.py"

# config.py 路径
CONFIG_PATH: Path = PROJECT_ROOT / "config.py"
