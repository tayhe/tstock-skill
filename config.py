"""
外部技能路径与 API Key 配置

所有对外部 skill 和 API Key 的引用统一在此管理。
修改此处即可全局生效，无需逐个脚本查找替换。

优先级：环境变量 > 本文件默认值
"""

from pathlib import Path
import os

# ── 搜索系列（minimax-web-search、tavily-search）───────────────────────────
# 用于 tstock-fundamental_analyzer 的定性信息搜索
SEARCH_SKILLS_ROOT = Path(os.environ.get(
    "SEARCH_SKILLS_ROOT",
    str(Path.home() / ".openclaw/skills"),
))

MINIMAX_WEB_SEARCH = SEARCH_SKILLS_ROOT / "minimax-web-search/scripts/web_search.py"
TAVILY_SEARCH = SEARCH_SKILLS_ROOT / "tavily-search-1-0-0/scripts/search.mjs"

# ── 同花顺系列（行业数据查询、研报搜索、公司经营数据查询）────────────────────
# 用于 tstock-data-source 的数据增强（行业估值、研报、经营数据）
IWENCAI_SKILLS_ROOT = Path(os.environ.get(
    "IWENCAI_SKILLS_ROOT",
    str(Path.home() / "Projects/iwencai-skills"),
))

# ── 东方财富系列（financial-data、financial-search、select-stock）────────────
# 当前代码通过 HTTP API 直接调用东方财富，未调用本地脚本
# 保留此配置以便后续集成 eastmoney-financial-search 等本地技能
EASTMONEY_SKILLS_ROOT = Path(os.environ.get(
    "EASTMONEY_SKILLS_ROOT",
    str(Path.home() / "Projects/eastmoney-skills"),
))

# ── 本地数据路径 ──────────────────────────────────────────────────────────────
# 项目根目录（基于 config.py 所在位置推导）
PROJECT_ROOT = Path(__file__).resolve().parent

# 分析报告输出目录
REPORT_DIR = Path(os.environ.get(
    "TSTOCK_REPORT_DIR",
    str(PROJECT_ROOT / "memory" / "股票分析"),
))

# 自选股数据库路径
WATCHLIST_DB = Path(os.environ.get(
    "OPENCLAW_WATCHLIST_DB",
    str(PROJECT_ROOT / "memory" / "watchlist.json"),
))

# ── API Keys ────────────────────────────────────────────────────────────────

# 东方财富 API Key（用于 PE/PB/PEG 及行业估值数据）
# 获取：https://marketing.dfcfs.com/
EASTMONEY_APIKEY = os.environ.get("EASTMONEY_APIKEY", "mkt_0ELIRboa3w7joBoeI7V1BRNJ4wrHX3j-haFqKJ5rpPo")

# 同花顺 API（用于行业数据查询、研报搜索、公司经营数据查询）
IWENCAI_BASE_URL = os.environ.get("IWENCAI_BASE_URL", "https://openapi.iwencai.com")
IWENCAI_API_KEY = os.environ.get("IWENCAI_API_KEY", "sk-proj-00-eYSCskGL9M4I-hfD-9ODH2IAjVy7y9gH5g1WTMomktTWsM3030hIIn2RN-og5-yzW0Ijvos1XXq8-AJ2TFQVnvCYwZJkLjpFnz8FkIrvR4K3ooS1PHw-KYZxzqy2ZqGVyylBWg")

# Minimax API Key（用于定性信息搜索）
MINIMAX_API_KEY = os.environ.get("MINIMAX_API_KEY", "sk-cp-FqfsGBAtePc-YyDls_x9S3B2DYQsp3IQpSSe5AM0DBVRE19Z5-PbdJ2ADlTzc2ZSWw-NSX_eiq0o5-fk171SLtIduTDTvK6NTcwM2Nhr1MUPDnVEJy-zWe0")
