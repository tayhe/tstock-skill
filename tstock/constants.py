"""全局常量 — 阈值、超时、评分参数等。

按域分组，便于查找和维护。
"""

# ── HTTP 超时（秒）─────────────────────────────────────────────────────────
HTTP_TIMEOUT_DEFAULT = 15
HTTP_TIMEOUT_DFCF = 18
HTTP_TIMEOUT_IWENCAI = 20
HTTP_TIMEOUT_TENCENT = 8

# ── 重试 / 睡眠 ──────────────────────────────────────────────────────────
RETRY_BACKOFF = 0.8
BATCH_SLEEP = 0.2

# ── 行业均值最小样本量 ────────────────────────────────────────────────────
INDUSTRY_MIN_SAMPLE = 8

# ── 估值阈值 ──────────────────────────────────────────────────────────────
PREMIUM_HIGH_PCT = 25       # 溢价率 > 此值 → 偏高估
PREMIUM_LOW_PCT = -20       # 溢价率 < 此值 → 偏低估
PEG_HIGH = 1.5              # PEG > 此值 → 成长定价偏贵
PEG_LOW = 0.8               # PEG < 此值 → 成长定价偏低

# ── 基本面评分 ────────────────────────────────────────────────────────────
SCORE_BASE = 50
ROE_EXCELLENT = 0.2
ROE_GOOD = 0.12
NET_MARGIN_GOOD = 0.1
NET_MARGIN_LOW = 0.03
DEBT_SAFE = 0.6
PE_PCT_HIGH = 80
PE_PCT_LOW = 30

SCORE_STRONG = 70           # 基本面较强
SCORE_NEUTRAL = 55          # 中性偏强
SCORE_WEAK = 40             # 一般（低于此为偏弱）

# ── 风险评分 ──────────────────────────────────────────────────────────────
RISK_PE_HIGH = 60
RISK_PE_MED = 40
RISK_PB_HIGH = 8
RISK_PE_PCT_HIGH = 85
RISK_DEBT_HIGH = 0.7
RISK_DEBT_MED = 0.6
RISK_CURRENT_LOW = 1.0
RISK_MCAP_SMALL = 5e10
RISK_MCAP_MID = 1e11
RISK_AMPLITUDE_HIGH = 0.5
RISK_AMPLITUDE_MED = 0.3
RISK_PRICE_CHANGE_HIGH = 7
HIGH_RISK_INDUSTRIES = ["房地产", "游戏", "教育"]

RISK_LOW_THRESHOLD = 35
RISK_MED_THRESHOLD = 60

# ── 策略权重 ──────────────────────────────────────────────────────────────
WEIGHT_FUNDAMENTAL = 0.4
WEIGHT_TECHNICAL = 0.25
WEIGHT_RISK = 0.35

# ── 技术面 ────────────────────────────────────────────────────────────────
ATR_STOP_MULTIPLIER = 1.5
