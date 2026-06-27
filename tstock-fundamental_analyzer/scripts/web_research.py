#!/usr/bin/env python3
"""网络搜索、文本处理、公司画像提取。供 fundamental_analyzer 调用。"""

import logging
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import config

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 搜索
# ─────────────────────────────────────────────────────────────────────────────

def _search_with_minimax(query: str) -> str:
    cmd = config.MINIMAX_WEB_SEARCH
    try:
        env = {**os.environ, 'MINIMAX_API_KEY': config.MINIMAX_API_KEY}
        p = subprocess.run(
            [cmd, 'search', 'query', '--q', query, '--output', 'text', '--quiet'],
            capture_output=True, text=True, timeout=60,
            env=env
        )
        if p.returncode != 0:
            logger.debug("mmx search returned %d: %s", p.returncode, p.stderr[:200])
            return ''
        return p.stdout
    except FileNotFoundError:
        logger.debug("mmx CLI not found, falling back to tavily")
        return ''
    except Exception as e:
        logger.debug("minimax search failed: %s", e)
        return ''


def _search_with_tavily(query: str) -> str:
    script = config.TAVILY_SEARCH
    if not os.path.exists(script):
        return ''
    try:
        p = subprocess.run(
            ['node', str(script), query],
            capture_output=True, text=True, timeout=60
        )
        return p.stdout if p.returncode == 0 else ''
    except Exception as e:
        logger.debug("tavily search failed: %s", e)
        return ''


def search(query: str) -> str:
    """级联搜索：minimax → tavily。"""
    result = _search_with_minimax(query)
    if result and len(result.strip()) > 50:
        return result
    return _search_with_tavily(query)


# ─────────────────────────────────────────────────────────────────────────────
# 文本处理
# ─────────────────────────────────────────────────────────────────────────────

_EN_TO_ZH = {
    'revenue': '营收', 'profit': '利润', 'net profit': '净利润',
    'gross margin': '毛利率', 'net margin': '净利率',
    'market share': '市场份额', 'guidance': '业绩指引',
    'capex': '资本开支', 'cash flow': '现金流',
    'order backlog': '在手订单', 'shipment': '出货量',
    'policy': '政策', 'regulation': '监管',
    'competition': '竞争', 'technology barrier': '技术壁垒',
}


def _to_chinese_terms(s: str) -> str:
    out = s
    for en, zh in _EN_TO_ZH.items():
        out = re.sub(en, zh, out, flags=re.IGNORECASE)
    return out


def _is_mostly_english(s: str) -> bool:
    if not s:
        return False
    letters = len(re.findall(r'[A-Za-z]', s))
    chinese = len(re.findall(r'[一-鿿]', s))
    return letters > 0 and letters > chinese * 1.2


def clean_text(s: str) -> str:
    s = re.sub(r'\s+', ' ', s or '').strip()
    s = _to_chinese_terms(s)
    return s


def extract_lines(raw: str) -> list:
    """从搜索结果提取有效行，过滤短行和低信任文本。"""
    raw = raw or ''
    lines = []
    for ln in raw.split('\n'):
        ln = clean_text(ln)
        if len(ln) < 16:
            continue
        if is_low_trust_text(ln):
            continue
        if _is_mostly_english(ln):
            ln = f"【英文研报要点-中文化】{ln}"
        lines.append(ln)
    return lines


# ─────────────────────────────────────────────────────────────────────────────
# 来源分级与引用
# ─────────────────────────────────────────────────────────────────────────────

_LOW_TRUST_DOMAINS = [
    'weibo.com', 'xueqiu.com', 'zhihu.com', 'toutiao.com', 'douyin.com',
    'bilibili.com', 'xiaohongshu.com'
]
_HIGH_TRUST_DOMAINS = [
    'cninfo.com.cn', 'sse.com.cn', 'szse.cn',
    'pdf.dfcfw.com', 'eastmoney.com', 'cicc.com',
    'stcn.com', 'cs.com.cn', 'cls.cn', 'thepaper.cn',
    'finance.sina.com.cn', '10jqka.com.cn'
]
_LOW_TRUST_KEYWORDS = ['微博', '雪球', '知乎', '自媒体', '股吧', '论坛', '网友']


def is_low_trust_text(s: str) -> bool:
    t = (s or '').lower()
    return any(k in t for k in _LOW_TRUST_KEYWORDS)


def _is_authoritative_url(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    if any(x in u for x in _LOW_TRUST_DOMAINS):
        return False
    return any(x in u for x in _HIGH_TRUST_DOMAINS)


def extract_refs(raw: str) -> list:
    urls = re.findall(r'https?://[^\s)\]]+', raw or '')
    uniq = []
    for u in urls:
        u = u.rstrip('.,;')
        if u not in uniq:
            uniq.append(u)
    return uniq


def filter_authoritative_refs(refs: list) -> list:
    auth = [u for u in refs if _is_authoritative_url(u)]
    return auth if auth else refs[:2]


def pick_metric(text: str, keyword: str):
    """从文本里提取某关键词附近的百分比，取第一个命中。"""
    if not text:
        return None
    idx = text.find(keyword)
    if idx < 0:
        return None
    seg = text[max(0, idx - 40): idx + 80]
    m = re.search(r'(\d+(?:\.\d+)?)\s*%', seg)
    return float(m.group(1)) if m else None


# ─────────────────────────────────────────────────────────────────────────────
# 公司画像提取
# ─────────────────────────────────────────────────────────────────────────────

def build_company_profile(name: str, code: str, lines: list) -> dict:
    """从搜索结果中动态提取公司业务画像。"""
    good_lines = [l.strip() for l in lines
                  if len(l.strip()) > 20 and not is_low_trust_text(l)]
    txt = ' '.join(good_lines[:80])

    if good_lines:
        first = good_lines[0]
        summary = (first[:120] + '...') if len(first) > 120 else first
    else:
        summary = f"{name}（{code}）为主要业务构成，详见业务描述。"

    # 业务分段提取
    seg_pat = re.compile(
        r'([一-龥a-zA-Z0-9]{2,20})'
        r'(?:业务|产品|板块|分部|服务|收入|销售|制造)[^，。\n]{0,30}?(%|第一|第二|领先|龙头|主要|占比|份额)',
        re.IGNORECASE
    )
    found_segs = []
    seen = set()
    SKIP = {'公司', '公司业务', '主营', '营业收入', '主营业务', '公司产品', '其他', '其他业务'}
    for m in seg_pat.finditer(txt):
        seg_name = m.group(1).strip()
        if seg_name in seen or seg_name in SKIP:
            continue
        seen.add(seg_name)
        raw_desc = m.group(0)[:60]
        pct_val = None
        m_pct = re.search(r'([\d\.]+)\s*%', raw_desc)
        if m_pct:
            try:
                v = float(m_pct.group(1))
                if 0 < v <= 100:
                    pct_val = v
            except Exception:
                pass
        found_segs.append({'segment_name': seg_name, 'market_share_pct': pct_val, 'description': raw_desc})
        if len(found_segs) >= 8:
            break

    # 行业排名提取
    rank_pat = re.compile(
        r'[一-龥]{0,6}'
        r'(?:全球第一|全球第二|行业第一|龙头|第[一二三四五六七八九十\d]+名|领先)'
        r'[一-龥]{0,15}',
        re.IGNORECASE
    )
    rank_stmts = []
    seen_r = set()
    for m in rank_pat.finditer(txt):
        stmt = m.group(0).strip()
        if len(stmt) > 3 and stmt not in seen_r:
            seen_r.add(stmt)
            rank_stmts.append(stmt)
            if len(rank_stmts) >= 5:
                break

    # 净利润增速提取
    ni_growth = None
    for pat in [
        r'净利润[增长同比：:为]*\s*([\-\d\.]+)\s*%?\s*增长',
        r'归母净利润[增长同比：:为]*\s*增长\s*([\-\d\.]+)\s*%',
        r'净利润[增长同比：:为]*\s*([\-\d\.]+)\s*%',
    ]:
        m = re.search(pat, txt)
        if m:
            try:
                ni_growth = float(m.group(1))
                break
            except Exception:
                pass

    return {
        'summary': summary,
        'segments': found_segs,
        'rank_statements': rank_stmts,
        'growth_yoy_pct': ni_growth,
        'unverified_fields_note': '未检索到可验证数值的字段保持 null，避免模糊表述。'
    }


# ─────────────────────────────────────────────────────────────────────────────
# 定性信息收集
# ─────────────────────────────────────────────────────────────────────────────

def _add_source_tag(arr):
    out = []
    for x in arr:
        if str(x).startswith('【英文研报要点-中文化】'):
            out.append(f"{x}（出处：英文研报检索/已中文化）")
        else:
            out.append(f"{x}（出处：中文检索）")
    return out


def collect_qualitative(name: str, code: str) -> dict:
    """构建宏观-行业-公司-增长的认知材料（含引用链接）。"""
    queries = {
        'policy_macro': f'{name} {code} 所在行业 政策 监管 2025 2026',
        'business_model': f'{name} {code} 主营业务构成 行业份额 收入占比 利润贡献',
        'industry_position': f'{name} {code} 行业地位 市场份额 第一 第二 技术壁垒 竞争格局',
        'growth_points': f'{name} {code} 新产品 扩产 订单 海外 增长点 2026',
    }

    bucket = {k: [] for k in queries.keys()}
    refs = {k: [] for k in queries.keys()}
    for k, q in queries.items():
        raw = search(q)
        lines = extract_lines(raw)
        bucket[k] = lines[:6]
        refs[k] = filter_authoritative_refs(extract_refs(raw)[:10])

    # 宏观判断
    macro_view = '中性'
    macro_text = ' '.join(bucket['policy_macro'])
    if any(x in macro_text for x in ['利好', '支持', '鼓励', '增长']):
        macro_view = '偏利好'
    if any(x in macro_text for x in ['收紧', '限制', '监管趋严']):
        macro_view = '偏审慎'

    # 壁垒判断
    moat = '中'
    comp_text = ' '.join(bucket['industry_position'])
    if any(x in comp_text for x in ['龙头', '领先', '第一', '壁垒', '专利']):
        moat = '强'
    elif any(x in comp_text for x in ['同质化', '价格战', '竞争激烈']):
        moat = '弱'

    company_profile = build_company_profile(name, code, bucket['business_model'] + bucket['industry_position'])

    return {
        'macro_policy': {
            'view': macro_view,
            'highlights': _add_source_tag(bucket['policy_macro'][:3]),
            'references': refs['policy_macro']
        },
        'business_profile': {
            'highlights': _add_source_tag(bucket['business_model'][:4]),
            'references': refs['business_model']
        },
        'industry_competition': {
            'moat_level': moat,
            'highlights': _add_source_tag(bucket['industry_position'][:4]),
            'references': refs['industry_position']
        },
        'growth_map': {
            'highlights': _add_source_tag(bucket['growth_points'][:4]),
            'references': refs['growth_points']
        },
        'company_profile': company_profile
    }
