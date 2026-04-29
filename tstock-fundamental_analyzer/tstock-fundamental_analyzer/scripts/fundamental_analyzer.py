#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path


def _safe_float(v):
    try:
        if v is None:
            return None
        if isinstance(v, str):
            v = v.replace('%', '').replace(',', '').strip()
            if v in ('', '--', 'nan', 'None'):
                return None
        return float(v)
    except Exception:
        return None


def _load_snapshot(code=None, snapshot=None):
    if snapshot:
        with open(snapshot, 'r', encoding='utf-8') as f:
            return json.load(f)

    # 相对于脚本位置动态推导 workspace 根路径
    _ws_root = Path(__file__).resolve().parent.parent.parent.parent
    script = _ws_root / "skills/tstock-data-source/scripts/data_source.py"
    tmp = f'/tmp/{code}_snapshot.json'
    cmd = ['python3', script, '--code', code, '--data-type', 'all', '--output', tmp]
    subprocess.run(cmd, check=True)
    with open(tmp, 'r', encoding='utf-8') as f:
        return json.load(f)


def _search_with_minimax(query: str) -> str:
    """优先调用 minimax-web-search（中文支持好，无需 API Key）。"""
    _root = Path(__file__).resolve().parent.parent.parent.parent
    script = _root / 'skills/minimax-web-search/scripts/web_search.py'
    if not os.path.exists(script):
        return ''
    try:
        p = subprocess.run(
            ['python3', str(script), query],
            capture_output=True, text=True, timeout=60
        )
        return p.stdout if p.returncode == 0 else ''
    except Exception:
        return ''


def _search_with_tavily(query: str) -> str:
    """备选：调用 tavily-search（需 node + API key）。"""
    _root = Path(__file__).resolve().parent.parent.parent.parent.parent
    script = _root / 'skills/tavily-search-1-0-0/scripts/search.mjs'
    if not os.path.exists(script):
        return ''
    try:
        p = subprocess.run(
            ['node', str(script), query],
            capture_output=True, text=True, timeout=60
        )
        return p.stdout if p.returncode == 0 else ''
    except Exception:
        return ''


def _search(query: str) -> str:
    """
    级联搜索策略：
    1. minimax-web-search（首选，中文支持好，无需 API）
    2. tavily-search（备选，通用 AI 搜索）
    
    eastmoney-financial-search 可单独调用（需 EASTMONEY_APIKEY），
    适用于精准金融查询（公告/研报），不在此级联中自动调用。
    """
    result = _search_with_minimax(query)
    if result and len(result.strip()) > 50:
        return result
    # minimax 失败或结果过短，尝试 tavily
    return _search_with_tavily(query)


def _to_chinese_terms(s: str) -> str:
    """常见英文金融术语中文化，避免中英夹杂。"""
    mapping = {
        'revenue': '营收',
        'profit': '利润',
        'net profit': '净利润',
        'gross margin': '毛利率',
        'net margin': '净利率',
        'market share': '市场份额',
        'guidance': '业绩指引',
        'capex': '资本开支',
        'cash flow': '现金流',
        'order backlog': '在手订单',
        'shipment': '出货量',
        'policy': '政策',
        'regulation': '监管',
        'competition': '竞争',
        'technology barrier': '技术壁垒',
    }
    out = s
    for en, zh in mapping.items():
        out = re.sub(en, zh, out, flags=re.IGNORECASE)
    return out


def _is_mostly_english(s: str) -> bool:
    if not s:
        return False
    letters = len(re.findall(r'[A-Za-z]', s))
    chinese = len(re.findall(r'[\u4e00-\u9fff]', s))
    return letters > 0 and letters > chinese * 1.2


def _clean_text(s: str) -> str:
    s = re.sub(r'\s+', ' ', s or '').strip()
    s = _to_chinese_terms(s)
    return s


def _extract_lines(raw: str):
    raw = raw or ''
    lines = []
    for ln in raw.split('\n'):
        ln = _clean_text(ln)
        if len(ln) < 16:
            continue
        # 英文高占比内容不丢弃：中文化后保留，避免漏掉高质量英文研报信息
        if _is_mostly_english(ln):
            # 轻量中文化（术语替换）+ 标注来源属性
            ln = f"【英文研报要点-中文化】{ln}"
        lines.append(ln)
    return lines


def _fetch_valuation_compare(snapshot: dict) -> dict:
    """
    估值由 tstock-data-source 独家提供，下游只读取本函数输出。

    数据优先级：
    1. valuation_stable（AkShare/东方财富原始口径）
    2. valuation_comparable（Transform层，同花顺二级行业正确均值）
       → 用于覆盖 industry_avg / premium_pct
    3. basic（个股基本信息兜底）
    """
    val = snapshot.get('valuation_stable') or {}
    comp = snapshot.get('valuation_comparable') or {}

    # 行业均值 & 溢价率：优先用 Transform 层（来自同花顺正确二级行业）
    industry_avg = {
        'pe': comp.get('industry_pe_median') or val.get('industry_avg', {}).get('pe'),
        'pb': comp.get('industry_pb_median') or val.get('industry_avg', {}).get('pb'),
        'pr': None,
        'industry_name': comp.get('industry_name'),
    }
    premium_pct = {
        'pe': comp.get('premium_vs_industry_pe_pct'),
        'pb': comp.get('premium_vs_industry_pb_pct'),
        'pr': None,
    }

    return {
        'pe': val.get('pe_ttm') if val.get('pe_ttm') is not None else _safe_float((snapshot.get('basic') or {}).get('pe_ttm')),
        'pb': val.get('pb') if val.get('pb') is not None else _safe_float((snapshot.get('basic') or {}).get('pb')),
        'pr': val.get('pr'),
        'peg': val.get('peg'),
        'growth_yoy_pct': val.get('growth_yoy_pct'),
        'industry_avg': industry_avg,
        'premium_pct': premium_pct,
        'assessment': val.get('assessment', {}),
        'sample_size': val.get('sample_size', 0),
        'meta': val.get('meta', {
            'valuation_basis': 'PE(TTM)/PB(当前)/PR(TTM)/PEG=PE÷净利润同比增速%，行业均值来自同花顺二级行业（Transform层）',
            'as_of': datetime.now().strftime('%Y-%m-%d'),
            'source_used': []
        }),
    }


def _extract_refs(raw: str) -> list:
    urls = re.findall(r'https?://[^\s)\]]+', raw or '')
    uniq = []
    for u in urls:
        u = u.rstrip('.,;')
        if u not in uniq:
            uniq.append(u)
    return uniq


def _is_authoritative_url(url: str) -> bool:
    """来源分级：优先官方公告/券商研报/主流财经媒体。"""
    if not url:
        return False
    u = url.lower()

    # 明确降权来源（社交/问答）
    low_trust = [
        'weibo.com', 'xueqiu.com', 'zhihu.com', 'toutiao.com', 'douyin.com',
        'bilibili.com', 'xiaohongshu.com'
    ]
    if any(x in u for x in low_trust):
        return False

    # 权威来源白名单（交易所/公司/券商研报/主流财经）
    high_trust = [
        'cninfo.com.cn', 'sse.com.cn', 'szse.cn',                    # 公告/交易所
        'pdf.dfcfw.com', 'eastmoney.com', 'cicc.com',                # 研报/券商/东财
        'stcn.com', 'cs.com.cn', 'cls.cn', 'thepaper.cn',            # 主流财经媒体
        'finance.sina.com.cn', '10jqka.com.cn'
    ]
    return any(x in u for x in high_trust)


def _filter_authoritative_refs(refs: list) -> list:
    auth = [u for u in refs if _is_authoritative_url(u)]
    # 若权威源为空，保留前2个兜底，避免完全无引用
    return auth if auth else refs[:2]


def _is_low_trust_text(s: str) -> bool:
    t = (s or '').lower()
    bad_kw = ['微博', '雪球', '知乎', '自媒体', '股吧', '论坛', '网友']
    return any(k in t for k in bad_kw)


def _pick_metric(text: str, keyword: str):
    """从文本里提取某关键词附近的百分比，取第一个命中。"""
    if not text:
        return None
    idx = text.find(keyword)
    if idx < 0:
        return None
    seg = text[max(0, idx - 40): idx + 80]
    m = re.search(r'(\d+(?:\.\d+)?)\s*%', seg)
    return float(m.group(1)) if m else None


def _build_company_profile(name: str, code: str, lines: list) -> dict:
    """
    从搜索结果中动态提取公司业务画像（完全通用版）。
    不硬编码任何产品名或行业关键词，完全依赖实际抓取内容。
    """
    # 过滤低质量内容行
    good_lines = [l.strip() for l in lines
                  if len(l.strip()) > 20 and not _is_low_trust_text(l)]
    txt = ' '.join(good_lines[:80])

    # ── 1. 生成摘要：从第一段有实质内容的句子截取 ──
    if good_lines:
        first = good_lines[0]
        summary = (first[:120] + '...') if len(first) > 120 else first
    else:
        summary = f"{name}（{code}）为主要业务构成，详见业务描述。"

    # ── 2. 动态发现主营构成（通用模式） ──
    # 匹配模式：XX业务/产品/板块 + 量化词
    seg_pat = re.compile(
        r'([\u4e00-\u9fa5a-zA-Z0-9]{2,20})'
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
        # 提 % 数值
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

    # ── 3. 动态发现行业位次描述 ──
    rank_pat = re.compile(
        r'[\u4e00-\u9fa5]{0,6}'
        r'(?:全球第一|全球第二|行业第一|龙头|第[一二三四五六七八九十\d]+名|领先)'
        r'[\u4e00-\u9fa5]{0,15}',
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

    # ── 4. 动态提取净利润增速 ──
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

    # ── 5. 构建输出 ──
    return {
        'summary': summary,
        'segments': found_segs,
        'rank_statements': rank_stmts,
        'growth_yoy_pct': ni_growth,
        'unverified_fields_note': '未检索到可验证数值的字段保持 null，避免模糊表述。'
    }


def _collect_qualitative(name: str, code: str) -> dict:
    """构建宏观-行业-公司-增长的认知材料（含引用链接）。"""
    queries = {
        'policy_macro': f'{name} {code} 所在行业 政策 监管 2025 2026',
        'business_model': f'{name} {code} 主营业务构成 行业份额 收入占比 利润贡献',
        'industry_position': f'{name} {code} 行业地位 市场份额 第一 第二 技术壁垒 竞争格局',
        'growth_points': f'{name} {code} 新产品 扩产 订单 海外 增长点 2026',
    }

    bucket = {k: [] for k in queries.keys()}
    refs = {k: [] for k in queries.keys()}
    raw_map = {}
    for k, q in queries.items():
        raw = _search(q)
        raw_map[k] = raw
        lines = [x for x in _extract_lines(raw) if not _is_low_trust_text(x)]
        bucket[k] = lines[:6]
        refs[k] = _filter_authoritative_refs(_extract_refs(raw)[:10])

    # 从抓取内容中给出简化标签
    macro_view = '中性'
    macro_text = ' '.join(bucket['policy_macro'])
    if any(x in macro_text for x in ['利好', '支持', '鼓励', '增长']):
        macro_view = '偏利好'
    if any(x in macro_text for x in ['收紧', '限制', '监管趋严']):
        macro_view = '偏审慎'

    moat = '中'
    comp_text = ' '.join(bucket['industry_position'])
    if any(x in comp_text for x in ['龙头', '领先', '第一', '壁垒', '专利']):
        moat = '强'
    elif any(x in comp_text for x in ['同质化', '价格战', '竞争激烈']):
        moat = '弱'

    def add_source_tag(arr):
        out = []
        for x in arr:
            if str(x).startswith('【英文研报要点-中文化】'):
                out.append(f"{x}（出处：英文研报检索/已中文化）")
            else:
                out.append(f"{x}（出处：中文检索）")
        return out

    company_profile = _build_company_profile(name, code, bucket['business_model'] + bucket['industry_position'])

    return {
        'macro_policy': {
            'view': macro_view,
            'highlights': add_source_tag(bucket['policy_macro'][:3]),
            'references': refs['policy_macro']
        },
        'business_profile': {
            'highlights': add_source_tag(bucket['business_model'][:4]),
            'references': refs['business_model']
        },
        'industry_competition': {
            'moat_level': moat,
            'highlights': add_source_tag(bucket['industry_position'][:4]),
            'references': refs['industry_position']
        },
        'growth_map': {
            'highlights': add_source_tag(bucket['growth_points'][:4]),
            'references': refs['growth_points']
        },
        'company_profile': company_profile
    }


def _build_scorecard(result: dict) -> dict:
    """固定打分卡：政策、行业、壁垒、增长（各10分）"""
    q = result.get('qualitative', {}) or {}
    macro = q.get('macro_policy', {}) or {}
    comp = q.get('industry_competition', {}) or {}
    growth = q.get('growth_map', {}) or {}

    # 政策分
    macro_view = str(macro.get('view', '中性'))
    policy_score = 6
    if '偏利好' in macro_view:
        policy_score = 8
    elif '偏审慎' in macro_view:
        policy_score = 4

    # 行业与竞争（用行业信息存在性 + 文本条数粗量化）
    industry_score = min(10, 5 + len(comp.get('highlights', []) or []))

    # 壁垒分
    moat = str(comp.get('moat_level', '中'))
    moat_score = 6
    if moat == '强':
        moat_score = 9
    elif moat == '弱':
        moat_score = 3

    # 增长分
    growth_score = min(10, 4 + len(growth.get('highlights', []) or []))

    total = policy_score + industry_score + moat_score + growth_score
    return {
        'policy': policy_score,
        'industry': industry_score,
        'moat': moat_score,
        'growth': growth_score,
        'total': total,
        'max': 40
    }


def analyze(snapshot, with_qualitative: bool = True):
    basic = snapshot.get('basic', {})
    valuation = snapshot.get('valuation', {}) or {}
    baostock = snapshot.get('baostock', {}) or {}

    pe = _safe_float(basic.get('pe_ttm'))
    pb = _safe_float(basic.get('pb'))
    pe_pct = _safe_float(valuation.get('pe_ttm_percentile'))
    pb_pct = _safe_float(valuation.get('pb_percentile'))

    # 尝试从 baostock 拿关键指标
    profit = baostock.get('profit') or {}
    balance = baostock.get('balance') or {}

    roe = _safe_float(profit.get('roeAvg'))
    np_margin = _safe_float(profit.get('npMargin'))
    gp_margin = _safe_float(profit.get('gpMargin'))
    debt_ratio = _safe_float(balance.get('liabilityToAsset'))
    current_ratio = _safe_float(balance.get('currentRatio'))

    score = 50
    reasons = []

    if roe is not None:
        if roe >= 0.2:
            score += 12; reasons.append('ROE优秀')
        elif roe >= 0.12:
            score += 6; reasons.append('ROE良好')
        else:
            score -= 5; reasons.append('ROE偏弱')

    if np_margin is not None:
        if np_margin > 0.1:
            score += 8; reasons.append('净利率较好')
        elif np_margin < 0.03:
            score -= 8; reasons.append('净利率偏低')

    if debt_ratio is not None:
        if debt_ratio < 0.6:
            score += 5; reasons.append('负债水平可控')
        else:
            score -= 8; reasons.append('杠杆偏高')

    if pe_pct is not None:
        if pe_pct > 80:
            score -= 8; reasons.append('PE历史分位偏高')
        elif pe_pct < 30:
            score += 5; reasons.append('PE分位较低')

    score = max(0, min(100, int(score)))

    if score >= 70:
        view = '基本面较强'
    elif score >= 55:
        view = '基本面中性偏强'
    elif score >= 40:
        view = '基本面一般'
    else:
        view = '基本面偏弱'

    valuation_compare = _fetch_valuation_compare(snapshot)
    # 尽量补齐 pe/pb
    pe_final = pe if pe is not None else valuation_compare.get('pe')
    pb_final = pb if pb is not None else valuation_compare.get('pb')

    result = {
        'code': snapshot.get('code'),
        'name': basic.get('name'),
        'analysis_date': datetime.now().strftime('%Y-%m-%d'),
        'profitability': {
            'roe': roe,
            'net_margin': np_margin,
            'gross_margin': gp_margin,
        },
        'financial_health': {
            'debt_ratio': debt_ratio,
            'current_ratio': current_ratio,
        },
        'valuation': {
            'pe_ttm': pe_final,
            'pb': pb_final,
            'pr': valuation_compare.get('pr'),
            'peg': valuation_compare.get('peg'),
            'growth_yoy_pct': valuation_compare.get('growth_yoy_pct'),
            'pe_percentile': pe_pct,
            'pb_percentile': pb_pct,
            'industry_avg': valuation_compare.get('industry_avg'),
            'premium_pct': valuation_compare.get('premium_pct'),
            'assessment': valuation_compare.get('assessment'),
            'sample_size': valuation_compare.get('sample_size'),
            'meta': valuation_compare.get('meta'),
        },
        'score': score,
        'view': view,
        'reasons': reasons,
        'quality': snapshot.get('quality', {})
    }

    if with_qualitative:
        result['qualitative'] = _collect_qualitative(basic.get('name') or snapshot.get('code'), snapshot.get('code'))
    else:
        result['qualitative'] = {}

    result['scorecard'] = _build_scorecard(result)

    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--code', help='股票代码')
    p.add_argument('--snapshot', help='快照JSON路径')
    p.add_argument('--output', help='输出文件')
    p.add_argument('--no-qualitative', action='store_true', help='禁用定性信息抓取')
    args = p.parse_args()

    if not args.code and not args.snapshot:
        raise SystemExit('请提供 --code 或 --snapshot')

    snap = _load_snapshot(args.code, args.snapshot)
    result = analyze(snap, with_qualitative=not args.no_qualitative)
    text = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f'已写入: {args.output}')
    else:
        print(text)


if __name__ == '__main__':
    main()
