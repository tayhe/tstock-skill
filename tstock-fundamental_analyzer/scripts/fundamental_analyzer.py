#!/usr/bin/env python3
"""基本面分析：定量财务评分 + 定性信息整合。"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tstock_lib.logging_config import setup_logging
from tstock_lib.utils import safe_float
from tstock_lib.snapshot import load_snapshot
from tstock_lib.constants import (
    SCORE_BASE, SCORE_STRONG, SCORE_NEUTRAL, SCORE_WEAK,
    ROE_EXCELLENT, ROE_GOOD, NET_MARGIN_GOOD, NET_MARGIN_LOW,
    DEBT_SAFE, PE_PCT_HIGH, PE_PCT_LOW,
)
from web_research import collect_qualitative


def _fetch_valuation_compare(snapshot: dict) -> dict:
    """估值由 tstock-data-source 独家提供，下游只读取本函数输出。"""
    val = snapshot.get('valuation_stable') or {}
    comp = snapshot.get('valuation_comparable') or {}

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
        'pe': val.get('pe_ttm') if val.get('pe_ttm') is not None else safe_float((snapshot.get('basic') or {}).get('pe_ttm')),
        'pb': val.get('pb') if val.get('pb') is not None else safe_float((snapshot.get('basic') or {}).get('pb')),
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


def _build_scorecard(result: dict) -> dict:
    """固定打分卡：政策、行业、壁垒、增长（各10分）"""
    q = result.get('qualitative', {}) or {}
    macro = q.get('macro_policy', {}) or {}
    comp = q.get('industry_competition', {}) or {}
    growth = q.get('growth_map', {}) or {}

    macro_view = str(macro.get('view', '中性'))
    policy_score = 6
    if '偏利好' in macro_view:
        policy_score = 8
    elif '偏审慎' in macro_view:
        policy_score = 4

    industry_score = min(10, 5 + len(comp.get('highlights', []) or []))

    moat = str(comp.get('moat_level', '中'))
    moat_score = 6
    if moat == '强':
        moat_score = 9
    elif moat == '弱':
        moat_score = 3

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
    """执行基本面分析，返回结构化结果。"""
    basic = snapshot.get('basic', {})
    valuation = snapshot.get('valuation', {}) or {}
    baostock = snapshot.get('baostock', {}) or {}

    pe = safe_float(basic.get('pe_ttm'))
    pb = safe_float(basic.get('pb'))
    pe_pct = safe_float(valuation.get('pe_ttm_percentile'))
    pb_pct = safe_float(valuation.get('pb_percentile'))

    profit = baostock.get('profit') or {}
    balance = baostock.get('balance') or {}

    roe = safe_float(profit.get('roeAvg'))
    np_margin = safe_float(profit.get('npMargin'))
    gp_margin = safe_float(profit.get('gpMargin'))
    debt_ratio = safe_float(balance.get('liabilityToAsset'))
    current_ratio = safe_float(balance.get('currentRatio'))

    # 定量评分
    score = SCORE_BASE
    reasons = []

    if roe is not None:
        if roe >= ROE_EXCELLENT:
            score += 12; reasons.append('ROE优秀')
        elif roe >= ROE_GOOD:
            score += 6; reasons.append('ROE良好')
        else:
            score -= 5; reasons.append('ROE偏弱')

    if np_margin is not None:
        if np_margin > NET_MARGIN_GOOD:
            score += 8; reasons.append('净利率较好')
        elif np_margin < NET_MARGIN_LOW:
            score -= 8; reasons.append('净利率偏低')

    if debt_ratio is not None:
        if debt_ratio < DEBT_SAFE:
            score += 5; reasons.append('负债水平可控')
        else:
            score -= 8; reasons.append('杠杆偏高')

    if pe_pct is not None:
        if pe_pct > PE_PCT_HIGH:
            score -= 8; reasons.append('PE历史分位偏高')
        elif pe_pct < PE_PCT_LOW:
            score += 5; reasons.append('PE分位较低')

    score = max(0, min(100, int(score)))

    if score >= SCORE_STRONG:
        view = '基本面较强'
    elif score >= SCORE_NEUTRAL:
        view = '基本面中性偏强'
    elif score >= SCORE_WEAK:
        view = '基本面一般'
    else:
        view = '基本面偏弱'

    valuation_compare = _fetch_valuation_compare(snapshot)
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

    # 定性信息
    if with_qualitative:
        result['qualitative'] = collect_qualitative(basic.get('name') or snapshot.get('code'), snapshot.get('code'))
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
    p.add_argument('--verbose', action='store_true', help='显示详细日志')
    p.add_argument('--debug', action='store_true', help='显示调试日志')
    args = p.parse_args()
    setup_logging("DEBUG" if args.debug else ("INFO" if args.verbose else "WARNING"))

    if not args.code and not args.snapshot:
        raise SystemExit('请提供 --code 或 --snapshot')

    snap = load_snapshot(args.code, args.snapshot, data_type="all")
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
