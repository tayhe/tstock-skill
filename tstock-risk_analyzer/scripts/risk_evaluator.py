#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tstock.logging_config import setup_logging
from tstock.utils import safe_float
from tstock.snapshot import load_snapshot
from tstock.constants import (
    RISK_PE_HIGH, RISK_PE_MED, RISK_PB_HIGH, RISK_PE_PCT_HIGH,
    RISK_DEBT_HIGH, RISK_DEBT_MED, RISK_CURRENT_LOW,
    RISK_MCAP_SMALL, RISK_MCAP_MID,
    RISK_AMPLITUDE_HIGH, RISK_AMPLITUDE_MED, RISK_PRICE_CHANGE_HIGH,
    HIGH_RISK_INDUSTRIES, RISK_LOW_THRESHOLD, RISK_MED_THRESHOLD,
)


def evaluate(snapshot, fundamental=None):
    basic = snapshot.get('basic', {})
    market = snapshot.get('market', {})
    valuation = snapshot.get('valuation', {})
    baostock = snapshot.get('baostock', {})

    factors = []

    pe = safe_float(basic.get('pe_ttm'))
    pb = safe_float(basic.get('pb'))
    pe_pct = safe_float(valuation.get('pe_ttm_percentile'))
    val_score = 30
    if pe and pe > RISK_PE_HIGH:
        val_score += 30
    elif pe and pe > RISK_PE_MED:
        val_score += 15
    if pb and pb > RISK_PB_HIGH:
        val_score += 15
    if pe_pct and pe_pct > RISK_PE_PCT_HIGH:
        val_score += 10
    factors.append({'name': '估值风险', 'score': min(100, val_score)})

    bal = baostock.get('balance') or {}
    debt = safe_float(bal.get('liabilityToAsset'))
    curr = safe_float(bal.get('currentRatio'))
    fin_score = 30
    if debt and debt > RISK_DEBT_HIGH:
        fin_score += 25
    elif debt and debt > RISK_DEBT_MED:
        fin_score += 10
    if curr and curr < RISK_CURRENT_LOW:
        fin_score += 15
    factors.append({'name': '财务风险', 'score': min(100, fin_score)})

    pchg = safe_float(market.get('price_change_pct'))
    hi = safe_float(market.get('high_60d'))
    lo = safe_float(market.get('low_60d'))
    vol_score = 30
    if hi and lo and lo > 0:
        amp = (hi - lo) / lo
        if amp > RISK_AMPLITUDE_HIGH:
            vol_score += 25
        elif amp > RISK_AMPLITUDE_MED:
            vol_score += 12
    if pchg and abs(pchg) > RISK_PRICE_CHANGE_HIGH:
        vol_score += 10
    factors.append({'name': '波动风险', 'score': min(100, vol_score)})

    mcap = safe_float(basic.get('market_cap'))
    liq_score = 20
    if mcap and mcap < RISK_MCAP_SMALL:
        liq_score += 20
    elif mcap and mcap < RISK_MCAP_MID:
        liq_score += 10
    factors.append({'name': '流动性风险', 'score': min(100, liq_score)})

    industry = str(basic.get('industry', ''))
    ind_score = 25
    for k in HIGH_RISK_INDUSTRIES:
        if k in industry:
            ind_score += 20

    macro_signals = []
    if fundamental:
        q = (fundamental.get('qualitative') or {})
        macro = q.get('macro_policy', {})
        comp = q.get('industry_competition', {})

        macro_view = str(macro.get('view', ''))
        if '审慎' in macro_view or '偏空' in macro_view:
            ind_score += 10
            macro_signals.append('政策环境偏审慎')

        moat = str(comp.get('moat_level', ''))
        if moat == '弱':
            ind_score += 10
            macro_signals.append('竞争壁垒偏弱')
        elif moat == '强':
            ind_score -= 5
            macro_signals.append('竞争壁垒较强')

    factors.append({'name': '行业风险', 'score': max(0, min(100, ind_score))})

    risk_score = int(sum(f['score'] for f in factors) / len(factors))
    overall = '低' if risk_score < RISK_LOW_THRESHOLD else ('中等' if risk_score < RISK_MED_THRESHOLD else '高')

    rec = []
    if overall == '高':
        rec += ['建议降仓或回避', '必须设置止损', '等待波动收敛后再评估']
    elif overall == '中等':
        rec += ['控制单票仓位', '分批建仓', '设置硬止损']
    else:
        rec += ['风险可控，按计划执行', '继续跟踪财报与行业数据']

    if macro_signals:
        rec.append('宏观/行业信号：' + '；'.join(macro_signals))

    return {
        'code': snapshot.get('code'),
        'name': basic.get('name'),
        'risk_score': risk_score,
        'overall_risk': overall,
        'factors': factors,
        'macro_signals': macro_signals,
        'recommendations': rec,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--code')
    p.add_argument('--snapshot')
    p.add_argument('--fundamental-json')
    p.add_argument('--output')
    p.add_argument('--verbose', action='store_true', help='显示详细日志')
    p.add_argument('--debug', action='store_true', help='显示调试日志')
    args = p.parse_args()
    setup_logging("DEBUG" if args.debug else ("INFO" if args.verbose else "WARNING"))

    if not args.code and not args.snapshot:
        raise SystemExit('请提供 --code 或 --snapshot')

    snap = load_snapshot(args.code, args.snapshot, data_type="all")
    fund = None
    if args.fundamental_json:
        with open(args.fundamental_json, 'r', encoding='utf-8') as f:
            fund = json.load(f)
    result = evaluate(snap, fund)
    text = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f'已写入: {args.output}')
    else:
        print(text)


if __name__ == '__main__':
    main()
