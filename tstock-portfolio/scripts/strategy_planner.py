#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tstock_lib.logging_config import setup_logging
from tstock_lib.constants import (
    SCORE_STRONG, SCORE_NEUTRAL, SCORE_WEAK,
    WEIGHT_FUNDAMENTAL, WEIGHT_TECHNICAL, WEIGHT_RISK,
)


def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def plan(code, f, t, r):
    f_score = f.get('score', 50)
    trend = t.get('trend', '震荡')
    r_score = r.get('risk_score', 50)

    # 技术面映射分
    t_score = 60 if trend == '多头' else (40 if trend == '空头' else 50)

    total = int(
        f_score * WEIGHT_FUNDAMENTAL
        + t_score * WEIGHT_TECHNICAL
        + (100 - r_score) * WEIGHT_RISK
    )

    if total >= SCORE_STRONG:
        action = '买入'
        position = '20%-35%'
    elif total >= SCORE_NEUTRAL:
        action = '持有'
        position = '10%-25%'
    elif total >= SCORE_WEAK:
        action = '观望'
        position = '0%-10%'
    else:
        action = '减仓/卖出'
        position = '0%-5%'

    reasons = []
    reasons += [f"基本面评分: {f_score}", f"技术趋势: {trend}", f"风险评分: {r_score}"]
    reasons += f.get('reasons', [])[:2]
    reasons += t.get('signals', [])[:2]

    return {
        'code': code,
        'action': action,
        'confidence': '高' if total >= SCORE_STRONG else ('中' if total >= SCORE_NEUTRAL else '低'),
        'score': total,
        'position_recommendation': position,
        'stop_ref': t.get('stop_ref'),
        'support_20d': t.get('support_20d'),
        'resistance_20d': t.get('resistance_20d'),
        'reasons': reasons
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--code', required=True)
    p.add_argument('--fundamental', required=True)
    p.add_argument('--technical', required=True)
    p.add_argument('--risk', required=True)
    p.add_argument('--output')
    p.add_argument('--verbose', action='store_true', help='显示详细日志')
    p.add_argument('--debug', action='store_true', help='显示调试日志')
    args = p.parse_args()
    setup_logging("DEBUG" if args.debug else ("INFO" if args.verbose else "WARNING"))

    f = load_json(args.fundamental)
    t = load_json(args.technical)
    r = load_json(args.risk)

    out = plan(args.code, f, t, r)
    text = json.dumps(out, ensure_ascii=False, indent=2)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as fp:
            fp.write(text)
        print(f'已写入: {args.output}')
    else:
        print(text)


if __name__ == '__main__':
    main()
