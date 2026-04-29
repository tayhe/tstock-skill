#!/usr/bin/env python3
import argparse
import json


def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def plan(code, f, t, r):
    f_score = f.get('score', 50)
    trend = t.get('trend', '震荡')
    r_score = r.get('risk_score', 50)

    # 技术面映射分
    t_score = 60 if trend == '多头' else (40 if trend == '空头' else 50)

    # 综合：基本面40 + 技术面25 + 风险35(反向)
    total = int(f_score * 0.4 + t_score * 0.25 + (100 - r_score) * 0.35)

    if total >= 70:
        action = '买入'
        position = '20%-35%'
    elif total >= 55:
        action = '持有'
        position = '10%-25%'
    elif total >= 40:
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
        'confidence': '高' if total >= 70 else ('中' if total >= 55 else '低'),
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
    args = p.parse_args()

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
