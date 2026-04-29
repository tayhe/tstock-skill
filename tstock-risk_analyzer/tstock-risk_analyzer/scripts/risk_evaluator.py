#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
from pathlib import Path


def _safe(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def get_snapshot(code=None, snapshot=None):
    if snapshot:
        with open(snapshot, 'r', encoding='utf-8') as f:
            return json.load(f)
    # 相对于脚本位置动态推导 workspace 根路径
    _ws_root = Path(__file__).resolve().parent.parent.parent.parent
    script = _ws_root / "skills/tstock-data-source/scripts/data_source.py"
    tmp = f'/tmp/{code}_risk_snapshot.json'
    subprocess.run(['python3', script, '--code', code, '--data-type', 'all', '--output', tmp], check=True)
    with open(tmp, 'r', encoding='utf-8') as f:
        return json.load(f)


def _load_json(path):
    if not path:
        return None
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def evaluate(snapshot, fundamental: dict | None = None):
    basic = snapshot.get('basic', {})
    market = snapshot.get('market', {})
    valuation = snapshot.get('valuation', {})
    baostock = snapshot.get('baostock', {})

    factors = []

    pe = _safe(basic.get('pe_ttm'))
    pb = _safe(basic.get('pb'))
    pe_pct = _safe(valuation.get('pe_ttm_percentile'))
    val_score = 30
    if pe and pe > 60:
        val_score += 30
    elif pe and pe > 40:
        val_score += 15
    if pb and pb > 8:
        val_score += 15
    if pe_pct and pe_pct > 85:
        val_score += 10
    factors.append({'name': '估值风险', 'score': min(100, val_score)})

    bal = baostock.get('balance') or {}
    debt = _safe(bal.get('liabilityToAsset'))
    curr = _safe(bal.get('currentRatio'))
    fin_score = 30
    if debt and debt > 0.7:
        fin_score += 25
    elif debt and debt > 0.6:
        fin_score += 10
    if curr and curr < 1.0:
        fin_score += 15
    factors.append({'name': '财务风险', 'score': min(100, fin_score)})

    pchg = _safe(market.get('price_change_pct'))
    hi = _safe(market.get('high_60d'))
    lo = _safe(market.get('low_60d'))
    vol_score = 30
    if hi and lo and lo > 0:
        amp = (hi - lo) / lo
        if amp > 0.5:
            vol_score += 25
        elif amp > 0.3:
            vol_score += 12
    if pchg and abs(pchg) > 7:
        vol_score += 10
    factors.append({'name': '波动风险', 'score': min(100, vol_score)})

    mcap = _safe(basic.get('market_cap'))
    liq_score = 20
    if mcap and mcap < 5e10:
        liq_score += 20
    elif mcap and mcap < 1e11:
        liq_score += 10
    factors.append({'name': '流动性风险', 'score': min(100, liq_score)})

    industry = str(basic.get('industry', ''))
    ind_score = 25
    for k in ['房地产', '游戏', '教育']:
        if k in industry:
            ind_score += 20

    # 使用 Fundamental 的定性信息进行风险映射
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
    overall = '低' if risk_score < 35 else ('中等' if risk_score < 60 else '高')

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
        'overall_risk': overall,
        'risk_score': risk_score,
        'factors': factors,
        'macro_signals': macro_signals,
        'recommendations': rec,
        'quality': snapshot.get('quality', {})
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--code')
    p.add_argument('--snapshot')
    p.add_argument('--fundamental-json', help='tstock-fundamental_analyzer 输出JSON（可选）')
    p.add_argument('--output')
    args = p.parse_args()

    if not args.code and not args.snapshot:
        raise SystemExit('请提供 --code 或 --snapshot')

    snap = get_snapshot(args.code, args.snapshot)
    fundamental = _load_json(args.fundamental_json)
    result = evaluate(snap, fundamental=fundamental)
    text = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f'已写入: {args.output}')
    else:
        print(text)


if __name__ == '__main__':
    main()
