#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import pandas as pd
from pathlib import Path


def load_snapshot(code=None, snapshot=None):
    if snapshot:
        with open(snapshot, 'r', encoding='utf-8') as f:
            return json.load(f)
    # 相对于脚本位置动态推导 workspace 根路径
    _ws_root = Path(__file__).resolve().parent.parent.parent.parent
    script = _ws_root / "skills/tstock-data-source/scripts/data_source.py"
    tmp = f'/tmp/{code}_tech_snapshot.json'
    subprocess.run(['python3', script, '--code', code, '--data-type', 'core', '--output', tmp], check=True)
    with open(tmp, 'r', encoding='utf-8') as f:
        return json.load(f)


def calc_indicators(df: pd.DataFrame):
    df = df.copy()
    df['close'] = pd.to_numeric(df['收盘'], errors='coerce')
    df['high'] = pd.to_numeric(df['最高'], errors='coerce')
    df['low'] = pd.to_numeric(df['最低'], errors='coerce')

    for n in (5, 10, 20, 60):
        df[f'ma{n}'] = df['close'].rolling(n).mean()

    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_dif'] = ema12 - ema26
    df['macd_dea'] = df['macd_dif'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = (df['macd_dif'] - df['macd_dea']) * 2

    delta = df['close'].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, pd.NA)
    df['rsi14'] = 100 - (100 / (1 + rs))

    m = df['close'].rolling(20).mean()
    s = df['close'].rolling(20).std()
    df['boll_mid'] = m
    df['boll_up'] = m + 2 * s
    df['boll_dn'] = m - 2 * s

    # KDJ (9,3,3)
    low_n = df['low'].rolling(9).min()
    high_n = df['high'].rolling(9).max()
    rsv = (df['close'] - low_n) / (high_n - low_n).replace(0, pd.NA) * 100
    df['kdj_k'] = rsv.ewm(alpha=1/3, adjust=False).mean()
    df['kdj_d'] = df['kdj_k'].ewm(alpha=1/3, adjust=False).mean()
    df['kdj_j'] = 3 * df['kdj_k'] - 2 * df['kdj_d']

    tr1 = (df['high'] - df['low']).abs()
    tr2 = (df['high'] - df['close'].shift(1)).abs()
    tr3 = (df['low'] - df['close'].shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()
    return df


def analyze(snapshot):
    rows = snapshot.get('market', {}).get('price_data', [])
    if not rows:
        return {'error': '缺少行情数据'}
    df = pd.DataFrame(rows)
    df = calc_indicators(df)
    last = df.iloc[-1]

    close = float(last['close'])
    ma20 = float(last['ma20']) if pd.notna(last['ma20']) else None
    ma60 = float(last['ma60']) if pd.notna(last['ma60']) else None

    trend = '震荡'
    if ma20 and ma60:
        if close > ma20 > ma60:
            trend = '多头'
        elif close < ma20 < ma60:
            trend = '空头'

    rsi = float(last['rsi14']) if pd.notna(last['rsi14']) else None
    macd_hist = float(last['macd_hist']) if pd.notna(last['macd_hist']) else None

    signal = []
    if macd_hist is not None:
        signal.append('MACD偏多' if macd_hist > 0 else 'MACD偏空')
    if rsi is not None:
        if rsi >= 70:
            signal.append('RSI超买')
        elif rsi <= 30:
            signal.append('RSI超卖')
        else:
            signal.append('RSI中性')

    k = float(last['kdj_k']) if pd.notna(last['kdj_k']) else None
    d = float(last['kdj_d']) if pd.notna(last['kdj_d']) else None
    j = float(last['kdj_j']) if pd.notna(last['kdj_j']) else None

    boll_up = float(last['boll_up']) if pd.notna(last['boll_up']) else None
    boll_mid = float(last['boll_mid']) if pd.notna(last['boll_mid']) else None
    boll_dn = float(last['boll_dn']) if pd.notna(last['boll_dn']) else None

    if k is not None and d is not None:
        signal.append('KDJ金叉' if k > d else 'KDJ死叉')
    if j is not None:
        if j > 100:
            signal.append('KDJ高位钝化风险')
        elif j < 0:
            signal.append('KDJ低位修复机会')

    if boll_up is not None and boll_dn is not None:
        if close > boll_up:
            signal.append('BOLL上轨突破')
        elif close < boll_dn:
            signal.append('BOLL下轨跌破')
        else:
            signal.append('BOLL区间运行')

    rolling20 = df['close'].tail(20)
    support = float(rolling20.min()) if len(rolling20) else None
    resistance = float(rolling20.max()) if len(rolling20) else None

    atr = float(last['atr14']) if pd.notna(last['atr14']) else None
    stop_ref = (close - 1.5 * atr) if atr else None

    return {
        'code': snapshot.get('code'),
        'name': snapshot.get('basic', {}).get('name'),
        'trend': trend,
        'close': close,
        'signals': signal,
        'rsi14': rsi,
        'macd_hist': macd_hist,
        'kdj': {'k': k, 'd': d, 'j': j},
        'boll': {'up': boll_up, 'mid': boll_mid, 'dn': boll_dn},
        'support_20d': support,
        'resistance_20d': resistance,
        'atr14': atr,
        'stop_ref': round(stop_ref, 2) if stop_ref else None,
        'quality': snapshot.get('quality', {})
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--code')
    p.add_argument('--snapshot')
    p.add_argument('--output')
    args = p.parse_args()

    if not args.code and not args.snapshot:
        raise SystemExit('请提供 --code 或 --snapshot')

    snap = load_snapshot(args.code, args.snapshot)
    result = analyze(snap)
    text = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f'已写入: {args.output}')
    else:
        print(text)


if __name__ == '__main__':
    main()
