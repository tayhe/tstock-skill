#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tstock_lib.logging_config import setup_logging

import pandas as pd

from tstock_lib.snapshot import load_snapshot
from tstock_lib.constants import ATR_STOP_MULTIPLIER


def calc_indicators(df: pd.DataFrame):
    df = df.copy()
    df['close'] = pd.to_numeric(df['收盘'], errors='coerce')

    for w in [5, 10, 20, 60]:
        df[f'ma{w}'] = df['close'].rolling(w).mean()

    delta = df['close'].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, 1e-9)
    df['rsi14'] = 100 - 100 / (1 + rs)

    ema12 = df['close'].ewm(span=12).mean()
    ema26 = df['close'].ewm(span=26).mean()
    df['macd_dif'] = ema12 - ema26
    df['macd_dea'] = df['macd_dif'].ewm(span=9).mean()
    df['macd_hist'] = 2 * (df['macd_dif'] - df['macd_dea'])

    df['boll_mid'] = df['close'].rolling(20).mean()
    std20 = df['close'].rolling(20).std()
    df['boll_up'] = df['boll_mid'] + 2 * std20
    df['boll_dn'] = df['boll_mid'] - 2 * std20

    low9 = df['close'].rolling(9).min()
    high9 = df['close'].rolling(9).max()
    rsv = (df['close'] - low9) / (high9 - low9).replace(0, 1e-9) * 100
    df['kdj_k'] = rsv.ewm(com=2).mean()
    df['kdj_d'] = df['kdj_k'].ewm(com=2).mean()
    df['kdj_j'] = 3 * df['kdj_k'] - 2 * df['kdj_d']

    high = pd.to_numeric(df['最高'], errors='coerce')
    low = pd.to_numeric(df['最低'], errors='coerce')
    prev_close = df['close'].shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()

    return df


def analyze(snapshot):
    price_data = snapshot.get('market', {}).get('price_data', [])
    if not price_data:
        return {'error': '缺少行情数据'}

    df = pd.DataFrame(price_data)
    df = calc_indicators(df)
    last = df.iloc[-1]
    close = float(last['close']) if pd.notna(last['close']) else None

    ma5 = float(last['ma5']) if pd.notna(last.get('ma5')) else None
    ma20 = float(last['ma20']) if pd.notna(last.get('ma20')) else None
    ma60 = float(last['ma60']) if pd.notna(last.get('ma60')) else None

    if close and ma20 and ma60:
        if close > ma20 > ma60:
            trend = '多头'
        elif close < ma20 < ma60:
            trend = '空头'
        else:
            trend = '震荡'
    elif close and ma5 and ma20:
        if close > ma5 > ma20:
            trend = '短多（数据不足60日，基于MA5/MA20判断）'
        elif close < ma5 < ma20:
            trend = '短空（数据不足60日，基于MA5/MA20判断）'
        else:
            trend = '震荡（数据不足60日，基于MA5/MA20判断）'
    else:
        trend = '数据不足'

    signal = []
    rsi = float(last['rsi14']) if pd.notna(last.get('rsi14')) else None
    if rsi:
        if rsi > 70:
            signal.append('RSI超买')
        elif rsi < 30:
            signal.append('RSI超卖')

    macd_hist = float(last['macd_hist']) if pd.notna(last.get('macd_hist')) else None
    if macd_hist is not None:
        if macd_hist > 0:
            signal.append('MACD多头')
        else:
            signal.append('MACD空头')

    k = float(last['kdj_k']) if pd.notna(last.get('kdj_k')) else None
    d = float(last['kdj_d']) if pd.notna(last.get('kdj_d')) else None
    j = float(last['kdj_j']) if pd.notna(last.get('kdj_j')) else None

    boll_up = float(last['boll_up']) if pd.notna(last.get('boll_up')) else None
    boll_mid = float(last['boll_mid']) if pd.notna(last.get('boll_mid')) else None
    boll_dn = float(last['boll_dn']) if pd.notna(last.get('boll_dn')) else None

    if close and boll_up and boll_dn:
        if close > boll_up:
            signal.append('突破布林上轨')
        elif close < boll_dn:
            signal.append('跌破布林下轨')
        else:
            signal.append('BOLL区间运行')

    rolling20 = df['close'].tail(20)
    support = float(rolling20.min()) if len(rolling20) else None
    resistance = float(rolling20.max()) if len(rolling20) else None

    atr = float(last['atr14']) if pd.notna(last['atr14']) else None
    stop_ref = (close - ATR_STOP_MULTIPLIER * atr) if atr else None

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
    p.add_argument('--verbose', action='store_true', help='显示详细日志')
    p.add_argument('--debug', action='store_true', help='显示调试日志')
    args = p.parse_args()
    setup_logging("DEBUG" if args.debug else ("INFO" if args.verbose else "WARNING"))

    if not args.code and not args.snapshot:
        raise SystemExit('请提供 --code 或 --snapshot')

    snap = load_snapshot(args.code, args.snapshot, data_type="core")
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
