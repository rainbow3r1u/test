#!/usr/bin/env python3
"""
币种质量评分 + 两连阳筛选
流程：两连阳 -> 质量评分>=40 -> 按成交额排序 -> TOP10
输出JSON到output目录
"""

import io
import json
import os
import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import ccxt

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config
from scripts.cos_client import read_cos_data

# 图表缓存目录
CHART_CACHE_DIR = os.path.join(os.path.dirname(__file__), '..', 'static', 'charts')

BIG_COINS = [
    'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'DOGEUSDT', 'XRPUSDT', 'ADAUSDT', 'AVAXUSDT', 'DOTUSDT', 'LINKUSDT',
    'LTCUSDT', 'UNIUSDT', 'TRXUSDT', 'MATICUSDT', 'SHIBUSDT', 'APEUSDT', 'LUNCUSDT', 'FLOKIUSDT', 'BONKUSDT',
    'XAUUSDT', 'XAGUSDT', 'TAOUSDT', 'STOUSDT', 'RIVERUSDT', 'WLDUSDT', 'TRUMPUSDT', 'PIPPINUSDT', 'SUIUSDT',
    'XMRUSDT', 'ZECUSDT', 'BCHUSDT', 'HYPEUSDT', 'MSTRUSDT', '1000BONKUSDT', 'CRCLUSDT', 'BEATUSDT', 'CFGUSDT'
]

def calc_score(df_coin):
    if len(df_coin) < 19 * 24:
        return None
    group = df_coin.sort_values('timestamp').tail(456).copy()
    
    turnover = group['quote_volume'].sum()
    liq = 30 if turnover >= 10_000_000_000 else 25 if turnover >= 5_000_000_000 else 20 if turnover >= 1_000_000_000 else 15 if turnover >= 500_000_000 else 10 if turnover >= 100_000_000 else 0
    
    int_ = 15
    if (datetime.now() - group['timestamp'].max()).total_seconds() / 3600 > 2:
        int_ -= 5
    if len(group) < 456 * 0.9:
        int_ -= 10
    elif len(group) < 456 * 0.8:
        int_ -= 5
    closes = group['close'].tail(3).values
    if len(closes) == 3 and closes[0] == closes[1] == closes[2]:
        int_ -= 5
    int_ = max(0, int_)
    
    if len(group) < 20:
        vol = 0
    else:
        trs = []
        for i in range(1, len(group)):
            tr = max(
                group.iloc[i]['high'] - group.iloc[i]['low'],
                abs(group.iloc[i]['high'] - group.iloc[i-1]['close']),
                abs(group.iloc[i-1]['close'] - group.iloc[i]['low'])
            )
            trs.append(tr)
        atr = np.mean(trs[-456:]) if len(trs) >= 456 else np.mean(trs)
        price = group.iloc[-1]['close']
        daily_vol = (atr * 24 / price) * 100 if price > 0 else 0
        vol = 25 if 1 <= daily_vol <= 5 else 15 if (0.5 <= daily_vol < 1 or 5 < daily_vol <= 8) else 5 if daily_vol < 0.5 else 10
    
    ma5 = np.mean(group['close'].tail(120)) if len(group) >= 120 else np.mean(group['close'].tail(5))
    ma10 = np.mean(group['close'].tail(240)) if len(group) >= 240 else np.mean(group['close'].tail(10))
    ma19 = np.mean(group['close'])
    price = group.iloc[-1]['close']
    ma_trend = 10 if (ma5 > ma10 > ma19 and price > ma5) or (ma5 < ma10 < ma19 and price < ma5) else 0
    if len(group) >= 20:
        x = np.arange(len(group))
        slope = np.polyfit(x, group['close'].values, 1)[0]
        slope_pct = (slope / price) * 100 * 24
        slope_score = 10 if abs(slope_pct) >= 0.2 else 5 if abs(slope_pct) >= 0.1 else 0
    else:
        slope_score = 0
    trend = ma_trend + slope_score
    
    recent_3d = group.tail(72)
    prev_16d = group.tail(456).head(384)
    vol_3d = recent_3d['quote_volume'].mean()
    vol_16d = prev_16d['quote_volume'].mean() if len(prev_16d) > 0 else 0
    heat = 10 if vol_16d > 0 and vol_3d / vol_16d >= 2.0 else 5 if vol_16d > 0 and vol_3d / vol_16d >= 1.5 else 0
    
    total = liq + int_ + vol + trend + heat
    return {
        'liq': liq, 'int': int_, 'vol': vol, 'trend': trend, 'heat': heat,
        'total': total, 'turnover': turnover / 1e6
    }

def generate_charts(symbols):
    """生成K线图表并保存到缓存目录"""
    os.makedirs(CHART_CACHE_DIR, exist_ok=True)
    
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })
    
    for i, symbol in enumerate(symbols):
        full_symbol = f"{symbol}/USDT:USDT"
        cache_file = os.path.join(CHART_CACHE_DIR, f"{symbol}_USDT:USDT.png")
        
        # 跳过已有且1小时内的缓存
        if os.path.exists(cache_file) and os.path.getmtime(cache_file) > time.time() - 3600:
            continue
        
        try:
            ohlcv_1h = exchange.fetch_ohlcv(full_symbol, timeframe='1h', limit=24)
            ohlcv_4h = exchange.fetch_ohlcv(full_symbol, timeframe='4h', limit=40)
            
            if not ohlcv_1h:
                continue
            
            df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # 聚合日K
            df_4h_sorted = df_4h.sort_values('timestamp').reset_index(drop=True)
            daily_data = []
            for j in range(0, len(df_4h_sorted) - 3, 4):
                group = df_4h_sorted.iloc[j:j+4]
                daily_data.append({
                    'open': group.iloc[0]['open'],
                    'high': group['high'].max(),
                    'low': group['low'].min(),
                    'close': group.iloc[-1]['close'],
                    'volume': group['volume'].sum()
                })
            df_daily = pd.DataFrame(daily_data).tail(10)
            df_4h_6 = df_4h.tail(6)
            
            # 画图
            plt.style.use('dark_background')
            fig = plt.figure(figsize=(18, 16))
            gs = fig.add_gridspec(3, 1, height_ratios=[1.2, 1, 0.8], hspace=0.25)
            ax1 = fig.add_subplot(gs[0])
            ax2 = fig.add_subplot(gs[1])
            ax3 = fig.add_subplot(gs[2])
            fig.patch.set_facecolor('#1a1a1a')
            
            # 1H K线
            for j in range(len(df_1h)):
                o = df_1h.iloc[j]['open']
                h = df_1h.iloc[j]['high']
                l = df_1h.iloc[j]['low']
                c = df_1h.iloc[j]['close']
                color = '#00a854' if c >= o else '#eb3c3c'
                ax1.plot([j, j], [l, h], color=color, linewidth=1)
                width = 0.6
                if c >= o:
                    ax1.bar([j], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
                else:
                    ax1.bar([j], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
            ax1.set_facecolor('#1a1a1a')
            ax1.grid(True, alpha=0.2, color='#333')
            ax1.set_title(f'{symbol} - 1H (24 candles)', fontsize=12, fontweight='bold', color='#fff', pad=10)
            ax1.tick_params(colors='#999', labelsize=8)
            ax1.set_xlim(-0.5, len(df_1h)-0.5)
            
            # 日K
            for j in range(len(df_daily)):
                o = df_daily.iloc[j]['open']
                h = df_daily.iloc[j]['high']
                l = df_daily.iloc[j]['low']
                c = df_daily.iloc[j]['close']
                color = '#00a854' if c >= o else '#eb3c3c'
                ax2.plot([j, j], [l, h], color=color, linewidth=1.5)
                width = 0.5
                if c >= o:
                    ax2.bar([j], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
                else:
                    ax2.bar([j], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
            ax2.set_facecolor('#1a1a1a')
            ax2.grid(True, alpha=0.2, color='#333')
            ax2.set_title('Daily (from 4H, 10 candles ~10 days)', fontsize=11, color='#fff', pad=8)
            ax2.tick_params(colors='#999', labelsize=8)
            ax2.set_xlim(-0.5, len(df_daily)-0.5)
            
            # 4H
            for j in range(len(df_4h_6)):
                o = df_4h_6.iloc[j]['open']
                h = df_4h_6.iloc[j]['high']
                l = df_4h_6.iloc[j]['low']
                c = df_4h_6.iloc[j]['close']
                color = '#00a854' if c >= o else '#eb3c3c'
                ax3.plot([j, j], [l, h], color=color, linewidth=2)
                width = 0.5
                if c >= o:
                    ax3.bar([j], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
                else:
                    ax3.bar([j], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
            ax3.set_facecolor('#1a1a1a')
            ax3.grid(True, alpha=0.2, color='#333')
            ax3.set_title('4H (6 candles ~1 day)', fontsize=11, color='#fff', pad=8)
            ax3.tick_params(colors='#999', labelsize=8)
            ax3.set_xlim(-0.5, len(df_4h_6)-0.5)
            
            plt.tight_layout()
            plt.savefig(cache_file, format='png', dpi=130, facecolor='#1a1a1a')
            plt.close()
            
            print(f"  [{i+1}/{len(symbols)}] {symbol} OK")
            time.sleep(0.1)  # 避免频率限制
            
        except Exception as e:
            print(f"  [{i+1}/{len(symbols)}] {symbol} Error: {e}")

def main():
    # 使用北京时间
    beijing_now = datetime.utcnow() + timedelta(hours=8)
    
    print(f"生成时间: {beijing_now.strftime('%Y-%m-%d %H:%M')} (北京时间)")
    
    df = read_cos_data()
    print(f"数据: {df['symbol'].nunique()} 个币种")
    
    # 步骤1: 两连阳筛选
    df['date'] = df['timestamp'].dt.date
    daily = df.groupby(['symbol', 'date']).agg({
        'open': 'first',
        'close': 'last',
        'quote_volume': 'sum'
    }).reset_index()
    
    two_green = []
    for symbol, group in daily.groupby('symbol'):
        name = symbol.replace('/USDT:USDT', '')
        if name in BIG_COINS or len(group) < 2:
            continue
        group = group.sort_values('date').tail(2).copy()
        d1, d2 = group.iloc[0], group.iloc[1]
        if d1['close'] <= d1['open'] or d2['close'] <= d2['open']:
            continue
        g1 = (d1['close'] - d1['open']) / d1['open'] * 100
        g2 = (d2['close'] - d2['open']) / d2['open'] * 100
        if g1 > 5 or g2 > 5:
            continue
        two_green.append({
            'symbol': name,
            'day1': round(g1, 2),
            'day2': round(g2, 2),
            'qv': d2['quote_volume'] / 1e6
        })
    
    print(f"步骤1: 两连阳（排除大币）: {len(two_green)} 个")
    
    # 步骤2: 计算质量评分
    results = []
    for r in two_green:
        full_sym = r['symbol']  # df['symbol'] 已经是 BTCUSDT 格式
        scores = calc_score(df[df['symbol'] == full_sym])
        if scores:
            results.append({
                'symbol': r['symbol'],
                'day1': r['day1'],
                'day2': r['day2'],
                'qv': round(r['qv'], 2),
                **scores
            })
    
    print(f"步骤2: 有评分的: {len(results)} 个")
    
    # 步骤3: 质量评分>=40过滤
    filtered = [r for r in results if r['total'] >= 40]
    print(f"步骤3: 评分>=40: {len(filtered)} 个")
    
    # 步骤4: 按成交额排序
    filtered.sort(key=lambda x: x['qv'], reverse=True)
    top10 = filtered[:10]
    
    print(f"步骤4: TOP10 (按成交额排序)")
    
    # 生成图表
    print("生成K线图表...")
    generate_charts([r['symbol'] for r in top10])
    print("图表生成完成")
    
    # 输出JSON到output目录
    json_output = {
        'strategy_name': 'coin_quality',
        'title': '币种质量评分',
        'timestamp': beijing_now.strftime('%Y-%m-%d %H:%M:%S'),
        'conditions': [
            '两连阳',
            '质量评分≥40',
            '按成交额排序'
        ],
        'summary': {
            'two_green_count': len(two_green),
            'passed_score_count': len(filtered),
            'display_count': len(top10)
        },
        'items': [{
            'symbol': r['symbol'],
            'price': '-',
            'volume': f"{r['qv']:.1f}M",
            'change': r['day1'],
            'indicator': f"总分{r['total']}(流{r['liq']}完{r['int']}波{r['vol']}趋{r['trend']}热{r['heat']})",
            'note': f"两连阳 +{r['day1']}% / +{r['day2']}%"
        } for r in top10]
    }
    
    json_file = Path(config.OUTPUT_DIR) / f"coin_quality_{beijing_now.strftime('%Y%m%d_%H%M%S')}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(json_output, f, ensure_ascii=False, indent=2)
    
    print(f"JSON已保存: {json_file}")
    print(f"找到 {len(filtered)} 个符合条件的币")
    
    return json_file

if __name__ == "__main__":
    main()
