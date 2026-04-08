#!/usr/bin/env python3
"""
布林带收敛后反弹策略
- 布林带收敛（通道收窄到5%以内）
- 价格在下轨和中轨之间运行了连续5根K线
- 然后迅速贴近中轨
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

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config
from scripts.cos_client import read_cos_data

# 策略参数
BB_PERIOD = config.BB_PERIOD
BB_STD = config.BB_STD
BB_CONVERGE_THRESHOLD = config.BB_CONVERGE_THRESHOLD
BB_BETWEEN_K = config.BB_BETWEEN_K

def convert_to_4h(df):
    """转换为4小时K线"""
    df = df.sort_values(['symbol', 'timestamp'])
    df['hour'] = df['timestamp'].dt.floor('4h')
    agg = df.groupby(['symbol', 'hour']).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()
    agg.columns = ['symbol', 'open_time', 'open', 'high', 'low', 'close', 'volume']
    return agg

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
            time.sleep(0.1)
            
        except Exception as e:
            print(f"  [{i+1}/{len(symbols)}] {symbol} Error: {e}")

def check_converge_breakout(candles, min_between_k=BB_BETWEEN_K):
    """检查布林带收敛后反弹"""
    if candles is None or len(candles) < BB_PERIOD + min_between_k + 15:
        return None
    
    closes = candles['close'].values
    
    mid = pd.Series(closes).rolling(BB_PERIOD).mean()
    std = pd.Series(closes).rolling(BB_PERIOD).std()
    upper = mid + BB_STD * std
    lower = mid - BB_STD * std
    
    for i in range(len(candles) - BB_PERIOD - min_between_k - 15):
        start_idx = i
        end_idx = i + BB_PERIOD + 10 + min_between_k + 5
        
        if end_idx > len(candles):
            break
        
        section_closes = closes[start_idx:end_idx]
        section_mid = mid.iloc[start_idx:end_idx].values
        section_lower = lower.iloc[start_idx:end_idx].values
        section_upper = upper.iloc[start_idx:end_idx].values
        
        converge_start = BB_PERIOD
        converge_end = converge_start + 10
        
        mid_of_converge = section_mid[converge_start:converge_end]
        lower_of_converge = section_lower[converge_start:converge_end]
        upper_of_converge = section_upper[converge_start:converge_end]
        
        if any(np.isnan(mid_of_converge)) or len(mid_of_converge) < 10:
            continue
        
        last_converge_mid = mid_of_converge[-3:]
        last_converge_lower = lower_of_converge[-3:]
        last_converge_upper = upper_of_converge[-3:]
        
        if any(np.isnan(last_converge_mid)) or any(np.isnan(last_converge_lower)):
            continue
        
        converge_width = np.mean(last_converge_upper - last_converge_lower)
        converge_mid_val = np.mean(last_converge_mid)
        
        if converge_mid_val == 0:
            continue
        
        relative_width = converge_width / converge_mid_val
        
        # 收敛检查
        if relative_width > BB_CONVERGE_THRESHOLD:
            continue
        
        # 检查连续5根在下轨和中轨之间
        between_start = converge_end
        between_end = between_start + min_between_k
        
        if between_end > len(section_closes):
            continue
        
        between_closes = section_closes[between_start:between_end]
        between_lowers = section_lower[between_start:between_end]
        between_mids = section_mid[between_start:between_end]
        
        all_in_range = True
        for j in range(min_between_k):
            c = between_closes[j]
            l = between_lowers[j]
            m = between_mids[j]
            if np.isnan(l) or np.isnan(m) or not (l < c < m):
                all_in_range = False
                break
        
        if not all_in_range:
            continue
        
        # 检查贴近中轨
        final_start = between_end
        final_end = final_start + 3
        
        if final_end > len(section_closes):
            continue
        
        final_closes = section_closes[final_start:final_end]
        final_mids = section_mid[final_start:final_end]
        
        closeness_count = 0
        for j in range(len(final_closes)):
            c = final_closes[j]
            m = final_mids[j]
            if not np.isnan(m) and m > 0:
                ratio = c / m
                if 0.95 < ratio < 1.05:
                    closeness_count += 1
        
        if closeness_count >= 2:
            min_low = min(between_lowers[:min_between_k])
            strength = (converge_mid_val - min_low) / converge_mid_val * 100
            
            return {
                'converge_width_pct': round(relative_width * 100, 2),
                'strength': round(strength, 2),
                'last_price': round(closes[-1], 6),
                'price_change_4h': round((closes[-1] - closes[-2]) / closes[-2] * 100, 2) if len(closes) >= 2 else 0,
                'volume_24h': candles['volume'].tail(6).sum()
            }
    
    return None

def format_volume(v):
    """格式化成交量"""
    if v >= 1e9:
        return f"{v/1e9:.1f}B"
    elif v >= 1e6:
        return f"{v/1e6:.1f}M"
    elif v >= 1e3:
        return f"{v/1e3:.1f}K"
    return f"{v:.0f}"

def parse_volume(v):
    """解析成交量字符串为数字"""
    if isinstance(v, (int, float)):
        return v
    v = str(v)
    if 'B' in v:
        return float(v.replace('B','')) * 1e9
    elif 'M' in v:
        return float(v.replace('M','')) * 1e6
    elif 'K' in v:
        return float(v.replace('K','')) * 1e3
    return float(v)

def run():
    # 使用北京时间
    beijing_now = datetime.utcnow() + timedelta(hours=8)
    
    print("="*60)
    print("布林带收敛策略扫描")
    print("="*60)
    
    # 读取数据
    print("\n从COS读取数据...")
    try:
        df = read_cos_data()
        print(f"原始数据: {len(df)} 条")
    except Exception as e:
        print(f"COS读取失败: {e}")
        return
    
    # 排除BTC ETH
    df = df[~df['symbol'].str.contains(r'BTC/USDT:USDT|ETH/USDT:USDT', regex=True)]
    print(f"排除BTC/ETH后: {len(df)} 条")
    
    # 转换为4小时K线
    print("转换为4小时K线...")
    df_4h = convert_to_4h(df)
    print(f"4小时K线: {len(df_4h)} 条")
    
    # 扫描
    print("扫描策略...")
    symbols = df_4h['symbol'].unique()
    results = []
    
    for i, symbol in enumerate(symbols, 1):
        if i % 100 == 0:
            print(f"  进度: {i}/{len(symbols)}")
        
        symbol_data = df_4h[df_4h['symbol'] == symbol].sort_values('open_time')
        
        if len(symbol_data) < BB_PERIOD + 20:
            continue
        
        result = check_converge_breakout(symbol_data)
        if result:
            results.append({
                'symbol': symbol.replace('/USDT:USDT', ''),
                'price': result['last_price'],
                'volume': format_volume(result['volume_24h']),
                'change': result['price_change_4h'],
                'indicator': f"收敛{result['converge_width_pct']}% | 反弹{result['strength']}%",
                'note': ''
            })
    
    # 按成交量排序
    results.sort(key=lambda x: parse_volume(x['volume']), reverse=True)
    
    # 生成图表
    print("生成K线图表...")
    top_symbols = [r['symbol'] for r in results[:30]]  # 只生成TOP30
    generate_charts(top_symbols)
    
    # 保存结果JSON
    json_output = {
        'strategy_name': 'bollinger_converge',
        'title': '布林带收敛反弹',
        'timestamp': beijing_now.strftime('%Y-%m-%d %H:%M:%S'),
        'conditions': [
            '布林带收敛（宽度<5%）',
            '价格在下轨中轨间运行5根K以上',
            '贴近中轨'
        ],
        'items': results[:50]
    }
    
    json_file = Path(config.OUTPUT_DIR) / f"bollinger_converge_{beijing_now.strftime('%Y%m%d_%H%M%S')}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(json_output, f, ensure_ascii=False, indent=2)
    
    print(f"\nJSON已保存: {json_file}")
    print(f"找到 {len(results)} 个符合条件的币")
    
    return json_output

if __name__ == '__main__':
    run()
