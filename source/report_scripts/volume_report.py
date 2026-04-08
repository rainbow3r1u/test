#!/usr/bin/env python3
"""
合约交易量日报（排除BTC/ETH）
每天下午3点自动生成
输出JSON到output目录
"""

import io
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config
from scripts.cos_client import read_cos_data

def main():
    # 使用北京时间
    beijing_now = datetime.utcnow() + timedelta(hours=8)
    
    print(f"生成时间: {beijing_now.strftime('%Y-%m-%d %H:%M')} (北京时间)")
    
    df = read_cos_data()
    
    # 排除BTC和ETH，按天汇总
    df_filtered = df[~df['symbol'].str.contains('BTC/USDT:USDT|ETH/USDT:USDT', regex=True)]
    daily = df_filtered.groupby(df_filtered['timestamp'].dt.date)['quote_volume'].sum()
    daily = daily.reset_index()
    daily.columns = ['date', 'total_volume']
    daily = daily[daily['date'] < beijing_now.date()]  # 排除今天
    daily = daily.sort_values('date')
    
    # 近19天数据
    recent = daily.tail(19).copy()
    recent['change_pct'] = recent['total_volume'].pct_change() * 100
    
    # 计算趋势
    total_start = recent.iloc[0]['total_volume']
    total_end = recent.iloc[-1]['total_volume']
    total_change = (total_end - total_start) / total_start * 100
    up_days = int((recent['change_pct'] > 0).sum())
    down_days = int((recent['change_pct'] < 0).sum())
    
    if total_change > 5:
        trend = "明显上涨趋势"
    elif total_change > 0:
        trend = "小幅上涨趋势"
    elif total_change > -5:
        trend = "小幅下跌趋势"
    else:
        trend = "明显下跌趋势"
    
    # 输出JSON
    items = []
    for _, row in recent.iterrows():
        vol = row['total_volume'] / 1e9
        change = row['change_pct']
        change_str = f"{change:+.2f}%" if pd.notna(change) else "-"
        items.append({
            'symbol': str(row['date']),
            'price': f"{vol:.2f}B",
            'volume': '-',
            'change': change if pd.notna(change) else 0,
            'indicator': change_str,
            'note': 'USDT'
        })
    
    json_output = {
        'strategy_name': 'volume_daily',
        'title': '合约交易量日报',
        'timestamp': beijing_now.strftime('%Y-%m-%d %H:%M:%S'),
        'conditions': ['排除BTC/ETH', '近19天数据', '每日成交量汇总'],
        'summary': {
            'start_date': str(recent.iloc[0]['date']),
            'end_date': str(recent.iloc[-1]['date']),
            'total_change_pct': round(total_change, 2),
            'up_days': up_days,
            'down_days': down_days,
            'trend': trend
        },
        'items': items
    }
    
    json_file = Path(config.OUTPUT_DIR) / f"volume_daily_{beijing_now.strftime('%Y%m%d_%H%M%S')}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(json_output, f, ensure_ascii=False, indent=2)
    
    print(f"JSON已保存: {json_file}")
    print(f"趋势: {trend} ({total_change:+.2f}%)")
    
    return json_file

if __name__ == "__main__":
    main()
