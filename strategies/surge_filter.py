"""
策略: 1小时涨幅筛选
筛选条件: 1h涨幅3-10% + 24h成交额<2000万USDT
"""
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from strategies.base import BaseStrategy, format_volume
from core.data_loader import DataLoader


class SurgeFilterStrategy(BaseStrategy):
    strategy_id = 'surge_filter'
    strategy_name = '1h涨幅3-10%'
    
    def __init__(self, min_gain: float = 0.03, max_gain: float = 0.10, max_volume: float = 20_000_000):
        super().__init__()
        self.min_gain = min_gain
        self.max_gain = max_gain
        self.max_volume = max_volume
    
    def scan(self) -> List[Dict[str, Any]]:
        df = self.load_data(use_cache=True)
        df = df.sort_values(['symbol', 'timestamp'])
        
        results = []
        for sym in df['symbol'].unique():
            grp = df[df['symbol'] == sym].tail(25)
            if len(grp) < 2:
                continue
            
            old_price = grp.iloc[-2]['close']
            new_price = grp.iloc[-1]['close']
            gain = (new_price - old_price) / old_price
            
            vol_24h = grp.tail(24)['quote_volume'].sum()
            
            if self.min_gain <= gain <= self.max_gain and vol_24h < self.max_volume:
                results.append({
                    'symbol': sym,
                    'price': round(new_price, 6),
                    'gain_1h': round(gain * 100, 2),
                    'vol_24h': round(vol_24h, 0),
                    'time': grp.iloc[-1]['timestamp'].strftime('%H:%M')
                })
        
        results.sort(key=lambda x: -x['gain_1h'])
        return results
    
    def run(self, generate_charts: bool = False):
        items = self.scan()
        report = {
            'strategy_id': self.strategy_id,
            'strategy_name': self.strategy_name,
            'title': '1小时涨幅3-10%筛选',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'items': items,
            'summary': {
                'total': len(items),
                'params': {
                    'min_gain': f'{self.min_gain*100}%',
                    'max_gain': f'{self.max_gain*100}%',
                    'max_volume': f'{self.max_volume/1000000}M USDT'
                }
            }
        }
        self.save_report(report, save_to_db=False)
        return report
