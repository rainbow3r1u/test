"""
合约交易量日报策略
排除BTC/ETH，近19天成交量趋势
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from strategies.base import BaseStrategy


class VolumeStrategy(BaseStrategy):
    strategy_id = 'volume'
    strategy_name = '合约趋势'
    
    def scan(self) -> List[Dict[str, Any]]:
        df = self.df.copy()
        
        exclude = self.params.get('exclude_symbols', ['BTC', 'ETH'])
        exclude_symbols = [f"{s}USDT" for s in exclude]
        df_filtered = df[~df['symbol'].isin(exclude_symbols)]
        
        daily = df_filtered.groupby(df_filtered['timestamp'].dt.date)['quote_volume'].sum()
        daily = daily.reset_index()
        daily.columns = ['date', 'total_volume']
        
        beijing_now = datetime.utcnow() + timedelta(hours=8)
        daily = daily[daily['date'] < beijing_now.date()]
        daily = daily.sort_values('date')
        
        history_days = self.params.get('history_days', 19)
        recent = daily.tail(history_days).copy()
        recent['change_pct'] = recent['total_volume'].pct_change() * 100
        
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
        
        return items
    
    def create_report(self, items: List[Dict[str, Any]], **kwargs) -> 'StrategyReport':
        from core.database import Database; from models.signal import StrategyReport
        beijing_now = datetime.utcnow() + timedelta(hours=8)
        
        if items:
            total_start = items[0]['change'] if items else 0
            total_end = items[-1]['change'] if items else 0
            
            up_days = sum(1 for item in items if item['change'] > 0)
            down_days = sum(1 for item in items if item['change'] < 0)
            
            total_change = total_end - total_start
            if total_change > 5:
                trend = "明显上涨趋势"
            elif total_change > 0:
                trend = "小幅上涨趋势"
            elif total_change > -5:
                trend = "小幅下跌趋势"
            else:
                trend = "明显下跌趋势"
        else:
            trend = "无数据"
            up_days = down_days = total_change = 0
        
        return StrategyReport(
            strategy_name=self.strategy_id,
            title=self.params.get('name', self.strategy_name),
            timestamp=beijing_now,
            conditions=['排除BTC/ETH', f"近{self.params.get('history_days', 19)}天数据", '每日成交量汇总'],
            summary={
                'start_date': items[0]['symbol'] if items else '-',
                'end_date': items[-1]['symbol'] if items else '-',
                'total_change_pct': round(total_change, 2),
                'up_days': up_days,
                'down_days': down_days,
                'trend': trend
            },
            items=items
        )


def run():
    strategy = VolumeStrategy()
    return strategy.run(generate_charts=False, save_to_db=True)


if __name__ == '__main__':
    run()
