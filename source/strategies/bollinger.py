"""
布林带收敛反弹策略
布林带收敛(宽度<5%) + 价格在下轨中轨间运行5K+
"""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from strategies.base import BaseStrategy, format_volume, parse_volume


class BollingerStrategy(BaseStrategy):
    strategy_id = 'bollinger'
    strategy_name = '布林收敛通道'
    
    def convert_to_4h(self, df: pd.DataFrame) -> pd.DataFrame:
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
    
    def check_converge_breakout(self, candles: pd.DataFrame) -> Optional[Dict[str, Any]]:
        period = self.params.get('period', 20)
        std_mult = self.params.get('std', 2.0)
        converge_threshold = self.params.get('converge_threshold', 0.05)
        min_between_k = self.params.get('min_between_k', 5)
        
        if candles is None or len(candles) < period + min_between_k + 15:
            return None
        
        closes = candles['close'].values
        
        mid = pd.Series(closes).rolling(period).mean()
        std = pd.Series(closes).rolling(period).std()
        upper = mid + std_mult * std
        lower = mid - std_mult * std
        
        for i in range(len(candles) - period - min_between_k - 15):
            start_idx = i
            end_idx = i + period + 10 + min_between_k + 5
            
            if end_idx > len(candles):
                break
            
            section_closes = closes[start_idx:end_idx]
            section_mid = mid.iloc[start_idx:end_idx].values
            section_lower = lower.iloc[start_idx:end_idx].values
            section_upper = upper.iloc[start_idx:end_idx].values
            
            converge_start = period
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
            
            if relative_width > converge_threshold:
                continue
            
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
    
    def scan(self) -> List[Dict[str, Any]]:
        df = self.df.copy()
        
        df = df[~df['symbol'].str.contains(r'BTC/USDT:USDT|ETH/USDT:USDT', regex=True)]
        self.logger.info(f"排除BTC/ETH后: {len(df)} 条")
        
        self.logger.info("转换为4小时K线...")
        df_4h = self.convert_to_4h(df)
        self.logger.info(f"4小时K线: {len(df_4h)} 条")
        
        period = self.params.get('period', 20)
        symbols = df_4h['symbol'].unique()
        results = []
        
        for i, symbol in enumerate(symbols, 1):
            if i % 100 == 0:
                self.logger.info(f"进度: {i}/{len(symbols)}")
            
            symbol_data = df_4h[df_4h['symbol'] == symbol].sort_values('open_time')
            
            if len(symbol_data) < period + 20:
                continue
            
            result = self.check_converge_breakout(symbol_data)
            if result:
                results.append({
                    'symbol': symbol.replace('/USDT:USDT', ''),
                    'price': result['last_price'],
                    'volume': format_volume(result['volume_24h']),
                    'change': result['price_change_4h'],
                    'indicator': f"收敛{result['converge_width_pct']}% | 反弹{result['strength']}%",
                    'note': ''
                })
        
        results.sort(key=lambda x: parse_volume(x['volume']), reverse=True)
        return results
    
    def create_report(self, items: List[Dict[str, Any]], **kwargs) -> 'StrategyReport':
        from models import StrategyReport
        beijing_now = datetime.utcnow() + timedelta(hours=8)
        
        return StrategyReport(
            strategy_name=self.strategy_id,
            title=self.params.get('name', self.strategy_name),
            timestamp=beijing_now,
            conditions=[
                f"布林带收敛（宽度<{self.params.get('converge_threshold', 0.05)*100}%）",
                f"价格在下轨中轨间运行{self.params.get('min_between_k', 5)}根K以上",
                "贴近中轨"
            ],
            summary={'total_found': len(items)},
            items=items[:50]
        )


def run():
    strategy = BollingerStrategy()
    return strategy.run(generate_charts=True, save_to_db=True)


if __name__ == '__main__':
    run()
