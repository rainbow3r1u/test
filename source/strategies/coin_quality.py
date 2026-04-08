"""
币种质量评分策略
两连阳 + 质量评分>=40
"""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from strategies.base import BaseStrategy, format_volume
from configs import BIG_COINS, SCORE_WEIGHTS


class CoinQualityStrategy(BaseStrategy):
    strategy_id = 'coin_quality'
    strategy_name = '币种质量评分'
    
    def calc_score(self, df_coin: pd.DataFrame) -> Optional[Dict[str, Any]]:
        history_days = self.params.get('history_days', 19)
        klines_per_day = self.params.get('klines_per_day', 24)
        required_klines = history_days * klines_per_day
        
        if len(df_coin) < required_klines:
            return None
        
        group = df_coin.sort_values('timestamp').tail(required_klines).copy()
        
        turnover = group['quote_volume'].sum()
        liq_weights = SCORE_WEIGHTS['liquidity']['tiers']
        liq = 0
        for threshold, score in liq_weights:
            if turnover >= threshold:
                liq = score
                break
        
        int_ = 15
        if (datetime.now() - group['timestamp'].max()).total_seconds() / 3600 > 2:
            int_ -= SCORE_WEIGHTS['integrity']['data_age_penalty']
        if len(group) < required_klines * SCORE_WEIGHTS['integrity']['completeness_threshold']:
            int_ -= 10
        elif len(group) < required_klines * 0.8:
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
            atr = np.mean(trs[-required_klines:]) if len(trs) >= required_klines else np.mean(trs)
            price = group.iloc[-1]['close']
            daily_vol = (atr * klines_per_day / price) * 100 if price > 0 else 0
            
            ideal = SCORE_WEIGHTS['volatility']['ideal_range']
            acceptable = SCORE_WEIGHTS['volatility']['acceptable_range']
            
            if ideal[0] <= daily_vol <= ideal[1]:
                vol = SCORE_WEIGHTS['volatility']['max_score']
            elif any(a[0] <= daily_vol <= a[1] for a in acceptable):
                vol = 15
            elif daily_vol < 0.5:
                vol = 5
            else:
                vol = 10
        
        ma5 = np.mean(group['close'].tail(120)) if len(group) >= 120 else np.mean(group['close'].tail(5))
        ma10 = np.mean(group['close'].tail(240)) if len(group) >= 240 else np.mean(group['close'].tail(10))
        ma19 = np.mean(group['close'])
        price = group.iloc[-1]['close']
        
        ma_trend = SCORE_WEIGHTS['trend']['ma_trend_score'] if (ma5 > ma10 > ma19 and price > ma5) or (ma5 < ma10 < ma19 and price < ma5) else 0
        
        if len(group) >= 20:
            x = np.arange(len(group))
            slope = np.polyfit(x, group['close'].values, 1)[0]
            slope_pct = (slope / price) * 100 * klines_per_day
            slope_score = SCORE_WEIGHTS['trend']['slope_score'] if abs(slope_pct) >= 0.2 else 5 if abs(slope_pct) >= 0.1 else 0
        else:
            slope_score = 0
        trend = ma_trend + slope_score
        
        recent_3d = group.tail(72)
        prev_16d = group.tail(required_klines).head(384)
        vol_3d = recent_3d['quote_volume'].mean()
        vol_16d = prev_16d['quote_volume'].mean() if len(prev_16d) > 0 else 0
        
        high_threshold = SCORE_WEIGHTS['heat']['high_threshold']
        medium_threshold = SCORE_WEIGHTS['heat']['medium_threshold']
        heat = SCORE_WEIGHTS['heat']['max_score'] if vol_16d > 0 and vol_3d / vol_16d >= high_threshold else 5 if vol_16d > 0 and vol_3d / vol_16d >= medium_threshold else 0
        
        total = liq + int_ + vol + trend + heat
        return {
            'liq': liq, 'int': int_, 'vol': vol, 'trend': trend, 'heat': heat,
            'total': total, 'turnover': turnover / 1e6
        }
    
    def scan(self) -> List[Dict[str, Any]]:
        df = self.df.copy()
        
        df['date'] = df['timestamp'].dt.date
        daily = df.groupby(['symbol', 'date']).agg({
            'open': 'first',
            'close': 'last',
            'quote_volume': 'sum'
        }).reset_index()
        
        max_gain = self.params.get('max_single_day_gain', 5.0)
        min_score = self.params.get('min_score', 40)
        
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
            
            if g1 > max_gain or g2 > max_gain:
                continue
            
            two_green.append({
                'symbol': name,
                'price': d2['close'],
                'day1': round(g1, 2),
                'day2': round(g2, 2),
                'qv': d2['quote_volume'] / 1e6
            })
        
        self.logger.info(f"两连阳（排除大币）: {len(two_green)} 个")
        
        results = []
        for r in two_green:
            full_sym = r['symbol']
            scores = self.calc_score(df[df['symbol'] == full_sym])
            if scores:
                results.append({
                    'symbol': r['symbol'],
                    'price': r['price'],
                    'day1': r['day1'],
                    'day2': r['day2'],
                    'qv': round(r['qv'], 2),
                    **scores
                })
        
        self.logger.info(f"有评分的: {len(results)} 个")
        
        filtered = [r for r in results if r['total'] >= min_score]
        self.logger.info(f"评分>={min_score}: {len(filtered)} 个")
        
        filtered.sort(key=lambda x: x['qv'], reverse=True)
        return filtered
    
    def create_report(self, items: List[Dict[str, Any]], **kwargs) -> 'StrategyReport':
        from models import StrategyReport
        beijing_now = datetime.utcnow() + timedelta(hours=8)
        
        top10 = items[:10]
        
        return StrategyReport(
            strategy_name=self.strategy_id,
            title=self.params.get('name', self.strategy_name),
            timestamp=beijing_now,
            conditions=[
                '两连阳',
                f"质量评分≥{self.params.get('min_score', 40)}",
                '按成交额排序'
            ],
            summary={
                'two_green_count': len(items),
                'display_count': len(top10)
            },
            items=[{
                'symbol': r['symbol'],
                'price': r['price'],
                'volume': f"{r['qv']:.1f}M",
                'change': r['day1'],
                'indicator': f"总分{r['total']}(流{r['liq']}完{r['int']}波{r['vol']}趋{r['trend']}热{r['heat']})",
                'note': f"两连阳 +{r['day1']}% / +{r['day2']}%"
            } for r in top10]
        )


def run():
    strategy = CoinQualityStrategy()
    return strategy.run(generate_charts=True, save_to_db=True)


if __name__ == '__main__':
    run()
