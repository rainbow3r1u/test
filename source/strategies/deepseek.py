"""
DeepSeek策略 - AI分析推荐做多币种
基于19天日线数据分析
"""
import re
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from strategies.base import BaseStrategy
from configs import config


class DeepSeekStrategy(BaseStrategy):
    strategy_id = 'deepseek'
    strategy_name = 'DeepSeek 3.2分析'
    
    def aggregate_to_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['date'] = df['timestamp'].dt.date
        
        daily = df.groupby(['symbol', 'date']).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'quote_volume': 'sum'
        }).reset_index()
        
        daily['date'] = pd.to_datetime(daily['date'])
        return daily
    
    def get_daily_metrics(self, df: pd.DataFrame, days: int = 19) -> Optional[Dict[str, Any]]:
        daily = self.aggregate_to_daily(df)
        
        now_bj = datetime.utcnow() + timedelta(hours=8)
        cutoff = now_bj - timedelta(days=days)
        
        daily_recent = daily[daily['date'] >= cutoff].copy()
        
        if len(daily_recent) == 0:
            return None
        
        metrics = daily_recent.groupby('symbol').agg(
            start_price=('open', 'first'),
            end_price=('close', 'last'),
            high=('high', 'max'),
            low=('low', 'min'),
            total_volume=('quote_volume', 'sum'),
            avg_daily_volume=('quote_volume', 'mean'),
            days_count=('date', 'count')
        ).reset_index()
        
        metrics['change_pct'] = (metrics['end_price'] - metrics['start_price']) / metrics['start_price'] * 100
        metrics['volatility'] = (metrics['high'] - metrics['low']) / metrics['start_price'] * 100
        metrics['avg_daily_change'] = metrics['change_pct'] / metrics['days_count']
        
        total_volume = metrics['total_volume'].sum()
        
        top_gainers = metrics.nlargest(15, 'change_pct')[['symbol', 'change_pct', 'total_volume', 'end_price']].copy()
        top_gainers['total_volume'] = top_gainers['total_volume'] / 1e6
        
        top_volume = metrics.nlargest(15, 'total_volume')[['symbol', 'total_volume', 'change_pct', 'end_price']].copy()
        top_volume['total_volume'] = top_volume['total_volume'] / 1e6
        
        high_volatility = metrics.nlargest(15, 'volatility')[['symbol', 'volatility', 'change_pct', 'end_price']].copy()
        
        return {
            'total_volume': total_volume,
            'days': days,
            'top_gainers': top_gainers.to_dict('records'),
            'top_volume': top_volume.to_dict('records'),
            'high_volatility': high_volatility.to_dict('records'),
            'all_metrics': metrics
        }
    
    def find_technical_patterns(self, df: pd.DataFrame, days: int = 19) -> Dict[str, List[Dict]]:
        daily = self.aggregate_to_daily(df)
        
        now_bj = datetime.utcnow() + timedelta(hours=8)
        cutoff = now_bj - timedelta(days=days)
        daily_recent = daily[daily['date'] >= cutoff].copy()
        
        patterns = {
            'uptrend': [],
            'breakout': [],
            'accumulation': [],
            'oversold': []
        }
        
        for symbol, group in daily_recent.groupby('symbol'):
            if len(group) < 10:
                continue
            
            group = group.sort_values('date').reset_index(drop=True)
            
            closes = group['close'].values
            volumes = group['quote_volume'].values
            highs = group['high'].values
            lows = group['low'].values
            
            ma5 = np.convolve(closes, np.ones(5)/5, mode='valid')
            ma10 = np.convolve(closes, np.ones(10)/10, mode='valid')
            
            if len(ma5) >= 3 and len(ma10) >= 3:
                if ma5[-1] > ma10[-1] and ma5[-2] > ma10[-2] and ma5[-3] > ma10[-3]:
                    patterns['uptrend'].append({
                        'symbol': symbol,
                        'price': closes[-1],
                        'change': round((closes[-1] - closes[0]) / closes[0] * 100, 2),
                        'ma5': round(ma5[-1], 6),
                        'ma10': round(ma10[-1], 6)
                    })
            
            if len(group) >= 5:
                recent_5 = group.tail(5)
                prev_5 = group.tail(10).head(5)
                
                if len(prev_5) == 5:
                    recent_high = recent_5['high'].max()
                    prev_high = prev_5['high'].max()
                    recent_vol = recent_5['quote_volume'].mean()
                    prev_vol = prev_5['quote_volume'].mean()
                    
                    if recent_high > prev_high and recent_vol > prev_vol * 1.5:
                        patterns['breakout'].append({
                            'symbol': symbol,
                            'price': closes[-1],
                            'breakout_pct': round((recent_high - prev_high) / prev_high * 100, 2),
                            'vol_ratio': round(recent_vol / prev_vol, 2)
                        })
            
            if len(group) >= 10:
                recent_10 = group.tail(10)
                price_range = (recent_10['high'].max() - recent_10['low'].min()) / recent_10['low'].min() * 100
                avg_vol = recent_10['quote_volume'].mean()
                
                if price_range < 15 and avg_vol > 1e6:
                    patterns['accumulation'].append({
                        'symbol': symbol,
                        'price': closes[-1],
                        'range_pct': round(price_range, 2),
                        'avg_vol': round(avg_vol / 1e6, 2)
                    })
            
            if len(group) >= 5:
                recent_5 = group.tail(5)
                total_change = (recent_5['close'].iloc[-1] - recent_5['open'].iloc[0]) / recent_5['open'].iloc[0] * 100
                
                if total_change < -10:
                    rsi_approx = 100 - 100 / (1 + abs(total_change) / 10)
                    patterns['oversold'].append({
                        'symbol': symbol,
                        'price': closes[-1],
                        'drop_pct': round(total_change, 2),
                        'rsi_approx': round(rsi_approx, 1)
                    })
        
        for key in patterns:
            patterns[key].sort(key=lambda x: x.get('change', x.get('breakout_pct', x.get('drop_pct', 0))), 
                              reverse=(key != 'oversold'))
            patterns[key] = patterns[key][:10]
        
        return patterns
    
    def get_deepseek_recommendations(self, metrics: Dict[str, Any], patterns: Dict[str, List]) -> str:
        top_gainers_df = pd.DataFrame(metrics['top_gainers'])
        top_volume_df = pd.DataFrame(metrics['top_volume'])
        high_volatility_df = pd.DataFrame(metrics['high_volatility'])
        
        user_prompt = f"""
## 市场概况（过去{metrics['days']}天日线数据）

总成交额: {metrics['total_volume']/1e9:.2f}B USDT

### 涨幅Top15（日线级别）:
| 币种 | 涨幅% | 成交额(M) | 最新价格 |
|------|-------|-----------|----------|
"""
        for r in metrics['top_gainers'][:15]:
            user_prompt += f"| {r['symbol']} | {r['change_pct']:.2f}% | {r['total_volume']:.1f}M | {r['end_price']:.6f} |\n"
        
        user_prompt += f"""
### 成交额Top15:
| 币种 | 成交额(M) | 涨幅% | 最新价格 |
|------|-----------|-------|----------|
"""
        for r in metrics['top_volume'][:15]:
            user_prompt += f"| {r['symbol']} | {r['total_volume']:.1f}M | {r['change_pct']:.2f}% | {r['end_price']:.6f} |\n"
        
        user_prompt += f"""
### 技术形态分析:

#### 上升趋势（MA5>MA10连续3天）:
"""
        for p in patterns['uptrend'][:5]:
            user_prompt += f"- {p['symbol']}: 价格{p['price']:.6f}, {p['change']}%, MA5={p['ma5']:.6f}\n"
        
        user_prompt += f"""
#### 突破形态（放量突破前高）:
"""
        for p in patterns['breakout'][:5]:
            user_prompt += f"- {p['symbol']}: 突破{p['breakout_pct']}%, 量比{p['vol_ratio']}x\n"
        
        user_prompt += f"""
#### 横盘吸筹（10天波动<15%）:
"""
        for p in patterns['accumulation'][:5]:
            user_prompt += f"- {p['symbol']}: 波动{p['range_pct']}%, 日均{p['avg_vol']}M\n"
        
        user_prompt += f"""
#### 超跌反弹（5天跌>10%）:
"""
        for p in patterns['oversold'][:5]:
            user_prompt += f"- {p['symbol']}: 跌幅{p['drop_pct']}%, RSI约{p['rsi_approx']}\n"
        
        user_prompt += """
## 任务

你是一位专业的加密货币交易员，现在需要从以上数据中找出**最具做多潜力**的币种。

### 重点寻找以下特征：

1. **资金吸筹阶段**：
   - 横盘整理10天以上，波动率收窄
   - 成交量逐步放大，说明有大资金在悄悄建仓
   - 价格在低位或中位，尚未启动

2. **深度回调后企稳**：
   - 前期有过一波上涨（涨幅50%+）
   - 随后深度回调30%-50%
   - 最近3-5天出现止跌信号（放量阳线、下影线等）
   - 成交量萎缩后重新放大

3. **即将突破**：
   - 长期横盘后成交量突然放大
   - 价格突破关键阻力位
   - MA5刚刚上穿MA10

### 排除以下情况：

- 已经连续大涨5天以上的（追高风险太大）
- 涨幅已经超过100%且没有像样回调的
- 成交量极小、流动性差的

### 输出要求：

推荐5个币种，严格按照以下格式输出（每行一个币种）：

**币种名** | [阶段] | 技术特征(50字内) | 目标价位 | 入场建议(30字内)

示例：
**BTCUSDT** | [吸筹阶段] | 横盘10天波动8%量能放大 | 65000 | 回踩61000入场止损59000
"""
        
        headers = {
            'Authorization': f'Bearer {config.DEEPSEEK_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        data = {
            'model': config.DEEPSEEK_MODEL,
            'messages': [
                {"role": "system", "content": "你是专业的加密货币技术分析师。基于日线数据分析，推荐做多币种。每个币种用一句话说清理由和风险。"},
                {"role": "user", "content": user_prompt}
            ],
            'temperature': 0.7
        }
        
        response = requests.post(
            config.DEEPSEEK_BASE_URL,
            headers=headers,
            json=data,
            timeout=60
        )
        
        if response.status_code != 200:
            raise Exception(f"API调用失败: {response.status_code} - {response.text}")
        
        result = response.json()
        return result['choices'][0]['message']['content']
    
    def parse_recommendations(self, text: str) -> List[Dict[str, str]]:
        items = []
        lines = text.split('\n')
        for line in lines:
            if '**' in line and '|' in line:
                match = re.search(r'\*\*([A-Z0-9]+USDT?)\*\*', line)
                if match:
                    symbol = match.group(1)
                    if not symbol.endswith('USDT'):
                        symbol = symbol + 'USDT'
                    
                    parts = line.split('|')
                    if len(parts) >= 5:
                        stage = parts[1].strip() if len(parts) > 1 else '-'
                        stage = re.sub(r'[\[\]]', '', stage)
                        
                        tech_feature = parts[2].strip()[:80] if len(parts) > 2 else '-'
                        expected_space = parts[3].strip()[:50] if len(parts) > 3 else '-'
                        entry_advice = parts[4].strip()[:50] if len(parts) > 4 else '-'
                        
                        items.append({
                            'symbol': symbol,
                            'stage': stage,
                            'tech_feature': tech_feature,
                            'expected_space': expected_space,
                            'entry_advice': entry_advice
                        })
        return items[:5]
    
    def scan(self) -> List[Dict[str, Any]]:
        df = self.df.copy()
        
        days = self.params.get('history_days', 19)
        
        metrics = self.get_daily_metrics(df, days=days)
        if not metrics:
            self.logger.error("数据不足")
            return []
        
        patterns = self.find_technical_patterns(df, days=days)
        
        self.logger.info(f"技术形态: 上升趋势{len(patterns['uptrend'])}个, 突破{len(patterns['breakout'])}个, 吸筹{len(patterns['accumulation'])}个, 超跌{len(patterns['oversold'])}个")
        
        try:
            recommendations_text = self.get_deepseek_recommendations(metrics, patterns)
            self.logger.info("DeepSeek推荐结果:")
            self.logger.info(recommendations_text)
        except Exception as e:
            self.logger.error(f"DeepSeek调用失败: {e}")
            recommendations_text = "DeepSeek分析失败"
        
        items = self.parse_recommendations(recommendations_text)
        
        price_map = {}
        if 'all_metrics' in metrics:
            for _, row in metrics['all_metrics'].iterrows():
                price_map[row['symbol']] = row['end_price']
        
        return [{
            'symbol': item['symbol'],
            'price': price_map.get(item['symbol'], '-'),
            'volume': '-',
            'change': 0,
            'indicator': item.get('reason', '-')[:100],
            'note': item.get('risk', '-')[:100]
        } for item in items]
    
    def create_report(self, items: List[Dict[str, Any]], 
                      raw_analysis: str = None,
                      patterns: Dict = None,
                      **kwargs) -> 'StrategyReport':
        from models import StrategyReport
        beijing_now = datetime.utcnow() + timedelta(hours=8)
        
        formatted_items = []
        for item in items:
            formatted_items.append({
                'symbol': item.get('symbol', ''),
                'price': item.get('price', '-'),
                'volume': '-',
                'change': 0,
                'indicator': f"[{item.get('stage', '-')}] {item.get('tech_feature', '-')}",
                'note': f"目标:{item.get('expected_space', '-')}; 入场:{item.get('entry_advice', '-')}"
            })
        
        return StrategyReport(
            strategy_name=self.strategy_id,
            title=self.params.get('name', self.strategy_name),
            timestamp=beijing_now,
            conditions=[
                'DeepSeek AI分析',
                f'{self.params.get("history_days", 19)}天日线数据',
                '推荐做多币种',
                '技术形态分析'
            ],
            summary={
                'recommendations': len(items),
                'uptrend_count': len(patterns.get('uptrend', [])) if patterns else 0,
                'breakout_count': len(patterns.get('breakout', [])) if patterns else 0
            },
            items=formatted_items,
            raw_analysis=raw_analysis
        )
    
    def run(self, generate_charts: bool = True, save_to_db: bool = True) -> 'StrategyReport':
        self.logger.info(f"{'='*60}")
        self.logger.info(f"{self.strategy_name} 扫描开始")
        self.logger.info(f"{'='*60}")
        
        if self.df is None:
            self.load_data()
        
        df = self.df.copy()
        
        days = self.params.get('history_days', 19)
        metrics = self.get_daily_metrics(df, days=days)
        patterns = self.find_technical_patterns(df, days=days)
        
        self.logger.info(f"技术形态: 上升趋势{len(patterns['uptrend'])}个, 突破{len(patterns['breakout'])}个")
        
        try:
            recommendations_text = self.get_deepseek_recommendations(metrics, patterns)
        except Exception as e:
            self.logger.error(f"DeepSeek调用失败: {e}")
            recommendations_text = "DeepSeek分析失败"
        
        items = self.parse_recommendations(recommendations_text)
        
        price_map = {}
        if 'all_metrics' in metrics:
            for _, row in metrics['all_metrics'].iterrows():
                price_map[row['symbol']] = row['end_price']
        
        for item in items:
            item['price'] = price_map.get(item['symbol'], '-')
        
        if generate_charts and items:
            symbols = [item['symbol'] for item in items]
            self.generate_charts(symbols)
        
        report = self.create_report(
            items, 
            raw_analysis=recommendations_text,
            patterns=patterns
        )
        
        self.save_report(report, save_to_db=save_to_db)
        
        self.logger.info(f"推荐 {len(items)} 个币种")
        return report


def run():
    strategy = DeepSeekStrategy()
    return strategy.run(generate_charts=True, save_to_db=True)


if __name__ == '__main__':
    run()
