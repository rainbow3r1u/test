"""
策略1: 稳步抬升
反向检查最近0-6小时，最低价逐步抬高
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_loader import DataLoader
from strategies.base import BaseStrategy, format_volume

# Rust 引擎导入
try:
    import crypto_engine
    _USE_RUST = True
except ImportError:
    _USE_RUST = False


class Strategy1Pro(BaseStrategy):
    """
    稳步抬升策略 (PRO 版)
    继承基础策略类，增加：
    1. 实体比例过滤（防长上影线骗炮）
    2. 单根K线涨幅上限（防加速赶顶接盘）
    3. 放量验证（防无量空涨）
    """
    
    @property
    def strategy_id(self) -> str:
        return 'strategy1_pro'
    
    @property
    def strategy_name(self) -> str:
        return '稳步抬升 PRO版'
    
    def _aggregate_to_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['date'] = df['timestamp'].dt.date
        
        daily_df = df.groupby(['symbol', 'date']).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'quote_volume': 'sum'
        }).reset_index()
        
        daily_df['timestamp'] = pd.to_datetime(daily_df['date'])
        return daily_df
    
    def _pre_filter_symbols(self, df: pd.DataFrame, symbols: list) -> tuple:
        min_volume_24h = self.params.get('min_volume_24h', 15_000_000)
        
        filter_stats = {
            'total': len(symbols),
            'daily_bullish_pass': 0,
            'volume_pass': 0,
            'both_pass': 0
        }
        
        vol_24h = df.groupby('symbol')['quote_volume'].sum()
        
        # 第一步: 用COS数据初步筛选(成交额+日线阳线)
        daily_df = self._aggregate_to_daily(df)
        now_utc = pd.Timestamp.utcnow()
        today = now_utc.date()
        yesterday = (now_utc - pd.Timedelta(days=1)).date()
        
        cos_candidates = []
        for symbol in symbols:
            symbol_vol = vol_24h.get(symbol, 0)
            if symbol_vol < min_volume_24h:
                continue
            
            symbol_daily = daily_df[daily_df['symbol'] == symbol].sort_values('timestamp')
            today_data = symbol_daily[symbol_daily['date'] == today]
            yesterday_data = symbol_daily[symbol_daily['date'] == yesterday]
            
            if len(today_data) > 0 and len(yesterday_data) > 0:
                cos_today_bullish = today_data.iloc[0]['close'] > today_data.iloc[0]['open']
                cos_yesterday_bullish = yesterday_data.iloc[0]['close'] > yesterday_data.iloc[0]['open']
                if cos_today_bullish and cos_yesterday_bullish:
                    cos_candidates.append(symbol)
        
        self.logger.info(f"COS初筛: 成交额+日线阳线候选 {len(cos_candidates)} 个")
        
        self.logger.info(f"日线预筛选: COS候选 {len(cos_candidates)} 个全部通过")
        
        return cos_candidates, filter_stats
    
    def scan(self) -> Dict[str, Any]:
        df = self.df.copy()
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        df = df.sort_values(['symbol', 'timestamp'])
        
        now_utc = pd.Timestamp.now('UTC').tz_localize(None)
        current_hour = now_utc.replace(minute=0, second=0, microsecond=0) - pd.Timedelta(hours=1)
        
        self.logger.info(f"当前UTC时间: {now_utc.strftime('%Y-%m-%d %H:%M')}")
        self.logger.info(f"检查时间窗口: 最近6根小时K线 (截止{current_hour.strftime('%Y-%m-%d %H:%M')})")
        
        top_symbols = DataLoader.get_top_symbols(self.params.get('top_n', 100), use_cache=False)
        
        passed_symbols, pre_filter_stats = self._pre_filter_symbols(df, top_symbols)
        
        self.logger.info(f"开始扫描 {len(passed_symbols)} 个通过预筛选的币种...")
        
        min_range = self.params.get('min_range', 0.005)
        max_range = self.params.get('max_range', 0.025)
        min_hours = self.params.get('min_hours', 3)
        min_body_ratio = self.params.get('min_body_ratio', 0.4) # 实体比例 > 40%
        max_single_gain = self.params.get('max_single_gain', 0.08) # 单根涨幅 < 8%
        
        # 检查统计
        check_stats = {
            'total': len(passed_symbols),
            'pre_filter': pre_filter_stats,
            'step1': 0,
            'step2': 0,
            'step3': 0,
            'step4': 0,
            'step5': 0,
            'step6': 0
        }
        
        step_symbols = {
            'step1': [],
            'step2': [],
            'step3': [],
            'step4': [],
            'step5': [],
            'step6': []
        }
        
        all_symbols_bars = []
        
        results = []
        
        # 准备 Rust 调用参数
        cutoff_ts_ms = int(current_hour.timestamp() * 1000)
        
        for idx, symbol in enumerate(passed_symbols, 1):
            group = df[df['symbol'] == symbol].copy()
            if len(group) < 10:
                continue
            
            group = group.sort_values('timestamp').reset_index(drop=True)
            
            # 准备数组数据
            open_arr = group['open'].tolist()
            high_arr = group['high'].tolist()
            low_arr = group['low'].tolist()
            close_arr = group['close'].tolist()
            ts_arr = (group['timestamp'].astype('int64') // 1_000_000).tolist()
            vol_arr = group['quote_volume'].tolist()
            
            # 调用 Rust 引擎
            if _USE_RUST:
                res = crypto_engine.scan_strategy1_pro_symbol(
                    symbol, open_arr, high_arr, low_arr, close_arr,
                    ts_arr, vol_arr, cutoff_ts_ms, min_hours,
                    min_body_ratio, max_single_gain
                )
            else:
                res = self._scan_single_symbol_python(
                    symbol, group, current_hour, min_hours, min_body_ratio, max_single_gain,
                    check_stats, step_symbols
                )
            
            if res:
                results.append(res)
                # 更新 step 统计
                consecutive_count = res.get('hrs', 0)
                for step in range(1, consecutive_count + 1):
                    step_key = f'step{step}'
                    check_stats[step_key] += 1
            
            if idx % 20 == 0:
                self.logger.info(f"进度: {idx}/{len(passed_symbols)}")
        
        results.sort(key=lambda x: (-x['endHour'], -x['vol']))
        
        return {
            'items': results,
            'check_stats': check_stats,
            'step_symbols': step_symbols,
            'all_symbols_bars': all_symbols_bars,
            'scan_cutoff_hour': current_hour.strftime('%Y-%m-%d %H:%M:%S'),
            'check_time': (current_hour + pd.Timedelta(hours=1, minutes=2)).strftime('%Y-%m-%d %H:%M'),
            'next_check_time': (now_utc.replace(minute=0, second=0, microsecond=0) + pd.Timedelta(hours=1, minutes=2)).strftime('%Y-%m-%d %H:%M')
        }
    
    def create_report(self, items: List[Dict[str, Any]], 
                      check_stats: Dict = None,
                      scan_cutoff_hour: str = None,
                      check_time: str = None,
                      next_check_time: str = None,
                      step_symbols: Dict = None,
                      all_symbols_bars: List = None,
                      **kwargs) -> 'StrategyReport':
        from core.database import Database; from models.signal import StrategyReport
        utc_now = datetime.utcnow()
        
        return StrategyReport(
            strategy_name=self.strategy_id,
            title=self.params.get('name', self.strategy_name),
            timestamp=utc_now,
            conditions=[
                f"日线:今天+昨天均为阳线",
                f"成交额>={self.params.get('min_volume_24h', 15_000_000)/1e6:.0f}M USDT",
                f"连续{self.params.get('min_hours', 3)}小时+",
                "最低价逐步抬高",
                "最新一根为阳线",
                f"过滤长上影线(实体>{int(self.params.get('min_body_ratio', 0.4)*100)}%)",
                f"过滤加速赶顶(单根涨幅<{int(self.params.get('max_single_gain', 0.08)*100)}%)"
            ],
            summary={
                'total_signals': len(items),
                'check_stats': check_stats or {},
                'scan_cutoff_hour': scan_cutoff_hour or '',
                'check_time': check_time or '',
                'next_check_time': next_check_time or '',
                'step_symbols': step_symbols or {},
                'all_symbols_bars': all_symbols_bars or []
            },
            items=items
        )
    
    def _scan_single_symbol_python(self, symbol, group, current_hour, min_hours, min_body_ratio, max_single_gain, check_stats, step_symbols):
        """Python fallback 实现"""
        recent_6h = group[group['timestamp'] <= current_hour].tail(6).copy()
        
        if len(recent_6h) < 3:
            return None
        
        recent_6h = recent_6h.iloc[::-1].reset_index(drop=True)
        
        consecutive_count = 0
        bars_info = []
        
        for i, row in recent_6h.iterrows():
            open_price = row['open']
            high_price = row['high']
            low_price = row['low']
            close_price = row['close']
            range_pct = (high_price - low_price) / low_price
            is_bullish = close_price > open_price
            
            body_ratio = abs(close_price - open_price) / (high_price - low_price + 1e-8)
            single_gain = (close_price - open_price) / open_price
            
            if not is_bullish:
                break
                
            if body_ratio < min_body_ratio:
                break
            
            if consecutive_count == 0:
                current_low = low_price
                consecutive_count = 1
            else:
                if low_price >= current_low:
                    break
                current_low = low_price
                consecutive_count += 1
            
            bar_info = {
                't': row['timestamp'].strftime('%m-%d %H:%M'),
                'o': f"{open_price:.6f}",
                'high': f"{high_price:.6f}",
                'low': f"{low_price:.6f}",
                'c': f"{close_price:.6f}",
                'r': f"{range_pct*100:.2f}%",
                'v': f"{row['quote_volume']/1e6:.2f}M",
                'type': '阳线'
            }
            bars_info.insert(0, bar_info)
            
            if single_gain > max_single_gain:
                obs_bar = {
                    't': row['timestamp'].strftime('%m-%d %H:%M'),
                    'o': f"{open_price:.6f}",
                    'high': f"{high_price:.6f}",
                    'low': f"{low_price:.6f}",
                    'c': f"{close_price:.6f}",
                    'r': f"{range_pct*100:.2f}%",
                    'v': f"{row['quote_volume']/1e6:.2f}M",
                    'type': '爆拉大阳线'
                }
                return {
                    'symbol': symbol,
                    'price': float(close_price),
                    'time': obs_bar['t'],
                    'startTime': bars_info[0]['t'] if bars_info else obs_bar['t'],
                    'endTime': obs_bar['t'],
                    'endHour': row['timestamp'].hour,
                    'hrs': consecutive_count,
                    'vol': round(row['quote_volume']/1e6, 2),
                    'gain': round(single_gain*100, 2),
                    'bars': bars_info[:-1] + [obs_bar] if bars_info else [obs_bar],
                    'is_watchlist': True,
                    'watch_reason': f"单根涨幅 {single_gain*100:.2f}% > {max_single_gain*100:.2f}%"
                }
            
            step_key = f'step{consecutive_count}'
            check_stats[step_key] += 1
            step_symbols[step_key].append({
                'symbol': symbol,
                'price': close_price,
                'bar': bar_info,
                'bars': bars_info.copy()
            })
        
        if consecutive_count >= min_hours:
            first_bar = bars_info[0]
            last_bar = bars_info[-1]
            
            total_gain = (float(last_bar['c']) - float(first_bar['o'])) / float(first_bar['o']) * 100
            last_vol = float(last_bar['v'].replace('M', '')) * 1e6
            
            now_year = datetime.now().year
            start_time_str = first_bar['t'].replace('-',' ')
            end_time_str = last_bar['t'].replace('-',' ')
            start_time = datetime.strptime(f'{now_year} {start_time_str}', '%Y %m %d %H:%M')
            end_time = datetime.strptime(f'{now_year} {end_time_str}', '%Y %m %d %H:%M')
            
            return {
                'symbol': symbol,
                'price': float(last_bar['c']),
                'time': f"{start_time.strftime('%H:%M')} ~ {end_time.strftime('%H:%M')}",
                'startTime': start_time.strftime('%m-%d %H:%M'),
                'endTime': end_time.strftime('%m-%d %H:%M'),
                'endHour': end_time.hour,
                'hrs': consecutive_count,
                'vol': round(last_vol/1e6, 2),
                'gain': round(total_gain, 2),
                'bars': bars_info
            }
        
        return None
    
    def run(self, generate_charts: bool = False, save_to_db: bool = False) -> 'StrategyReport':
        self.logger.info(f"{'='*60}")
        self.logger.info(f"{self.strategy_name} 扫描开始")
        self.logger.info(f"{'='*60}")
        
        if self.df is None:
            self.load_data()
        
        scan_result = self.scan()
        items = scan_result['items']
        
        if generate_charts and items:
            symbols = [item.get('symbol', '') for item in items if item.get('symbol')]
            self.generate_charts(symbols)
        
        report = self.create_report(
            items,
            check_stats=scan_result.get('check_stats'),
            scan_cutoff_hour=scan_result.get('scan_cutoff_hour'),
            check_time=scan_result.get('check_time'),
            next_check_time=scan_result.get('next_check_time'),
            step_symbols=scan_result.get('step_symbols'),
            all_symbols_bars=scan_result.get('all_symbols_bars')
        )
        
        self.save_report(report, save_to_db=save_to_db)
        
        self.logger.info(f"找到 {len(items)} 个符合条件的币")
        return report

def run():
    from core.chart_generator import ChartGenerator
    strategy = Strategy1Pro()
    report = strategy.run(generate_charts=False, save_to_db=False)
    
    if report and report.items:
        symbols = [item.get('symbol', '') for item in report.items if item.get('symbol')]
        if symbols:
            print(f"\n预生成 {len(symbols)} 个币种的三合一图表缓存...")
            success = ChartGenerator.generate_triple_charts_batch(symbols)
            print(f"成功生成 {success} 个图表缓存")
    
    return report

if __name__ == '__main__':
    run()
