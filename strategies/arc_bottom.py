import pandas as pd
from datetime import datetime, timezone
from .base import BaseStrategy

class ArcBottomStrategy(BaseStrategy):
    def __init__(self, **kwargs):
        super().__init__()
        self.default_params = {
            'min_drop_pct': 0.01,
            'max_drop_pct': 0.10,
            'left_min_bars': 3,
            'left_max_bulls': 1,
            'box_max_amp': 0.05,
            'box_min_bars': 3,
            'right_bull_bars': 2,
            'lookback_hours': 120,
            'min_history': 50
        }
        for k, v in kwargs.items():
            if k in self.default_params:
                self.default_params[k] = v
        self.params.update(self.default_params)

    @property
    def strategy_id(self) -> str:
        return 'arc_bottom'

    @property
    def strategy_name(self):
        return '圆弧底突破'

    def scan(self) -> dict:
        PARAMS = self.params
        # =========================================================================
        
        items = []
        utc_now = datetime.utcnow()
        
        if self.df is None:
            self.load_data()
            
        df_all = self.df.copy()
        symbols = df_all['symbol'].unique()
        
        all_symbols_bars = []
        
        for symbol in symbols:
            df = df_all[df_all['symbol'] == symbol].copy()
            if df.empty or len(df) < PARAMS['min_history']:
                continue
                
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # 记录用于 debug 的 bars
            recent_debug = df.iloc[-PARAMS['lookback_hours']:].reset_index(drop=True)
            bars_raw = []
            for _, row in recent_debug.iterrows():
                bars_raw.append({
                    'timestamp': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                })
            all_symbols_bars.append({
                'symbol': symbol,
                'bars': bars_raw
            })
            
            # 取最近的时间窗口
            recent = df.iloc[-PARAMS['lookback_hours']:].reset_index(drop=True)
            n = len(recent)
            if n < PARAMS['left_min_bars'] + PARAMS['box_min_bars'] + PARAMS['right_bull_bars']: 
                continue
            
            # 检查右侧反弹信号 (最新 N 根 K 线是否全部为阳线)
            right_bars = recent.iloc[-PARAMS['right_bull_bars']:]
            is_breakout = all(right_bars['close'] > right_bars['open'])
            if not is_breakout:
                continue
            
            found = False
            # 倒推寻找底部箱体
            for box_len in range(PARAMS['box_min_bars'], 24):
                box_start_idx = n - PARAMS['right_bull_bars'] - box_len
                box_end_idx = n - PARAMS['right_bull_bars'] - 1
                if box_start_idx < 0: break
                
                box_segment = recent.iloc[box_start_idx : box_end_idx + 1]
                
                # 计算箱体振幅 (最高价和最低价的极差)
                box_high = box_segment['high'].max()
                box_low = box_segment['low'].min()
                if box_low <= 0: continue
                box_amp = (box_high - box_low) / box_low
                
                if box_amp > PARAMS['box_max_amp']:
                    continue # 振幅超过限制，继续尝试其他长度的箱体
                    
                # 箱体合格，往左寻找下跌段
                for left_len in range(PARAMS['left_min_bars'], 24):
                    left_start_idx = box_start_idx - left_len
                    left_end_idx = box_start_idx - 1
                    if left_start_idx < 0: break
                    
                    left_segment = recent.iloc[left_start_idx : left_end_idx + 1]
                    
                    # 检查左侧阳线反抽数量
                    bullish_count = sum(left_segment['close'] > left_segment['open'])
                    if bullish_count > PARAMS['left_max_bulls']:
                        continue
                        
                    # 计算左侧实体跌幅
                    start_body_high = max(left_segment.iloc[0]['open'], left_segment.iloc[0]['close'])
                    end_body_low = min(left_segment.iloc[-1]['open'], left_segment.iloc[-1]['close'])
                    if start_body_high <= 0: continue
                    
                    drop_pct = (start_body_high - end_body_low) / start_body_high
                    
                    # 判断跌幅是否在允许区间内
                    if PARAMS['min_drop_pct'] <= drop_pct <= PARAMS['max_drop_pct']:
                        c1 = recent.iloc[-1]
                        
                        # 构建前端展示的详情步骤
                        details = [
                            {'step': '右侧突破', 'time': c1['timestamp'].strftime('%m-%d %H:%M'), 'pass': True, 'reason': f"最新 {PARAMS['right_bull_bars']} 小时连阳突破"},
                            {'step': '底部盘整', 'time': f"{box_segment.iloc[0]['timestamp'].strftime('%H:%M')}~{box_segment.iloc[-1]['timestamp'].strftime('%H:%M')}", 'pass': True, 'reason': f"盘整 {box_len} 小时, 振幅 {box_amp*100:.2f}% (≤{PARAMS['box_max_amp']*100:.0f}%)"},
                            {'step': '左侧下跌', 'time': f"{left_segment.iloc[0]['timestamp'].strftime('%H:%M')}~{left_segment.iloc[-1]['timestamp'].strftime('%H:%M')}", 'pass': True, 'reason': f"下跌 {left_len} 小时, 跌幅 {drop_pct*100:.2f}%, 包含 {bullish_count} 根反抽阳线"}
                        ]
                        
                        items.append({
                            'symbol': symbol,
                            'price': float(c1['close']),
                            'vol': round(c1['quote_volume']/1e6, 2),
                            'endHour': c1['timestamp'].hour,
                            'time': c1['timestamp'].strftime('%m-%d %H:%M'),
                            'drop_pct': round(drop_pct * 100, 2),
                            'box_amp': round(box_amp * 100, 2),
                            'left_len': left_len,
                            'box_len': box_len,
                            'details': details,
                            'is_watchlist': False # 圆弧底暂不需要专门的异动观察窗样式
                        })
                        found = True
                        break # 找到一种合适的左侧长度即可
                if found: break # 找到一种合适的箱体长度即可
        
        # 按照跌幅大小降序排列，优先展示跌得最狠且成功圆弧底的币
        items.sort(key=lambda x: x.get('drop_pct', 0), reverse=True)
        
        return {
            'items': items,
            'all_symbols_bars': all_symbols_bars
        }

    def create_report(self, items: list, all_symbols_bars: list = None, **kwargs):
        from models.signal import StrategyReport
        utc_now = datetime.utcnow()
        PARAMS = self.params
        return StrategyReport(
            strategy_name=self.strategy_id,
            title=self.strategy_name,
            timestamp=utc_now,
            conditions=[
                f"左侧下跌: 跌幅{PARAMS['min_drop_pct']*100:.0f}%~{PARAMS['max_drop_pct']*100:.0f}% (容许{PARAMS['left_max_bulls']}根反抽)",
                f"底部箱体: 振幅≤{PARAMS['box_max_amp']*100:.0f}%, 耗时≥{PARAMS['box_min_bars']}h",
                f"右侧突破: 最新{PARAMS['right_bull_bars']}小时连续阳线"
            ],
            items=items,
            summary={
                'check_time': utc_now.strftime('%Y-%m-%d %H:%M:%S UTC'),
                'total_found': len(items),
                'params': PARAMS
            },
            metadata={
                'all_symbols_bars': all_symbols_bars or []
            }
        )

def run():
    strategy = ArcBottomStrategy()
    report = strategy.run(generate_charts=False, save_to_db=False)
    
    # 预生成图表
    if report and report.items:
        from core.chart_generator import ChartGenerator
        symbols = [item.get('symbol', '') for item in report.items if item.get('symbol')]
        if symbols:
            print(f"\n预生成 {len(symbols)} 个币种的三合一图表缓存...")
            success = ChartGenerator.generate_triple_charts_batch(symbols)
            print(f"成功生成 {success} 个图表缓存")
            
    return report
