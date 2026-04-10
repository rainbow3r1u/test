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
        items = []
        utc_now = datetime.utcnow()
        
        if self.df is None:
            self.load_data()
            
        df_all = self.df.copy()
        
        # Sort by timestamp globally to ensure sequential order
        df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])
        df_all = df_all.sort_values('timestamp').reset_index(drop=True)
        
        all_symbols_bars = []
        
        import crypto_engine
        
        grouped = df_all.groupby('symbol')
        for symbol, df in grouped:
            if df.empty or len(df) < PARAMS['min_history']:
                continue
                
            open_arr = df['open'].values.astype(float).tolist()
            high_arr = df['high'].values.astype(float).tolist()
            low_arr = df['low'].values.astype(float).tolist()
            close_arr = df['close'].values.astype(float).tolist()
            vol_arr = df['volume'].values.astype(float).tolist()
            quote_vol_arr = df['quote_volume'].values.astype(float).tolist()
            timestamps = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').values.tolist()
            
            # Call Rust engine
            res = crypto_engine.scan_single_symbol(
                symbol,
                open_arr,
                high_arr,
                low_arr,
                close_arr,
                timestamps,
                quote_vol_arr,
                PARAMS['min_history'],
                PARAMS['lookback_hours'],
                PARAMS['right_bull_bars'],
                PARAMS['box_min_bars'],
                float(PARAMS['box_max_amp']),
                PARAMS['left_min_bars'],
                PARAMS['left_max_bulls'],
                float(PARAMS['min_drop_pct']),
                float(PARAMS['max_drop_pct'])
            )
            
            if res:
                items.append(res)
                
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

