import os
import sys
import pandas as pd
import numpy as np

# 添加项目根目录到环境变量
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.data_loader import DataLoader

def run_arc_scan():
    print("="*50)
    print("开始扫描圆弧底策略 (Standalone Test)")
    print("免责声明：本脚本为独立运行，绝不修改任何网站代码或配置！")
    print("="*50)
    
    # 使用正确的 DataLoader API 获取数据
    try:
        raw_df = DataLoader.get_klines()
        if raw_df is None or len(raw_df) == 0:
            print("未能加载本地数据。")
            return
            
        # 将扁平的 DataFrame 转换为字典格式 {symbol: df}
        data = {}
        for symbol, group in raw_df.groupby('symbol'):
            data[symbol] = group.sort_values('timestamp').reset_index(drop=True)
            
    except Exception as e:
        print(f"数据加载失败: {e}")
        return
        
    matched_symbols = []
    
    for symbol, df in data.items():
        if len(df) < 200: # 需要足够的K线计算200均线
            continue
            
        # 仅使用开盘价和收盘价
        df = df.copy()
        df['body'] = abs(df['close'] - df['open']) / df['open']
        df['vol_ma20'] = df['quote_volume'].rolling(20).mean()
        df['ma200'] = df['close'].rolling(200).mean() # 使用200小时均线替代200日均线进行快速测试
        
        # 取最近的数据段进行形态匹配 (假设形态发生在最近 40 个小时内)
        latest = df.iloc[-40:]
        if len(latest) < 40: continue
        
        current_close = df.iloc[-1]['close']
        current_ma200 = df.iloc[-1]['ma200']
        
        # 七、过滤条件
        if pd.isna(current_ma200) or current_close <= current_ma200:
            continue # 趋势过滤：价格需 > 200均线
            
        # 注：VIX < 30 属于宏观数据，此处单币种扫描暂不引入，默认通过
            
        # 三、右侧反弹确认 (最近4根K线)
        rebound_candles = df.iloc[-4:]
        
        # 1. 连续4根阳线
        if not all(rebound_candles['close'] > rebound_candles['open']):
            continue
            
        # 2. 收盘价逐根抬高
        closes = rebound_candles['close'].values
        if not (closes[1] > closes[0] and closes[2] > closes[1] and closes[3] > closes[2]):
            continue
            
        # 3. 单根涨幅 0.3% ~ 2%
        gains = (rebound_candles['close'] - rebound_candles['open']) / rebound_candles['open']
        if not all((gains >= 0.003) & (gains <= 0.02)):
            continue
            
        # 4. 累计反弹 >= 2%
        total_rebound = (closes[-1] - rebound_candles.iloc[0]['open']) / rebound_candles.iloc[0]['open']
        if total_rebound < 0.02:
            continue
            
        # 二、底部止跌确认 (反弹前的2-3根K线)
        # 取反弹前的3根作为盘整区
        bottom_candles = df.iloc[-7:-4] 
        
        # 1. 连续2-3根K线实体幅度 <= 0.003
        if not all(bottom_candles['body'] <= 0.003):
            continue
            
        # 2. 成交量萎缩：VOL_curr < 0.5 * MA20(VOL)
        vol_curr = bottom_candles.iloc[-1]['quote_volume']
        vol_ma20 = bottom_candles.iloc[-1]['vol_ma20']
        if vol_curr >= 0.5 * vol_ma20:
            continue
            
        # 一、左侧圆弧形下跌
        left_arc = df.iloc[-40:-7]
        
        h1_idx = left_arc['close'].idxmax()
        h1_price = left_arc.loc[h1_idx, 'close']
        ln_price = bottom_candles['close'].min()
        
        # 半弦长 d > 10根K线
        d = df.index.get_loc(bottom_candles.index[-1]) - df.index.get_loc(h1_idx)
        if d <= 10:
            continue
            
        # 累计跌幅 >= 8%
        if (h1_price - ln_price) / h1_price < 0.08:
            continue
            
        # 实体由大转小 (左侧最大实体 > 最小实体)
        if left_arc['body'].max() <= left_arc['body'].min():
            continue
            
        matched_symbols.append(symbol)
        print(f"[+] 发现匹配圆弧底币种: {symbol} (当前价格: {current_close:.4f})")
        
    print("\n" + "="*50)
    print(f"扫描结束。共发现 {len(matched_symbols)} 个符合圆弧底形态的币种。")
    if not matched_symbols:
        print("⚠️ 提示：圆弧底策略条件极其严苛（需同时满足缩量、极小实体、连阳、严格涨幅等10多项指标），当前市场暂时没有完全符合的币种。")
    print("="*50)

if __name__ == "__main__":
    run_arc_scan()
