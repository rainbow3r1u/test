import os
import sys
import pandas as pd
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.data_loader import DataLoader

def run_relaxed_arc_scan():
    print("="*60)
    print("开始扫描圆弧底策略 V2 (放宽容错版)")
    print("条件:")
    print("1. 左侧下跌: 跌幅1%-10%(只看实体)，最少3根起，允许最多1根阳线反抽")
    print("2. 底部箱体: 最大振幅<=5%，最少3根起，直到出现2小时连涨")
    print("3. 右侧反弹: 最新2根K线为连续阳线 (作为提醒信号)")
    print("="*60)
    
    try:
        raw_df = DataLoader.get_klines()
        if raw_df is None or len(raw_df) == 0:
            print("未能加载本地数据。")
            return
            
        data = {}
        for symbol, group in raw_df.groupby('symbol'):
            data[symbol] = group.sort_values('timestamp').reset_index(drop=True)
            
    except Exception as e:
        print(f"数据加载失败: {e}")
        return
        
    matched_symbols = []
    
    for symbol, df in data.items():
        if len(df) < 50:
            continue
            
        # 取最近 48 小时数据进行形态寻找
        recent = df.iloc[-48:].reset_index(drop=True)
        n = len(recent)
        if n < 8: continue
        
        # 三、右侧反弹信号：最新2根K线为连阳
        c1 = recent.iloc[-1]
        c2 = recent.iloc[-2]
        
        is_c1_bullish = c1['close'] > c1['open']
        is_c2_bullish = c2['close'] > c2['open']
        
        if not (is_c1_bullish and is_c2_bullish):
            continue # 不满足2连阳突破，跳过
            
        # 寻找合适的底部箱体和左侧下跌
        # 倒推寻找箱体 (结束于倒数第3根K线，即 n-3)
        found = False
        
        # 遍历可能的箱体长度 (最少3根，最多假设24根)
        for box_len in range(3, 24):
            box_start_idx = n - 2 - box_len
            box_end_idx = n - 3
            if box_start_idx < 0: break
            
            box_segment = recent.iloc[box_start_idx : box_end_idx + 1]
            
            # 箱体最大振幅 <= 5% (用最高价和最低价计算振幅，这是最严谨的箱体画法)
            box_high = box_segment['high'].max()
            box_low = box_segment['low'].min()
            box_amp = (box_high - box_low) / box_low
            
            if box_amp > 0.05:
                continue # 这个长度的箱体振幅超过5%，不合格，试下一个长度
                
            # 如果箱体合格，往左寻找下跌段
            # 遍历可能的左侧下跌长度 (最少3根，最多假设24根)
            for left_len in range(3, 24):
                left_start_idx = box_start_idx - left_len
                left_end_idx = box_start_idx - 1
                if left_start_idx < 0: break
                
                left_segment = recent.iloc[left_start_idx : left_end_idx + 1]
                
                # 允许最多1根阳线
                bullish_count = sum(left_segment['close'] > left_segment['open'])
                if bullish_count > 1:
                    continue
                    
                # 计算累计跌幅 (只看实体)
                # 起点实体最高价
                start_body_high = max(left_segment.iloc[0]['open'], left_segment.iloc[0]['close'])
                # 终点实体最低价
                end_body_low = min(left_segment.iloc[-1]['open'], left_segment.iloc[-1]['close'])
                
                # 防御除零
                if start_body_high <= 0: continue
                
                drop_pct = (start_body_high - end_body_low) / start_body_high
                
                if 0.01 <= drop_pct <= 0.10:
                    # 完美匹配所有放宽后的条件！
                    matched_symbols.append({
                        'symbol': symbol,
                        'price': c1['close'],
                        'drop_pct': drop_pct * 100,
                        'left_len': left_len,
                        'box_amp': box_amp * 100,
                        'box_len': box_len
                    })
                    found = True
                    break # 找到一种左侧长度匹配即可
                    
            if found:
                break # 找到一种箱体长度匹配即可
                
    print(f"\n扫描结束。共发现 {len(matched_symbols)} 个符合【容错版圆弧底】的币种！")
    if matched_symbols:
        print("\n详细名单：")
        print(f"{'币种':<12} | {'当前价格':<10} | {'左侧跌幅(实体)':<15} | {'左侧耗时':<10} | {'箱体振幅':<12} | {'箱体耗时':<10}")
        print("-" * 80)
        # 按跌幅降序排序
        matched_symbols.sort(key=lambda x: x['drop_pct'], reverse=True)
        for m in matched_symbols:
            print(f"{m['symbol']:<12} | {m['price']:<10.4f} | {m['drop_pct']:>5.2f}%         | {m['left_len']:>2} 小时    | {m['box_amp']:>5.2f}%      | {m['box_len']:>2} 小时")
    print("="*60)

if __name__ == "__main__":
    run_relaxed_arc_scan()
