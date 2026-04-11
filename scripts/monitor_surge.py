#!/usr/bin/env python3
"""
暴涨监控脚本 - 检测1小时涨幅10%以上的币种
使用 COS 数据，"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import get_logger
from utils.surge_manager import SurgeManager
from core.chart_generator import ChartGenerator
from core.data_loader import DataLoader

logger = get_logger('surge_monitor')


def check_surge():
    """检查暴涨币种 - 使用 COS 数据"""
    try:
        logger.info("从 COS 读取 K 线数据...")
        df = DataLoader.get_klines(use_cache=False)
        
        if df is None or len(df) == 0:
            logger.error("无法获取 K 线数据")
            return
        
        logger.info(f"读取 {len(df)} 条 K 线记录")
        
        now_utc = datetime.utcnow()
        current_hour_start = now_utc.replace(minute=0, second=0, microsecond=0)
        today_utc = now_utc.date()
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        
        df_today = df[df['timestamp'].dt.date == today_utc]
        
        symbols = df_today['symbol'].unique()
        logger.info(f"检查 {len(symbols)} 个币种...")
        
        surge_count = 0
        
        for idx, symbol in enumerate(symbols, 1):
            try:
                symbol_df = df_today[df_today['symbol'] == symbol].sort_values('timestamp')
                
                if len(symbol_df) < 2:
                    continue
                
                for i in range(len(symbol_df)):
                    row = symbol_df.iloc[i]
                    candle_time = row['timestamp']
                    
                    if candle_time >= current_hour_start:
                        continue
                    
                    open_price = row['open']
                    close_price = row['close']
                    volume = row['volume']
                    
                    if open_price <= 0:
                        continue
                    
                    gain = (close_price - open_price) / open_price * 100
                    
                    if gain >= 10:
                        logger.info(f"发现暴涨: {symbol} +{gain:.2f}% at {candle_time}")
                        
                        try:
                            image_data = ChartGenerator.generate_triple_chart_from_cos(symbol, cutoff=candle_time)
                        except Exception as e:
                            logger.error(f"生成图表失败: {e}")
                            image_data = None
                        
                        recorded = SurgeManager.record_surge(
                            symbol=symbol.replace('/USDT:USDT', 'USDT').replace('USDT', 'USDT'),
                            gain=gain,
                            price=close_price,
                            volume=volume,
                            image_data=image_data,
                            surge_time=candle_time
                        )
                        
                        if recorded:
                            surge_count += 1
                
                if idx % 50 == 0:
                    logger.info(f"进度: {idx}/{len(symbols)}")
                    
            except Exception as e:
                logger.error(f"检查 {symbol} 失败: {e}")
                continue
        
        logger.info(f"检查完成，发现 {surge_count} 个暴涨币种")
        
    except Exception as e:
        logger.error(f"监控失败: {e}")


if __name__ == '__main__':
    logger.info("开始暴涨监控...")
    check_surge()
