#!/usr/bin/env python3
"""
暴涨监控脚本 - 检测1小时涨幅10%以上的币种
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

import ccxt
from utils.logger import get_logger
from utils.surge_manager import SurgeManager
from utils.chart_generator import ChartGenerator

logger = get_logger('surge_monitor')


def check_surge():
    """检查暴涨币种"""
    try:
        exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        
        logger.info("获取市场数据...")
        markets = exchange.load_markets()
        
        symbols = [
            m['symbol'] for m in markets.values()
            if m.get('swap') and m.get('quote') == 'USDT' and m.get('active')
        ]
        
        logger.info(f"检查 {len(symbols)} 个币种...")
        
        surge_count = 0
        
        for idx, symbol in enumerate(symbols, 1):
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=24)
                
                if len(ohlcv) < 2:
                    continue
                
                for i in range(len(ohlcv)):
                    candle = ohlcv[i]
                    candle_time = datetime.utcfromtimestamp(candle[0]/1000)
                    
                    now_utc = datetime.utcnow()
                    current_hour_start = now_utc.replace(minute=0, second=0, microsecond=0)
                    
                    if candle_time >= current_hour_start:
                        continue
                    
                    today_utc = now_utc.date()
                    if candle_time.date() != today_utc:
                        continue
                    
                    open_price = candle[1]
                    close_price = candle[4]
                    volume = candle[5]
                    
                    gain = (close_price - open_price) / open_price * 100
                    
                    if gain >= 10:
                        logger.info(f"发现暴涨: {symbol} +{gain:.2f}% at {candle_time}")
                        
                        try:
                            image_data = ChartGenerator.generate_triple_chart(symbol)
                        except Exception as e:
                            logger.error(f"生成图表失败: {e}")
                            image_data = None
                        
                        recorded = SurgeManager.record_surge(
                            symbol=symbol.replace('/USDT:USDT', 'USDT'),
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
