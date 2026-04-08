#!/usr/bin/env python3
"""
记录6小时连续信号
从最新的扫描结果中提取6小时信号并记录
"""
import sys
import json
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.history_manager import HistoryManager
from utils.logger import get_logger

logger = get_logger('record_six_hour')


def record_from_scan_results():
    """从扫描结果中记录6小时信号"""
    try:
        signal_file = '/var/www/all_signals.json'
        
        if not Path(signal_file).exists():
            logger.warning("信号文件不存在")
            return
        
        with open(signal_file, 'r', encoding='utf-8') as f:
            signals = json.load(f)
        
        if not signals:
            logger.info("没有信号数据")
            return
        
        recorded_count = 0
        for signal in signals:
            hours = signal.get('hrs', 0)
            
            if hours >= 6:
                symbol = signal.get('symbol', '')
                start_time_str = signal.get('startTime', '')
                end_time_str = signal.get('endTime', '')
                price = signal.get('price', 0)
                volume = signal.get('vol', 0) * 1e6
                gain = signal.get('gain', 0)
                
                try:
                    start_time = datetime.strptime(f"2026-{start_time_str}", '%Y-%m-%d %H:%M')
                    end_time = datetime.strptime(f"2026-{end_time_str}", '%Y-%m-%d %H:%M')
                    
                    recorded = HistoryManager.record_six_hour_signal(
                        symbol=symbol,
                        start_time=start_time,
                        end_time=end_time,
                        hours=hours,
                        price=price,
                        volume=volume,
                        gain=gain
                    )
                    
                    if recorded:
                        recorded_count += 1
                        
                except Exception as e:
                    logger.error(f"解析时间失败: {e}")
                    continue
        
        logger.info(f"记录了 {recorded_count} 个6小时信号")
        
    except Exception as e:
        logger.error(f"记录失败: {e}")


if __name__ == '__main__':
    record_from_scan_results()
