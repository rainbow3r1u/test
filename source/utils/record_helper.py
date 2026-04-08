"""
记录6小时连续信号的辅助函数
"""
from datetime import datetime, timedelta
from utils.logger import get_logger

logger = get_logger('record_helper')


def record_six_hour_signal(symbol: str, start_time, end_time, hours: int, 
                          price: float, volume: float, gain: float):
    """
    记录连续6小时的信号
    
    Args:
        symbol: 币种
        start_time: 开始时间
        end_time: 结束时间  
        hours: 连续小时数
        price: 价格
        volume: 成交量
        gain: 涨幅
    """
    try:
        from utils.history_manager import HistoryManager
        
        if isinstance(start_time, str):
            start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        if isinstance(end_time, str):
            end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        
        if hours >= 6:
            HistoryManager.record_six_hour_signal(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                hours=hours,
                price=price,
                volume=volume,
                gain=gain
            )
    except Exception as e:
        logger.error(f"记录6小时信号失败: {e}")
