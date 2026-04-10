"""
历史记录管理器 - 记录连续6小时的币种
"""
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config
from utils.logger import get_logger

logger = get_logger('history_manager')


class HistoryManager:
    HISTORY_FILE = config.DATA_DIR / 'six_hour_history.json'
    
    @classmethod
    def _load_history(cls) -> List[Dict[str, Any]]:
        if not cls.HISTORY_FILE.exists():
            return []
        
        try:
            with open(cls.HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载历史记录失败: {e}")
            return []
    
    @classmethod
    def _save_history(cls, history: List[Dict[str, Any]]):
        try:
            cls.HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(cls.HISTORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            logger.info(f"保存历史记录: {len(history)} 条")
        except Exception as e:
            logger.error(f"保存历史记录失败: {e}")
    
    @classmethod
    def record_six_hour_signal(cls, symbol: str, start_time: datetime, end_time: datetime, 
                                hours: int, price: float, volume: float, gain: float):
        beijing_now = datetime.utcnow() + timedelta(hours=8)
        
        record = {
            'symbol': symbol,
            'start_time': start_time.strftime('%Y-%m-%d %H:%M'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M'),
            'hours': hours,
            'price': price,
            'volume': volume,
            'gain': gain,
            'record_time': beijing_now.strftime('%Y-%m-%d %H:%M:%S'),
            'date': beijing_now.strftime('%Y-%m-%d')
        }
        
        history = cls._load_history()
        
        exists = False
        for h in history[-100:]:
            if (h['symbol'] == symbol and 
                h['start_time'] == record['start_time'] and
                h['end_time'] == record['end_time']):
                exists = True
                break
        
        if not exists:
            history.append(record)
            history.sort(key=lambda x: x['record_time'], reverse=True)
            
            if len(history) > 1000:
                history = history[:1000]
            
            cls._save_history(history)
            logger.info(f"记录6小时信号: {symbol} {start_time.strftime('%H:%M')} ~ {end_time.strftime('%H:%M')} ({hours}小时)")
        
        return not exists
    
    @classmethod
    def get_history(cls, days: int = 7, symbol: str = None) -> List[Dict[str, Any]]:
        history = cls._load_history()
        
        if days > 0:
            cutoff_date = (datetime.utcnow() + timedelta(hours=8) - timedelta(days=days)).strftime('%Y-%m-%d')
            history = [h for h in history if h['date'] >= cutoff_date]
        
        if symbol:
            history = [h for h in history if h['symbol'] == symbol.upper()]
        
        return history
    
    @classmethod
    def get_latest(cls, limit: int = 20) -> List[Dict[str, Any]]:
        history = cls._load_history()
        return history[:limit]
    
    @classmethod
    def get_stats(cls) -> Dict[str, Any]:
        history = cls._load_history()
        
        if not history:
            return {
                'total': 0,
                'today': 0,
                'symbols': []
            }
        
        beijing_now = datetime.utcnow() + timedelta(hours=8)
        today_str = beijing_now.strftime('%Y-%m-%d')
        
        today_count = sum(1 for h in history if h['date'] == today_str)
        
        symbol_counts = {}
        for h in history:
            symbol_counts[h['symbol']] = symbol_counts.get(h['symbol'], 0) + 1
        
        top_symbols = sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            'total': len(history),
            'today': today_count,
            'symbols': top_symbols
        }
