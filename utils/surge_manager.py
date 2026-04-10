"""
暴涨记录管理器 - 记录1小时涨幅10%以上的币种
"""
import json
import base64
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config
from utils.logger import get_logger

logger = get_logger('surge_manager')


class SurgeManager:
    SURGE_FILE = config.DATA_DIR / 'surge_records.json'
    SURGE_IMAGES_DIR = config.DATA_DIR / 'surge_images'
    
    @classmethod
    def _load_records(cls) -> List[Dict[str, Any]]:
        if not cls.SURGE_FILE.exists():
            return []
        
        try:
            with open(cls.SURGE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载暴涨记录失败: {e}")
            return []
    
    @classmethod
    def _save_records(cls, records: List[Dict[str, Any]]):
        try:
            cls.SURGE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(cls.SURGE_FILE, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            logger.info(f"保存暴涨记录: {len(records)} 条")
        except Exception as e:
            logger.error(f"保存暴涨记录失败: {e}")
    
    @classmethod
    def _save_image(cls, symbol: str, timestamp: str, image_data: bytes) -> Optional[str]:
        try:
            cls.SURGE_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
            
            filename = f"{symbol}_{timestamp.replace(':', '-').replace(' ', '_')}.png"
            filepath = cls.SURGE_IMAGES_DIR / filename
            
            with open(filepath, 'wb') as f:
                f.write(image_data)
            
            logger.info(f"保存暴涨图片: {filename}")
            return str(filepath)
        except Exception as e:
            logger.error(f"保存图片失败: {e}")
            return None
    
    @classmethod
    def record_surge(cls, symbol: str, gain: float, price: float, 
                     volume: float, image_data: Optional[bytes] = None,
                     surge_time: Optional[datetime] = None) -> bool:
        if surge_time is None:
            surge_time = datetime.utcnow()
        
        timestamp_str = surge_time.strftime('%Y-%m-%d %H:%M:%S')
        date_str = surge_time.strftime('%Y-%m-%d')
        
        record = {
            'symbol': symbol,
            'gain': round(gain, 2),
            'price': price,
            'volume': volume,
            'timestamp': timestamp_str,
            'date': date_str,
            'hour': surge_time.hour,
            'image_path': None
        }
        
        if image_data:
            image_path = cls._save_image(symbol, timestamp_str, image_data)
            if image_path:
                record['image_path'] = image_path
        
        records = cls._load_records()
        
        cutoff_time = surge_time - timedelta(hours=1)
        exists = False
        for r in records[-100:]:
            if (r['symbol'] == symbol and 
                r['date'] == date_str and
                datetime.strptime(r['timestamp'], '%Y-%m-%d %H:%M:%S') > cutoff_time):
                exists = True
                break
        
        if not exists:
            records.append(record)
            records.sort(key=lambda x: x['timestamp'], reverse=True)
            
            if len(records) > 1000:
                records = records[:1000]
            
            cls._save_records(records)
            logger.info(f"记录暴涨: {symbol} +{gain:.2f}% at {timestamp_str}")
            return True
        
        return False
    
    @classmethod
    def get_records(cls, days: int = 1, symbol: str = None) -> List[Dict[str, Any]]:
        records = cls._load_records()
        
        if days > 0:
            cutoff_date = (datetime.utcnow() - timedelta(days=days-1)).strftime('%Y-%m-%d')
            records = [r for r in records if r['date'] >= cutoff_date]
        
        if symbol:
            records = [r for r in records if r['symbol'] == symbol.upper()]
        
        return records
    
    @classmethod
    def get_today_stats(cls) -> Dict[str, Any]:
        records = cls._load_records()
        
        utc_now = datetime.utcnow()
        today_str = utc_now.strftime('%Y-%m-%d')
        
        today_records = [r for r in records if r['date'] == today_str]
        
        if not today_records:
            return {
                'total': 0,
                'max_gain': 0,
                'symbols': []
            }
        
        max_gain = max(r['gain'] for r in today_records)
        symbol_counts = {}
        for r in today_records:
            symbol_counts[r['symbol']] = symbol_counts.get(r['symbol'], 0) + 1
        
        top_symbols = sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            'total': len(today_records),
            'max_gain': max_gain,
            'symbols': top_symbols
        }
    
    @classmethod
    def get_image(cls, symbol: str, timestamp: str) -> Optional[bytes]:
        try:
            filename = f"{symbol}_{timestamp.replace(':', '-').replace(' ', '_')}.png"
            filepath = cls.SURGE_IMAGES_DIR / filename
            
            if filepath.exists():
                with open(filepath, 'rb') as f:
                    return f.read()
        except Exception as e:
            logger.error(f"读取图片失败: {e}")
        
        return None
