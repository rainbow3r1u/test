"""
币安账户信息获取模块
获取合约账户余额和持仓盈亏
"""
import time
import hmac
import hashlib
import requests
from typing import Dict, List, Optional
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config
from utils.logger import get_logger

logger = get_logger('binance_account')


class BinanceAccount:
    _cache: Optional[Dict] = None
    _cache_time: Optional[float] = None
    _cache_ttl: int = 5
    
    @classmethod
    def _generate_signature(cls, params: dict, timestamp: int) -> str:
        query_string = f"timestamp={timestamp}"
        for k, v in sorted(params.items()):
            if v is not None:
                query_string += f"&{k}={v}"
        
        signature = hmac.new(
            config.BINANCE_SECRET_KEY.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature, query_string
    
    @classmethod
    def _request(cls, endpoint: str, params: dict = None) -> Optional[dict]:
        timestamp = int(time.time() * 1000)
        
        if params is None:
            params = {}
        
        signature, query_string = cls._generate_signature(params, timestamp)
        url = f"{config.BINANCE_API}{endpoint}?{query_string}&signature={signature}"
        
        headers = {
            'X-MBX-APIKEY': config.BINANCE_API_KEY
        }
        
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"请求失败 {endpoint}: {e}")
            return None
    
    @classmethod
    def get_balance(cls, use_cache: bool = True) -> Dict:
        if use_cache and cls._cache is not None and cls._cache_time is not None:
            if time.time() - cls._cache_time < cls._cache_ttl:
                return cls._cache.get('balance', {})
        
        data = cls._request('/fapi/v2/balance')
        if not data:
            return {}
        
        balance = {}
        for item in data:
            asset = item.get('asset', '')
            if asset == 'USDT':
                balance = {
                    'asset': asset,
                    'balance': float(item.get('balance', 0)),
                    'availableBalance': float(item.get('availableBalance', 0)),
                    'crossWalletBalance': float(item.get('crossWalletBalance', 0)),
                    'crossUnPnl': float(item.get('crossUnPnl', 0))
                }
                break
        
        if cls._cache is None:
            cls._cache = {}
        cls._cache['balance'] = balance
        cls._cache_time = time.time()
        
        return balance
    
    @classmethod
    def get_positions(cls, use_cache: bool = True) -> List[Dict]:
        if use_cache and cls._cache is not None and cls._cache_time is not None:
            if time.time() - cls._cache_time < cls._cache_ttl:
                if 'positions' in cls._cache:
                    return cls._cache.get('positions', [])
        
        data = cls._request('/fapi/v2/positionRisk')
        if not data:
            return []
        
        positions = []
        for item in data:
            position_amt = float(item.get('positionAmt', 0))
            if position_amt != 0:
                unrealized_profit = float(item.get('unRealizedProfit', 0))
                entry_price = float(item.get('entryPrice', 0))
                mark_price = float(item.get('markPrice', 0))
                liquidation_price = float(item.get('liquidationPrice', 0))
                
                roi = 0
                if entry_price != 0 and position_amt != 0:
                    if position_amt > 0:
                        roi = (mark_price - entry_price) / entry_price * 100
                    else:
                        roi = (entry_price - mark_price) / entry_price * 100
                
                positions.append({
                    'symbol': item.get('symbol', ''),
                    'positionSide': item.get('positionSide', 'BOTH'),
                    'positionAmt': position_amt,
                    'entryPrice': entry_price,
                    'markPrice': mark_price,
                    'unRealizedProfit': unrealized_profit,
                    'liquidationPrice': liquidation_price,
                    'leverage': int(item.get('leverage', 1)),
                    'marginType': item.get('marginType', 'cross'),
                    'positionInitialMargin': float(item.get('positionInitialMargin', 0)),
                    'direction': '多' if position_amt > 0 else '空',
                    'roi': roi
                })
        
        positions.sort(key=lambda x: abs(x['unRealizedProfit']), reverse=True)
        
        if cls._cache is None:
            cls._cache = {}
        cls._cache['positions'] = positions
        cls._cache_time = time.time()
        
        return positions
    
    @classmethod
    def get_account_info(cls) -> Dict:
        balance = cls.get_balance()
        positions = cls.get_positions()
        
        total_pnl = sum(p['unRealizedProfit'] for p in positions)
        
        return {
            'balance': balance,
            'positions': positions,
            'totalPnl': total_pnl,
            'positionCount': len(positions)
        }
    
    @classmethod
    def clear_cache(cls):
        cls._cache = None
        cls._cache_time = None


def get_account_balance() -> Dict:
    return BinanceAccount.get_balance()


def get_positions() -> List[Dict]:
    return BinanceAccount.get_positions()


def get_account_info() -> Dict:
    return BinanceAccount.get_account_info()
