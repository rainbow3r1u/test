"""
通用工具函数
"""
from typing import Union
import pandas as pd


def format_volume(v: Union[int, float, str]) -> str:
    if isinstance(v, str):
        return v
    
    if v >= 1e9:
        return f"{v/1e9:.1f}B"
    elif v >= 1e6:
        return f"{v/1e6:.1f}M"
    elif v >= 1e3:
        return f"{v/1e3:.1f}K"
    return f"{v:.0f}"


def parse_volume(v: Union[str, int, float]) -> float:
    if isinstance(v, (int, float)):
        return float(v)
    
    v = str(v).upper()
    if 'B' in v:
        return float(v.replace('B', '')) * 1e9
    elif 'M' in v:
        return float(v.replace('M', '')) * 1e6
    elif 'K' in v:
        return float(v.replace('K', '')) * 1e3
    return float(v)


def convert_to_4h(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(['symbol', 'timestamp'])
    df['hour'] = df['timestamp'].dt.floor('4h')
    
    agg = df.groupby(['symbol', 'hour']).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum'
    }).reset_index()
    
    agg.columns = ['symbol', 'open_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume']
    return agg


def get_beijing_now():
    from datetime import datetime, timedelta
    return datetime.utcnow() + timedelta(hours=8)


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.replace('/USDT:USDT', '').replace('/USDT', '')
    return symbol


def full_symbol(symbol: str) -> str:
    if '/' in symbol:
        return symbol
    return f"{symbol}/USDT:USDT"
