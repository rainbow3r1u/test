#!/usr/bin/env python3
"""
COS客户端 - 读取币安K线数据
"""

import io
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config

def read_cos_data():
    """从COS读取K线数据"""
    from qcloud_cos import CosConfig, CosS3Client
    
    cos_config = CosConfig(
        Region=config.COS_REGION,
        SecretId=config.COS_SECRET_ID,
        SecretKey=config.COS_SECRET_KEY,
        Endpoint=config.COS_ENDPOINT
    )
    client = CosS3Client(cos_config)
    
    resp = client.get_object(Bucket=config.COS_BUCKET, Key=config.COS_KEY)
    data = resp['Body'].get_raw_stream().read()
    
    df = pd.read_parquet(io.BytesIO(data))
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df = df.dropna(subset=['symbol', 'close', 'volume'])
    df['quote_volume'] = df['close'] * df['volume']
    
    return df

if __name__ == '__main__':
    df = read_cos_data()
    print(f"读取到 {len(df)} 条数据")
    print(f"时间范围: {df['timestamp'].min()} ~ {df['timestamp'].max()}")
