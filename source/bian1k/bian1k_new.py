#!/usr/bin/env python3
"""
币安永续合约K线采集 - 重构版本
直连币安API，支持重试和错误处理
"""
import io
import time
import json
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

from qcloud_cos import CosConfig, CosS3Client
from requests.exceptions import RequestException
import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config
from utils.logger import get_logger

logger = get_logger('bian1k')


class BinanceKlineCollector:
    def __init__(self):
        self.api_base = config.BINANCE_API
        self.max_retries = config.MAX_RETRIES
        self.retry_delays = config.RETRY_DELAY_SECONDS
        self.timeout = config.REQUEST_TIMEOUT
    
    def _api_request(self, path: str, params: Dict = None) -> Any:
        url = f"{self.api_base}{path}"
        for attempt in range(self.max_retries):
            try:
                resp = requests.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"请求失败 (尝试 {attempt+1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delays[attempt])
                else:
                    logger.error(f"请求最终失败: {url}")
                    return None
        return None
    
    def get_perpetual_symbols(self) -> List[str]:
        data = self._api_request("/fapi/v1/exchangeInfo")
        if not data:
            logger.error("获取交易对信息失败")
            return []
        
        symbols = []
        for s in data.get('symbols', []):
            if (s.get('contractType') == 'PERPETUAL'
                    and s.get('quoteAsset') == 'USDT'
                    and s.get('status') == 'TRADING'
                    and s.get('isInverse') is not True):
                symbol = s['symbol']
                if 'UP' not in symbol and 'DOWN' not in symbol:
                    symbols.append(symbol)
        
        logger.info(f"获取到 {len(symbols)} 个 USDT 永续合约")
        return symbols
    
    def fetch_klines(self, symbol: str, interval: str = '1h', limit: int = 480) -> List[Dict]:
        params = {'symbol': symbol, 'interval': interval, 'limit': limit}
        data = self._api_request("/fapi/v1/klines", params=params)
        if not data:
            return []
        
        rows = []
        for k in data:
            rows.append({
                'symbol': symbol,
                'timestamp': k[0],
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
                'volume': float(k[5])
            })
        return rows
    
    def upload_to_cos(self, local_file: str, cos_key: str) -> bool:
        cos_config = CosConfig(
            Region=config.COS_REGION,
            SecretId=config.COS_SECRET_ID,
            SecretKey=config.COS_SECRET_KEY,
            Endpoint=config.COS_ENDPOINT
        )
        client = CosS3Client(cos_config)
        
        for attempt in range(self.max_retries):
            try:
                resp = client.upload_file(
                    Bucket=config.COS_BUCKET,
                    LocalFilePath=local_file,
                    Key=cos_key,
                    EnableMD5=False
                )
                logger.info(f"上传成功，ETag: {resp['ETag']}")
                return True
            except RequestException as e:
                logger.warning(f"上传失败 (尝试 {attempt+1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delays[attempt])
                else:
                    logger.error(f"上传最终失败: {local_file}")
                    return False
        return False
    
    def run(self) -> bool:
        start_time = datetime.now(timezone.utc)
        logger.info("="*60)
        logger.info("开始采集币安永续合约K线数据")
        logger.info("="*60)
        
        symbols = self.get_perpetual_symbols()
        if not symbols:
            logger.error("未获取到任何永续合约，退出")
            return False
        
        all_rows = []
        total = len(symbols)
        
        for idx, symbol in enumerate(symbols, 1):
            logger.info(f"进度: {idx}/{total} - {symbol}")
            rows = self.fetch_klines(symbol)
            if rows:
                all_rows.extend(rows)
            time.sleep(0.05)
        
        if not all_rows:
            logger.error("未采集到任何K线数据，退出")
            return False
        
        df = pd.DataFrame(all_rows)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        logger.info(f"共采集到 {len(df)} 条K线记录")
        
        local_file = "/tmp/perpetual_klines_latest.parquet"
        df.to_parquet(local_file, index=False)
        
        success = self.upload_to_cos(local_file, config.COS_KEY)
        
        import os
        if os.path.exists(local_file):
            os.remove(local_file)
        
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(f"总耗时: {elapsed:.2f} 秒")
        
        if success:
            logger.info("="*60)
            logger.info("数据采集上传成功")
            logger.info("="*60)
        else:
            logger.error("="*60)
            logger.error("数据上传失败")
            logger.error("="*60)
        
        return success


def main():
    collector = BinanceKlineCollector()
    return collector.run()


if __name__ == "__main__":
    main()
