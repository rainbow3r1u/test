#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
币安永续合约K线采集 - 并发版本
使用ThreadPoolExecutor并发获取K线数据，大幅提升采集速度
"""
import pandas as pd
import time
import logging
from datetime import datetime, timezone
from qcloud_cos import CosConfig, CosS3Client
from requests.exceptions import RequestException
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

SECRET_ID = "AKID7LTQ9fx0pRFpBIAnCNWVHtHQK1UkngNr"
SECRET_KEY = "JPK5fJn59yaz95HI7kBQTJuJUdYGl370"
REGION = "ap-seoul"
BUCKET = "lhsj-1h-1314017643"
ENDPOINT = "cos.ap-seoul.myqcloud.com"
COS_KEY = "klines/futures_latest.parquet"

BINANCE_API = "https://fapi.binance.com"
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = [5, 15, 30]
REQUEST_TIMEOUT = 10
CONCURRENT_WORKERS = 30

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

request_lock = threading.Lock()
request_count = 0

def api_request(path, params=None, retries=MAX_RETRIES):
    global request_count
    url = f"{BINANCE_API}{path}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            with request_lock:
                request_count += 1
            return resp.json()
        except Exception as e:
            logger.warning(f"请求失败 (尝试 {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY_SECONDS[attempt])
            else:
                logger.error(f"请求最终失败: {url}")
                return None
    return None

def get_perpetual_symbols():
    data = api_request("/fapi/v1/exchangeInfo")
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

def fetch_klines(symbol, interval='1h', limit=480):
    params = {'symbol': symbol, 'interval': interval, 'limit': limit}
    data = api_request("/fapi/v1/klines", params=params)
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

def fetch_klines_task(symbol, idx, total):
    rows = fetch_klines(symbol)
    if idx % 50 == 0 or idx == total:
        logger.info(f"进度: {idx}/{total}")
    time.sleep(0.02)
    return symbol, rows

def upload_to_cos(local_file_path, cos_key):
    config = CosConfig(Region=REGION, SecretId=SECRET_ID, SecretKey=SECRET_KEY, Endpoint=ENDPOINT)
    client = CosS3Client(config)
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.upload_file(Bucket=BUCKET, LocalFilePath=local_file_path, Key=cos_key, EnableMD5=False)
            logger.info(f"上传成功，ETag: {resp['ETag']}")
            return True
        except RequestException as e:
            logger.warning(f"上传失败 (尝试 {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_SECONDS[attempt])
            else:
                logger.error(f"上传最终失败: {local_file_path}")
                return False
    return False

def main():
    global request_count
    start_time = datetime.now(timezone.utc)
    logger.info("===== 开始采集币安永续合约K线数据（并发版本） =====")
    logger.info(f"并发数: {CONCURRENT_WORKERS}")

    symbols = get_perpetual_symbols()
    if not symbols:
        logger.error("未获取到任何永续合约，退出")
        return

    total = len(symbols)
    all_rows = []
    success_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        futures = {
            executor.submit(fetch_klines_task, symbol, idx, total): symbol
            for idx, symbol in enumerate(symbols, 1)
        }
        
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                _, rows = future.result()
                if rows:
                    all_rows.extend(rows)
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                logger.warning(f"{symbol} 获取失败: {e}")
                fail_count += 1

    if not all_rows:
        logger.error("未采集到任何K线数据，退出")
        return

    df = pd.DataFrame(all_rows)
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    logger.info(f"共采集到 {len(df)} 条K线记录, {df['symbol'].nunique()} 个币种")
    logger.info(f"成功: {success_count}, 失败: {fail_count}, 总请求: {request_count}")

    local_file = "/tmp/perpetual_klines_latest.parquet"
    df.to_parquet(local_file, index=False)

    if upload_to_cos(local_file, COS_KEY):
        logger.info("===== 数据上传成功 =====")
    else:
        logger.error("===== 数据上传失败 =====")

    import os
    os.remove(local_file)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info(f"总耗时: {elapsed:.2f} 秒")
    logger.info(f"平均每个币种: {elapsed/total*1000:.1f} 毫秒")

if __name__ == "__main__":
    main()
