"""
数据加载器 - 带缓存的COS数据读取
"""
import io
import time
import pandas as pd
from typing import Optional
from qcloud_cos import CosConfig, CosS3Client

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config
from utils.logger import get_logger

logger = get_logger('data_loader')


class DataLoader:
    _cache: Optional[pd.DataFrame] = None
    _cache_time: Optional[float] = None
    _cache_minutes: int = config.DATA_CACHE_MINUTES

    _cos_client: Optional[CosS3Client] = None

    @classmethod
    def _get_cos_client(cls) -> CosS3Client:
        if cls._cos_client is None:
            cos_config = CosConfig(
                Region=config.COS_REGION,
                SecretId=config.COS_SECRET_ID,
                SecretKey=config.COS_SECRET_KEY,
                Endpoint=config.COS_ENDPOINT
            )
            cls._cos_client = CosS3Client(cos_config)
        return cls._cos_client

    @classmethod
    def _fetch_from_cos(cls) -> pd.DataFrame:
        logger.info("从COS读取数据...")
        client = cls._get_cos_client()

        for attempt in range(config.MAX_RETRIES):
            try:
                resp = client.get_object(Bucket=config.COS_BUCKET, Key=config.COS_KEY)
                data = resp['Body'].get_raw_stream().read()
                df = pd.read_parquet(io.BytesIO(data))

                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                df = df.dropna(subset=['symbol', 'close', 'volume'])
                df['quote_volume'] = df['close'] * df['volume']

                logger.info(f"读取完成: {len(df)} 条K线, {df['symbol'].nunique()} 个币种")
                return df

            except Exception as e:
                logger.warning(f"读取失败 (尝试 {attempt+1}/{config.MAX_RETRIES}): {e}")
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(config.RETRY_DELAY_SECONDS[attempt])
                else:
                    logger.error(f"COS读取最终失败: {e}")
                    raise

    @classmethod
    def get_klines(cls, use_cache: bool = True) -> pd.DataFrame:
        if use_cache and cls._cache is not None and cls._cache_time is not None:
            elapsed = (time.time() - cls._cache_time) / 60
            if elapsed < cls._cache_minutes:
                logger.debug(f"使用缓存数据 (已缓存 {elapsed:.1f} 分钟)")
                return cls._cache.copy()

        cls._cache = cls._fetch_from_cos()
        cls._cache_time = time.time()
        return cls._cache.copy()

    @classmethod
    def clear_cache(cls):
        cls._cache = None
        cls._cache_time = None
        logger.info("缓存已清除")

    @classmethod
    def get_symbol_data(cls, symbol: str, use_cache: bool = True) -> Optional[pd.DataFrame]:
        df = cls.get_klines(use_cache=use_cache)
        symbol_data = df[df['symbol'] == symbol].copy()
        if len(symbol_data) == 0:
            return None
        return symbol_data.sort_values('timestamp').reset_index(drop=True)

    @classmethod
    def get_top_symbols(cls, n: int = 100, use_cache: bool = True) -> list:
        df = cls.get_klines(use_cache=use_cache)
        vol_24h = df.groupby('symbol')['quote_volume'].sum().sort_values(ascending=False)
        return vol_24h.head(n).index.tolist()


def read_cos_data() -> pd.DataFrame:
    return DataLoader.get_klines()
