"""
统一配置文件 - 使用环境变量管理敏感信息
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / 'output'
DATA_DIR = BASE_DIR / 'data'
STATIC_DIR = BASE_DIR / 'static'
CHARTS_DIR = STATIC_DIR / 'charts'

for d in [OUTPUT_DIR, DATA_DIR, STATIC_DIR, CHARTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

COS_SECRET_ID = os.environ.get('COS_SECRET_ID', '')
COS_SECRET_KEY = os.environ.get('COS_SECRET_KEY', '')
COS_REGION = os.environ.get('COS_REGION', 'ap-seoul')
COS_BUCKET = os.environ.get('COS_BUCKET', '')
COS_ENDPOINT = os.environ.get('COS_ENDPOINT', 'cos.ap-seoul.myqcloud.com')
COS_KEY = os.environ.get('COS_KEY', 'klines/futures_latest.parquet')

FEISHU_APP_ID = os.environ.get('FEISHU_APP_ID', '')
FEISHU_APP_SECRET = os.environ.get('FEISHU_APP_SECRET', '')
FEISHU_USER_OPEN_ID = os.environ.get('FEISHU_USER_OPEN_ID', '')

DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY', '')
DEEPSEEK_BASE_URL = os.environ.get('DEEPSEEK_BASE_URL', 'https://api.deepseek.com/chat/completions')
DEEPSEEK_MODEL = os.environ.get('DEEPSEEK_MODEL', 'deepseek-chat')

WEB_HOST = os.environ.get('WEB_HOST', '0.0.0.0')
WEB_PORT = int(os.environ.get('WEB_PORT', 5000))

DB_PATH = os.environ.get('DB_PATH', str(DATA_DIR / 'signals.db'))

BINANCE_API = os.environ.get('BINANCE_API', 'https://fapi.binance.com')

BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', '')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY', '')

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = [5, 15, 30]
REQUEST_TIMEOUT = 10

DATA_CACHE_MINUTES = int(os.environ.get('DATA_CACHE_MINUTES', 60))
CHART_CACHE_HOURS = int(os.environ.get('CHART_CACHE_HOURS', 1))
