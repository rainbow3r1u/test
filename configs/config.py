"""
配置文件
"""
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

COS_SECRET_ID = os.environ.get('COS_SECRET_ID', '')
COS_SECRET_KEY = os.environ.get('COS_SECRET_KEY', '')
COS_REGION = os.environ.get('COS_REGION', 'ap-seoul')
COS_BUCKET = os.environ.get('COS_BUCKET', '')
COS_ENDPOINT = os.environ.get('COS_ENDPOINT', 'cos.ap-seoul.myqcloud.com')
COS_KEY = os.environ.get('COS_KEY', 'klines/futures_latest.parquet')

FEISHU_APP_ID = os.environ.get('FEISHU_APP_ID', '')
FEISHU_APP_SECRET = os.environ.get('FEISHU_APP_SECRET', '')
FEISHU_USER_OPEN_ID = os.environ.get('FEISHU_USER_OPEN_ID', '')

BB_PERIOD = 20
BB_STD = 2
BB_CONVERGE_THRESHOLD = 0.05
BB_BETWEEN_K = 5

TWO_GREEN_MIN_DAYS = 2

WEB_HOST = os.environ.get('WEB_HOST', '0.0.0.0')
WEB_PORT = int(os.environ.get('WEB_PORT', 5000))
