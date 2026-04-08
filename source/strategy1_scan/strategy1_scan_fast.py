#!/usr/bin/env python3
"""策略1 稳步抬升信号扫描"""
import io, json, logging
import pandas as pd
from qcloud_cos import CosConfig, CosS3Client
from datetime import datetime, timedelta

SECRET_ID = "AKID7LTQ9fx0pRFpBIAnCNWVHtHQK1UkngNr"
SECRET_KEY = "JPK5fJn59yaz95HI7kBQTJuJUdYGl370"
REGION = "ap-seoul"
BUCKET = "lhsj-1h-1314017643"
ENDPOINT = "cos.ap-seoul.myqcloud.com"
COS_KEY = "klines/futures_latest.parquet"
OUTPUT_FILE = "/var/www/all_signals.json"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PARAMS = {'最小连续小时数': 3, '最小震幅': 0.005, '最大震幅': 0.025}

def main():
    logger.info("读取COS数据...")
    config = CosConfig(Region=REGION, SecretId=SECRET_ID, SecretKey=SECRET_KEY, Endpoint=ENDPOINT)
    client = CosS3Client(config)
    resp = client.get_object(Bucket=BUCKET, Key=COS_KEY)
    df = pd.read_parquet(io.BytesIO(resp['Body'].get_raw_stream().read()))
    logger.info(f"读取完成: {len(df)} 条K线, {df['symbol'].nunique()} 个币种")

    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms') + timedelta(hours=8)
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=['symbol', 'close', 'volume'])
    df['quote_volume'] = df['close'] * df['volume']
    df = df.sort_values(['symbol', 'timestamp'])

    now_utc = datetime.utcnow()
    now_bj = now_utc + timedelta(hours=8)
    t_start = now_bj - timedelta(hours=6)
    t_end = now_bj - timedelta(hours=3)
    logger.info(f"当前北京时间: {now_bj.strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"扫描窗口: {t_start.strftime('%H:%M')} ~ {t_end.strftime('%H:%M')}")

    vol_24h = df.groupby('symbol')['quote_volume'].sum().sort_values(ascending=False)
    top100 = vol_24h.head(100).index.tolist()
    logger.info(f"扫描Top100币种")

    results = []
    for idx, symbol in enumerate(top100, 1):
        group = df[df['symbol'] == symbol].copy()
        if len(group) < 5:
            continue
        group = group.sort_values('timestamp').tail(72).reset_index(drop=True)
        n = len(group)
        i = 0
        while i <= n - PARAMS['最小连续小时数']:
            first_range = (group.iloc[i]['high'] - group.iloc[i]['low']) / group.iloc[i]['low']
            if not (PARAMS['最小震幅'] <= first_range <= PARAMS['最大震幅']):
                i += 1
                continue
            consecutive_indices = [i]
            j = i + 1
            while j < n:
                curr_range = (group.iloc[j]['high'] - group.iloc[j]['low']) / group.iloc[j]['low']
                if not (PARAMS['最小震幅'] <= curr_range <= PARAMS['最大震幅']):
                    break
                if group.iloc[j]['low'] < group.iloc[j-1]['low']:
                    break
                consecutive_indices.append(j)
                j += 1
            if len(consecutive_indices) >= PARAMS['最小连续小时数']:
                seg = group.iloc[consecutive_indices]
                end_dt = seg.iloc[-1]['timestamp']
                if t_start <= end_dt <= t_end:
                    total_gain = (seg.iloc[-1]['close'] - seg.iloc[0]['open']) / seg.iloc[0]['open'] * 100
                    last_vol = seg.iloc[-1]['quote_volume']
                    bars = []
                    for _, row in seg.iterrows():
                        r = (row['high'] - row['low']) / row['low'] * 100
                        bars.append({
                            't': row['timestamp'].strftime('%m-%d %H:%M'),
                            'o': f"{row['open']:.6f}", 'high': f"{row['high']:.6f}",
                            'low': f"{row['low']:.6f}", 'c': f"{row['close']:.6f}",
                            'r': f"{r:.2f}%", 'v': f"{row['quote_volume']/1e6:.2f}M"
                        })
                    results.append({
                        'symbol': symbol,
                        'time': f"{seg.iloc[0]['timestamp'].strftime('%H:%M')} ~ {seg.iloc[-1]['timestamp'].strftime('%H:%M')}",
                        'startTime': seg.iloc[0]['timestamp'].strftime('%m-%d %H:%M'),
                        'hrs': len(consecutive_indices),
                        'vol': round(last_vol/1e6, 2),
                        'gain': round(total_gain, 2),
                        'bars': bars
                    })
            i += 1
        if idx % 20 == 0:
            logger.info(f"进度: {idx}/100")

    results.sort(key=lambda x: -x['vol'])
    logger.info(f"找到 {len(results)} 个信号")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f"已保存到 {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
