#!/usr/bin/env python3
"""
DeepSeek策略 - AI分析推荐做多币种
每小时运行，输出JSON到output目录
"""

import io
import os
import json
import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from openai import OpenAI
from qcloud_cos import CosConfig, CosS3Client
import ccxt

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config

# DeepSeek配置
DEEPSEEK_API_KEY = "sk-8eb6fca470574dfb882b4539ed24ac77"
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-chat"

# 图表缓存目录
CHART_CACHE_DIR = os.path.join(os.path.dirname(__file__), '..', 'static', 'charts')

def read_cos_data():
    """从COS读取K线数据"""
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

def condition_4_flat_break(df):
    """横盘10小时 + 放量启动形态"""
    results = []
    grouped = df.groupby('symbol')
    for symbol, group in grouped:
        if len(group) < 14:
            continue
        recent = group.tail(14).copy()
        recent = recent.sort_values('timestamp')
        
        cons = recent.iloc[0:10]
        rise = recent.iloc[10:14]
        
        if len(rise) < 2:
            continue
        
        cons_close_max = cons['close'].max()
        cons_close_min = cons['close'].min()
        cons_range = (cons_close_max - cons_close_min) / cons_close_min * 100
        cons_avg_vol = cons['quote_volume'].mean()
        
        h1 = rise.iloc[0]
        h2 = rise.iloc[1]
        rise1_gain = (h2['close'] - h1['close']) / h1['close'] * 100
        rise1_vol = h2['quote_volume']
        
        if cons_range < 6 and rise1_gain > 0.5:
            vol_ratio = rise1_vol / cons_avg_vol if cons_avg_vol > 0 else 0
            if vol_ratio > 1.5:
                results.append({
                    'symbol': symbol,
                    'cons_range': round(cons_range, 2),
                    'rise1_gain': round(rise1_gain, 2),
                    'vol_ratio': round(vol_ratio, 2)
                })
    
    results.sort(key=lambda x: (x['vol_ratio'], x['rise1_gain']), reverse=True)
    return results[:10]

def condition_2_flat_break10(df):
    """前10小时横盘(波动≤5%) + 第11小时涨幅≥10%且之后未回落"""
    results = []
    grouped = df.groupby('symbol')
    for symbol, group in grouped:
        if len(group) < 11:
            continue
        for i in range(len(group)-10):
            window = group.iloc[i:i+10]
            high = window['high'].max()
            low = window['low'].min()
            range_pct = (high - low) / low * 100
            if range_pct > 5:
                continue
            hour11 = group.iloc[i+10]
            open11 = hour11['open']
            close11 = hour11['close']
            gain11 = (close11 - open11) / open11 * 100
            if gain11 < 10:
                continue
            later = group.iloc[i+11:]
            if len(later) > 0 and (later['close'] > close11).any():
                continue
            results.append({
                'symbol': symbol,
                'break_time': str(hour11['timestamp']),
                'gain': round(gain11, 2),
                'range_pct': round(range_pct, 2)
            })
            break
    return results[:10]

def get_top_metrics(df):
    """获取市场指标"""
    now = datetime.utcnow() + timedelta(hours=8)  # 北京时间
    cutoff = now - timedelta(hours=72)
    df_72 = df[df['timestamp'] >= cutoff].copy()
    
    if len(df_72) == 0:
        return None
    
    metrics = df_72.groupby('symbol').agg(
        start_price=('open', 'first'),
        end_price=('close', 'last'),
        high=('high', 'max'),
        low=('low', 'min'),
        quote_volume=('quote_volume', 'sum')
    ).reset_index()
    metrics['change_pct'] = (metrics['end_price'] - metrics['start_price']) / metrics['start_price'] * 100
    metrics['volatility'] = (metrics['high'] - metrics['low']) / metrics['start_price'] * 100
    total_quote_volume = df_72['quote_volume'].sum()
    
    return {
        'total_volume': total_quote_volume,
        'top_gainers': metrics.nlargest(10, 'change_pct')[['symbol', 'change_pct', 'quote_volume']].to_dict('records'),
        'top_volume': metrics.nlargest(10, 'quote_volume')[['symbol', 'quote_volume', 'change_pct']].to_dict('records'),
        'high_volatility': metrics.nlargest(10, 'volatility')[['symbol', 'volatility']].to_dict('records')
    }

def get_deepseek_recommendations(metrics, c4=None):
    """使用DeepSeek推荐币种"""
    top_gainers = pd.DataFrame(metrics['top_gainers'])
    top_volume = pd.DataFrame(metrics['top_volume'])
    high_volatility = pd.DataFrame(metrics['high_volatility'])
    
    user_prompt = f"""
过去72小时市场总成交额: {metrics['total_volume']/1e6:.0f}M USDT

涨幅Top10:
{top_gainers.to_markdown(index=False)}

成交额Top10:
{top_volume.to_markdown(index=False)}

波动率Top10:
{high_volatility.to_markdown(index=False)}

"""
    
    # 添加横盘10小时后放量启动的币种
    if c4:
        user_prompt += "横盘10小时后放量启动的币种(刚启动形态):\n"
        user_prompt += "| 币种 | 横盘波动(%) | 启动涨幅(%) | 成交量放大倍数 |\n"
        user_prompt += "|------|--------------|-------------|----------------|\n"
        for r in c4:
            user_prompt += f"| {r['symbol']} | {r['cons_range']} | +{r['rise1_gain']}% | {r['vol_ratio']}x |\n"
        user_prompt += "\n请特别关注这些刚启动的币种,分析它们的走势持续性和风险。\n"
    
    user_prompt += "\n任务:基于以上数据,推荐5个**做多**的币种。每个币种给出:币种名称、推荐理由(结合涨跌、成交量、波动率)、风险提示。输出Markdown列表。"
    
    client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
    response = client.chat.completions.create(
        model=DEEPSEEK_MODEL,
        messages=[
            {"role": "system", "content": "你是加密货币分析师。只推荐做多币种,每个币种用一句话说清理由和风险。输出格式:- **币种**:理由;风险提示"},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.7
    )
    return response.choices[0].message.content

def parse_recommendations(text):
    """解析DeepSeek返回的推荐"""
    items = []
    lines = text.split('\n')
    for line in lines:
        if '**' in line:
            import re
            # 匹配 **币种/USDT** 格式
            match = re.search(r'\*\*([A-Z/]+)\*\*', line)
            if match:
                full_symbol = match.group(1)
                # 提取币种名（去掉 /USDT）
                symbol = full_symbol.replace('/USDT', '')
                # 提取理由和风险
                parts = line.split('**')
                if len(parts) >= 3:
                    content = parts[-1]
                    risk_parts = content.split('风险提示:')
                    reason = risk_parts[0].strip().rstrip(';').strip() if risk_parts else content.strip()
                    risk = risk_parts[1].strip() if len(risk_parts) > 1 else ''
                    
                    items.append({
                        'symbol': symbol,
                        'reason': reason[:100],
                        'risk': risk[:100]
                    })
    return items

def generate_charts(symbols):
    """生成K线图表"""
    os.makedirs(CHART_CACHE_DIR, exist_ok=True)
    
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })
    
    for i, symbol in enumerate(symbols):
        full_symbol = f"{symbol}/USDT:USDT"
        cache_file = os.path.join(CHART_CACHE_DIR, f"{symbol}_USDT:USDT.png")
        
        # 跳过已有缓存
        if os.path.exists(cache_file) and os.path.getmtime(cache_file) > time.time() - 3600:
            continue
        
        try:
            ohlcv_1h = exchange.fetch_ohlcv(full_symbol, timeframe='1h', limit=24)
            ohlcv_4h = exchange.fetch_ohlcv(full_symbol, timeframe='4h', limit=40)
            
            if not ohlcv_1h:
                continue
            
            df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # 聚合日K
            df_4h_sorted = df_4h.sort_values('timestamp').reset_index(drop=True)
            daily_data = []
            for j in range(0, len(df_4h_sorted) - 3, 4):
                group = df_4h_sorted.iloc[j:j+4]
                daily_data.append({
                    'open': group.iloc[0]['open'],
                    'high': group['high'].max(),
                    'low': group['low'].min(),
                    'close': group.iloc[-1]['close'],
                    'volume': group['volume'].sum()
                })
            df_daily = pd.DataFrame(daily_data).tail(10)
            df_4h_6 = df_4h.tail(6)
            
            # 画图
            plt.style.use('dark_background')
            fig = plt.figure(figsize=(18, 16))
            gs = fig.add_gridspec(3, 1, height_ratios=[1.2, 1, 0.8], hspace=0.25)
            ax1 = fig.add_subplot(gs[0])
            ax2 = fig.add_subplot(gs[1])
            ax3 = fig.add_subplot(gs[2])
            fig.patch.set_facecolor('#1a1a1a')
            
            # 1H K线
            for j in range(len(df_1h)):
                o = df_1h.iloc[j]['open']
                h = df_1h.iloc[j]['high']
                l = df_1h.iloc[j]['low']
                c = df_1h.iloc[j]['close']
                color = '#00a854' if c >= o else '#eb3c3c'
                ax1.plot([j, j], [l, h], color=color, linewidth=1)
                width = 0.6
                if c >= o:
                    ax1.bar([j], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
                else:
                    ax1.bar([j], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
            ax1.set_facecolor('#1a1a1a')
            ax1.grid(True, alpha=0.2, color='#333')
            ax1.set_title(f'{symbol} - 1H (24 candles)', fontsize=12, fontweight='bold', color='#fff', pad=10)
            ax1.tick_params(colors='#999', labelsize=8)
            ax1.set_xlim(-0.5, len(df_1h)-0.5)
            
            # 日K
            for j in range(len(df_daily)):
                o = df_daily.iloc[j]['open']
                h = df_daily.iloc[j]['high']
                l = df_daily.iloc[j]['low']
                c = df_daily.iloc[j]['close']
                color = '#00a854' if c >= o else '#eb3c3c'
                ax2.plot([j, j], [l, h], color=color, linewidth=1.5)
                width = 0.5
                if c >= o:
                    ax2.bar([j], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
                else:
                    ax2.bar([j], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
            ax2.set_facecolor('#1a1a1a')
            ax2.grid(True, alpha=0.2, color='#333')
            ax2.set_title('Daily (from 4H, 10 candles ~10 days)', fontsize=11, color='#fff', pad=8)
            ax2.tick_params(colors='#999', labelsize=8)
            ax2.set_xlim(-0.5, len(df_daily)-0.5)
            
            # 4H
            for j in range(len(df_4h_6)):
                o = df_4h_6.iloc[j]['open']
                h = df_4h_6.iloc[j]['high']
                l = df_4h_6.iloc[j]['low']
                c = df_4h_6.iloc[j]['close']
                color = '#00a854' if c >= o else '#eb3c3c'
                ax3.plot([j, j], [l, h], color=color, linewidth=2)
                width = 0.5
                if c >= o:
                    ax3.bar([j], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
                else:
                    ax3.bar([j], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
            ax3.set_facecolor('#1a1a1a')
            ax3.grid(True, alpha=0.2, color='#333')
            ax3.set_title('4H (6 candles ~1 day)', fontsize=11, color='#fff', pad=8)
            ax3.tick_params(colors='#999', labelsize=8)
            ax3.set_xlim(-0.5, len(df_4h_6)-0.5)
            
            plt.tight_layout()
            plt.savefig(cache_file, format='png', dpi=130, facecolor='#1a1a1a')
            plt.close()
            
            print(f"  [{i+1}] {symbol} OK")
            time.sleep(0.1)
            
        except Exception as e:
            print(f"  [{i+1}] {symbol} Error: {e}")

def main():
    # 使用北京时间
    beijing_now = datetime.utcnow() + timedelta(hours=8)
    
    print("="*60)
    print("DeepSeek策略分析")
    print("="*60)
    print(f"时间: {beijing_now.strftime('%Y-%m-%d %H:%M')} (北京时间)")
    
    # 读取数据
    print("\n读取数据...")
    try:
        df = read_cos_data()
        print(f"数据: {len(df)} 条")
    except Exception as e:
        print(f"读取失败: {e}")
        return
    
    # 获取市场指标
    print("\n计算市场指标...")
    metrics = get_top_metrics(df)
    if not metrics:
        print("数据不足")
        return
    
    # 横盘10小时后放量启动
    print("\n扫描横盘启动形态...")
    c4 = condition_4_flat_break(df)
    print(f"找到 {len(c4)} 个横盘启动形态的币种")
    for r in c4[:5]:
        print(f"  {r['symbol']}: 横盘{r['cons_range']}% 启动+{r['rise1_gain']}% 放量{r['vol_ratio']}x")
    
    # 横盘10小时后暴涨10%
    print("\n扫描横盘暴涨形态...")
    c2 = condition_2_flat_break10(df)
    print(f"找到 {len(c2)} 个横盘暴涨形态的币种")
    for r in c2[:5]:
        print(f"  {r['symbol']}: 横盘{r['range_pct']}% 暴涨+{r['gain']}%")
    
    # DeepSeek分析
    print("\nDeepSeek分析中...")
    try:
        recommendations_text = get_deepseek_recommendations(metrics, c4)
        print("DeepSeek推荐结果:")
        print(recommendations_text)
    except Exception as e:
        print(f"DeepSeek调用失败: {e}")
        recommendations_text = "DeepSeek分析失败"
    
    # 解析推荐币种
    items = parse_recommendations(recommendations_text)
    
    # 生成图表
    if items:
        print("\n生成K线图表...")
        symbols = [item['symbol'] for item in items]
        generate_charts(symbols)
    
    # 保存JSON
    json_output = {
        'strategy_name': 'deepseek_strategy',
        'title': 'DeepSeek策略',
        'timestamp': beijing_now.strftime('%Y-%m-%d %H:%M:%S'),
        'conditions': ['DeepSeek AI分析', '72小时市场数据', '推荐做多币种', '横盘10小时启动形态', '横盘10小时暴涨10%形态'],
        'raw_analysis': recommendations_text,
        'items': [{
            'symbol': item['symbol'],
            'price': '-',
            'volume': '-',
            'change': 0,
            'indicator': item['reason'][:50] if item['reason'] else '-',
            'note': item['risk'][:50] if item['risk'] else '-'
        } for item in items],
        'flat_break_coins': [{
            'symbol': r['symbol'].replace('/USDT:USDT', '').replace('/USDT', ''),
            'cons_range': r['cons_range'],
            'rise1_gain': r['rise1_gain'],
            'vol_ratio': r['vol_ratio']
        } for r in c4],
        'flat_break10_coins': [{
            'symbol': r['symbol'].replace('/USDT:USDT', '').replace('/USDT', ''),
            'range_pct': r['range_pct'],
            'gain': r['gain']
        } for r in c2]
    }
    
    json_file = Path(config.OUTPUT_DIR) / f"deepseek_strategy_{beijing_now.strftime('%Y%m%d_%H%M%S')}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(json_output, f, ensure_ascii=False, indent=2)
    
    print(f"\nJSON已保存: {json_file}")
    print(f"推荐 {len(items)} 个币种")
    
    return json_file

if __name__ == '__main__':
    main()
