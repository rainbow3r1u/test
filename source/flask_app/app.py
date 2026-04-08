#!/usr/bin/env python3
"""
加密货币策略扫描 - Web服务
手机电脑都能访问的本地网页
"""

import os
import io
import json
import glob
import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from flask import Flask, render_template, jsonify, Response, send_file, request

import ccxt
import pandas as pd
import numpy as np

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from configs import config

app = Flask(__name__)
app.template_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
app.static_folder = os.path.join(os.path.dirname(__file__), 'static')

def get_latest_report(strategy_name):
    """获取某个策略的最新报告"""
    pattern = os.path.join(config.OUTPUT_DIR, f'{strategy_name}_*.json')
    files = glob.glob(pattern)
    if not files:
        return None
    latest = max(files, key=os.path.getmtime)
    with open(latest, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_all_reports():
    """获取所有报告，每个策略只保留最新的"""
    pattern = os.path.join(config.OUTPUT_DIR, '*.json')
    files = glob.glob(pattern)
    
    # 按策略分组，每组只取最新的
    latest_by_strategy = {}
    for f in files:
        try:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                name = data.get('strategy_name', os.path.basename(f))
                timestamp = data.get('timestamp', '')
                
                # 解析时间用于比较
                try:
                    ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                except:
                    ts = datetime.min
                
                # 只保留每个策略最新的一条
                if name not in latest_by_strategy:
                    latest_by_strategy[name] = {
                        'name': name,
                        'title': data.get('title', '未命名'),
                        'timestamp': timestamp,
                        'ts': ts,
                        'filename': os.path.basename(f),
                        'data': data
                    }
                elif ts > latest_by_strategy[name]['ts']:
                    latest_by_strategy[name] = {
                        'name': name,
                        'title': data.get('title', '未命名'),
                        'timestamp': timestamp,
                        'ts': ts,
                        'filename': os.path.basename(f),
                        'data': data
                    }
        except:
            pass
    
    reports = list(latest_by_strategy.values())
    # 按时间排序，最新的在前
    reports.sort(key=lambda x: x.get('ts', datetime.min), reverse=True)
    return reports

@app.route('/')
def index():
    view = request.args.get('view', 'strategy1')
    reports = get_all_reports()
    # 每个策略只取最新一条
    latest_by_name = {}
    for r in reports:
        name = r.get('name', '')
        ts_str = r.get('timestamp', '')
        try:
            ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S') if ts_str else datetime.min
        except:
            ts = datetime.min
        if name not in latest_by_name or ts > latest_by_name[name]['ts']:
            latest_by_name[name] = {'name': name, 'timestamp': ts_str, 'title': r.get('title',''), 'ts': ts, 'data': r.get('data',{})}
    
    strategy_names = ['coin_quality','deepseek','bollinger','volume']
    strategies = [{'name': n, 'title': latest_by_name.get(n,{}).get('title',''), 'timestamp': latest_by_name.get(n,{}).get('timestamp',''), 'data': latest_by_name.get(n,{}).get('data',{})} for n in strategy_names]
    
    # 预填稳步抬升数据
    s1_data = []
    s1_file = '/var/www/all_signals.json'
    if os.path.exists(s1_file):
        with open(s1_file, 'r', encoding='utf-8') as f:
            s1_data = json.load(f)
    
    return render_template('index.html', strategies=strategies, s1_data=s1_data, current_view=view)

@app.route('/report/<strategy>')
def report(strategy):
    """单个报告页面"""
    report = get_latest_report(strategy)
    if report is None:
        return "报告不存在", 404
    return jsonify(report)

@app.route('/api/reports')
def api_reports():
    """API: 获取所有报告列表"""
    reports = get_all_reports()
    return jsonify({
        'code': 0,
        'data': [{
            'name': r['name'],
            'title': r['title'],
            'timestamp': r['timestamp'],
            'data': r.get('data', {})
        } for r in reports]
    })

@app.route('/api/report/<strategy>')
def api_report(strategy):
    """API: 获取某个策略的最新报告"""
    report = get_latest_report(strategy)
    if report is None:
        return jsonify({'code': 1, 'msg': '报告不存在'})
    return jsonify({'code': 0, 'data': report})

@app.route('/health')
def health():
    """健康检查"""
    return jsonify({'status': 'ok', 'time': datetime.now().isoformat()})

@app.route('/strategy1')
def strategy1():
    """策略1：连续缩量震荡上行信号页面"""
    return render_template('strategy1.html')

@app.route('/api/strategy1')
def api_strategy1():
    """API: 获取策略1信号数据（读本地静态文件）"""
    try:
        # 读预生成的信号文件
        signal_file = '/var/www/all_signals.json'
        if os.path.exists(signal_file):
            with open(signal_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify({'code': 0, 'data': data, 'count': len(data)})
        else:
            return jsonify({'code': 1, 'msg': '信号文件不存在，请先运行扫描脚本'})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})

@app.route('/preload/<symbol>')
def preload(symbol):
    """预加载K线图到本地"""
    symbol = symbol.replace('_', '/')
    if not symbol.endswith(':USDT'):
        symbol = symbol + '/USDT:USDT'
    
    # 先检查是否有缓存
    cache_dir = os.path.join(os.path.dirname(__file__), 'static', 'charts')
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, f'{symbol.replace("/", "_")}.png')
    
    # 缓存1小时
    if os.path.exists(cache_file) and os.path.getmtime(cache_file) > time.time() - 3600:
        return send_file(cache_file, mimetype='image/png')
    
    try:
        exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        
        ohlcv_1h = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=24)
        ohlcv_4h = exchange.fetch_ohlcv(symbol, timeframe='4h', limit=40)
        
        if not ohlcv_1h:
            return "No data", 404
        
        df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # 4小时K线聚合成日K线
        df_4h_sorted = df_4h.sort_values('timestamp').reset_index(drop=True)
        daily_data = []
        for i in range(0, len(df_4h_sorted) - 3, 4):
            group = df_4h_sorted.iloc[i:i+4]
            daily_data.append({
                'open': group.iloc[0]['open'],
                'high': group['high'].max(),
                'low': group['low'].min(),
                'close': group.iloc[-1]['close'],
                'volume': group['volume'].sum()
            })
        df_daily = pd.DataFrame(daily_data).tail(10)
        df_4h_6 = df_4h.tail(6)
        
        plt.style.use('dark_background')
        fig = plt.figure(figsize=(18, 16))
        gs = fig.add_gridspec(3, 1, height_ratios=[1.2, 1, 0.8], hspace=0.25)
        ax1 = fig.add_subplot(gs[0])
        ax2 = fig.add_subplot(gs[1])
        ax3 = fig.add_subplot(gs[2])
        fig.patch.set_facecolor('#1a1a1a')
        
        # 1小时K线
        for i in range(len(df_1h)):
            o = df_1h.iloc[i]['open']
            h = df_1h.iloc[i]['high']
            l = df_1h.iloc[i]['low']
            c = df_1h.iloc[i]['close']
            color = '#00a854' if c >= o else '#eb3c3c'
            ax1.plot([i, i], [l, h], color=color, linewidth=1)
            width = 0.6
            if c >= o:
                ax1.bar([i], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
            else:
                ax1.bar([i], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
        ax1.set_facecolor('#1a1a1a')
        ax1.grid(True, alpha=0.2, color='#333')
        ax1.set_title(f'{symbol.replace(":USDT", "")} - 1H (24 candles)', fontsize=12, fontweight='bold', color='#fff', pad=10)
        ax1.tick_params(colors='#999', labelsize=8)
        ax1.set_xlim(-0.5, len(df_1h)-0.5)
        
        # 日K线
        for i in range(len(df_daily)):
            o = df_daily.iloc[i]['open']
            h = df_daily.iloc[i]['high']
            l = df_daily.iloc[i]['low']
            c = df_daily.iloc[i]['close']
            color = '#00a854' if c >= o else '#eb3c3c'
            ax2.plot([i, i], [l, h], color=color, linewidth=1.5)
            width = 0.5
            if c >= o:
                ax2.bar([i], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
            else:
                ax2.bar([i], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
        ax2.set_facecolor('#1a1a1a')
        ax2.grid(True, alpha=0.2, color='#333')
        ax2.set_title('Daily (from 4H, 10 candles ~10 days)', fontsize=11, color='#fff', pad=8)
        ax2.tick_params(colors='#999', labelsize=8)
        ax2.set_xlim(-0.5, len(df_daily)-0.5)
        
        # 4小时K线
        for i in range(len(df_4h_6)):
            o = df_4h_6.iloc[i]['open']
            h = df_4h_6.iloc[i]['high']
            l = df_4h_6.iloc[i]['low']
            c = df_4h_6.iloc[i]['close']
            color = '#00a854' if c >= o else '#eb3c3c'
            ax3.plot([i, i], [l, h], color=color, linewidth=2)
            width = 0.5
            if c >= o:
                ax3.bar([i], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
            else:
                ax3.bar([i], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
        ax3.set_facecolor('#1a1a1a')
        ax3.grid(True, alpha=0.2, color='#333')
        ax3.set_title('4H (6 candles ~1 day)', fontsize=11, color='#fff', pad=8)
        ax3.tick_params(colors='#999', labelsize=8)
        ax3.set_xlim(-0.5, len(df_4h_6)-0.5)
        
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=130, facecolor='#1a1a1a')
        buf.seek(0)
        plt.close()
        
        # 保存缓存
        with open(cache_file, 'wb') as f:
            f.write(buf.getvalue())
        
        return send_file(buf, mimetype='image/png')
        
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/chart/<symbol>')
def chart(symbol):
    """生成K线图表 - 蜡烛图+黑色背景，三个周期"""
    symbol = symbol.replace('_', '/')
    if not symbol.endswith(':USDT'):
        symbol = symbol + '/USDT:USDT'
    
    try:
        # 获取K线数据
        exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        
        # 获取K线
        ohlcv_1h = exchange.fetch_ohlcv(symbol, timeframe='1h', limit=24)   # 24小时
        ohlcv_4h = exchange.fetch_ohlcv(symbol, timeframe='4h', limit=40)  # 40个4小时K线
        
        if not ohlcv_1h:
            return "No data", 404
        
        df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # 4小时K线聚合成日K线 (每4根聚合为1根日K)
        df_4h_sorted = df_4h.sort_values('timestamp').reset_index(drop=True)
        daily_data = []
        for i in range(0, len(df_4h_sorted) - 3, 4):
            group = df_4h_sorted.iloc[i:i+4]
            daily_data.append({
                'open': group.iloc[0]['open'],
                'high': group['high'].max(),
                'low': group['low'].min(),
                'close': group.iloc[-1]['close'],
                'volume': group['volume'].sum()
            })
        df_daily = pd.DataFrame(daily_data).tail(10)  # 取最后10根日K
        
        # 取最后6根4h K线
        df_4h_6 = df_4h.tail(6)
        
        # 黑色背景
        plt.style.use('dark_background')
        fig = plt.figure(figsize=(18, 16))
        
        # 创建网格
        gs = fig.add_gridspec(3, 1, height_ratios=[1.2, 1, 0.8], hspace=0.25)
        
        ax1 = fig.add_subplot(gs[0])  # 1小时K线
        ax2 = fig.add_subplot(gs[1])  # 日K线(聚合)
        ax3 = fig.add_subplot(gs[2])  # 4小时K线(6个)
        
        fig.patch.set_facecolor('#1a1a1a')
        
        # 1小时K线 (24个)
        for i in range(len(df_1h)):
            o = df_1h.iloc[i]['open']
            h = df_1h.iloc[i]['high']
            l = df_1h.iloc[i]['low']
            c = df_1h.iloc[i]['close']
            color = '#00a854' if c >= o else '#eb3c3c'
            ax1.plot([i, i], [l, h], color=color, linewidth=1)
            width = 0.6
            if c >= o:
                ax1.bar([i], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
            else:
                ax1.bar([i], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
        
        ax1.set_facecolor('#1a1a1a')
        ax1.grid(True, alpha=0.2, color='#333')
        ax1.set_title(f'{symbol.replace(":USDT", "")} - 1H (24 candles)', fontsize=12, fontweight='bold', color='#fff', pad=10)
        ax1.tick_params(colors='#999', labelsize=8)
        ax1.set_xlim(-0.5, len(df_1h)-0.5)
        
        # 日K线 (10个，由4H聚合)
        for i in range(len(df_daily)):
            o = df_daily.iloc[i]['open']
            h = df_daily.iloc[i]['high']
            l = df_daily.iloc[i]['low']
            c = df_daily.iloc[i]['close']
            color = '#00a854' if c >= o else '#eb3c3c'
            ax2.plot([i, i], [l, h], color=color, linewidth=1.5)
            width = 0.5
            if c >= o:
                ax2.bar([i], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
            else:
                ax2.bar([i], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
        
        ax2.set_facecolor('#1a1a1a')
        ax2.grid(True, alpha=0.2, color='#333')
        ax2.set_title('Daily (from 4H, 10 candles ~10 days)', fontsize=11, color='#fff', pad=8)
        ax2.tick_params(colors='#999', labelsize=8)
        ax2.set_xlim(-0.5, len(df_daily)-0.5)
        
        # 4小时K线 (6个)
        for i in range(len(df_4h_6)):
            o = df_4h_6.iloc[i]['open']
            h = df_4h_6.iloc[i]['high']
            l = df_4h_6.iloc[i]['low']
            c = df_4h_6.iloc[i]['close']
            color = '#00a854' if c >= o else '#eb3c3c'
            ax3.plot([i, i], [l, h], color=color, linewidth=2)
            width = 0.5
            if c >= o:
                ax3.bar([i], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
            else:
                ax3.bar([i], [o - c], width=width, bottom=[c], color=color, edgecolor=color)
        
        ax3.set_facecolor('#1a1a1a')
        ax3.grid(True, alpha=0.2, color='#333')
        ax3.set_title('4H (6 candles ~1 day)', fontsize=11, color='#fff', pad=8)
        ax3.tick_params(colors='#999', labelsize=8)
        ax3.set_xlim(-0.5, len(df_4h_6)-0.5)
        
        plt.tight_layout()
        
        # 转为图片
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=130, facecolor='#1a1a1a')
        buf.seek(0)
        plt.close()
        
        return send_file(buf, mimetype='image/png')
        
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/triple-chart/<symbol>')
def triple_chart(symbol):
    """生成三合一K线图表 (1H/4H/D)"""
    from utils.chart_generator import ChartGenerator
    
    symbol = symbol.replace('_', '/')
    if symbol.endswith('USDT'):
        symbol = symbol.replace('USDT', '')
    
    try:
        img_data = ChartGenerator.generate_triple_chart(symbol)
        if img_data is None:
            return "No data", 404
        
        buf = io.BytesIO(img_data)
        buf.seek(0)
        return send_file(buf, mimetype='image/png')
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/api/account')
def api_account():
    """获取币安账户余额和持仓信息"""
    from utils.binance_account import get_account_info
    
    try:
        info = get_account_info()
        return jsonify({'code': 0, 'data': info})
    except Exception as e:
        return jsonify({'code': -1, 'msg': str(e)})

if __name__ == '__main__':
    print(f"启动Web服务: http://{config.WEB_HOST}:{config.WEB_PORT}")
    app.run(host=config.WEB_HOST, port=config.WEB_PORT, debug=False)
