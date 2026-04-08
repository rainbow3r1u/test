#!/usr/bin/env python3
"""
加密货币策略扫描 - Web服务
重构版本：模块化、统一接口
"""
import os
import io
import json
import glob
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from flask import Flask, render_template, jsonify, Response, send_file, request
from flask_socketio import SocketIO, emit

import sys
sys.path.insert(0, str(Path(__file__).parent))
from configs import config
from utils.logger import get_logger
from utils.data_loader import DataLoader
from utils.chart_generator import ChartGenerator
from models import Database

logger = get_logger('web')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'crypto-scanner-secret-key-2024'
app.template_folder = str(Path(__file__).parent.parent / 'templates')
app.static_folder = str(config.STATIC_DIR)

socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

from utils.websocket_manager import ws_manager
ws_manager.set_socketio(socketio)


def get_latest_report(strategy_name: str) -> Dict[str, Any]:
    pattern = str(config.OUTPUT_DIR / f'{strategy_name}_*.json')
    files = glob.glob(pattern)
    if not files:
        return None
    latest = max(files, key=os.path.getmtime)
    with open(latest, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_all_reports() -> List[Dict[str, Any]]:
    pattern = str(config.OUTPUT_DIR / '*.json')
    files = glob.glob(pattern)
    
    latest_by_strategy = {}
    for f in files:
        try:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                name = data.get('strategy_name', os.path.basename(f))
                timestamp = data.get('timestamp', '')
                
                try:
                    ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                except:
                    ts = datetime.min
                
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
    reports.sort(key=lambda x: x.get('ts', datetime.min), reverse=True)
    return reports


@app.route('/')
def index():
    view = request.args.get('view', 'strategy1')
    reports = get_all_reports()
    
    latest_by_name = {}
    for r in reports:
        name = r.get('name', '')
        ts_str = r.get('timestamp', '')
        try:
            ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S') if ts_str else datetime.min
        except:
            ts = datetime.min
        if name not in latest_by_name or ts > latest_by_name[name]['ts']:
            latest_by_name[name] = {
                'name': name, 
                'timestamp': ts_str, 
                'title': r.get('title',''), 
                'ts': ts, 
                'data': r.get('data',{})
            }
    
    name_mapping = {
        'coin_quality': 'coin_quality',
        'deepseek': 'deepseek_strategy',
        'bollinger': 'bollinger_converge',
        'volume': 'volume_daily',
        'strategy1': 'strategy1'
    }
    strategy_names = ['coin_quality', 'deepseek', 'bollinger', 'volume']
    strategies = []
    for n in strategy_names:
        data = latest_by_name.get(n, {})
        strategies.append({
            'name': name_mapping.get(n, n),
            'title': data.get('title', ''),
            'timestamp': data.get('timestamp', ''),
            'data': data.get('data', {})
        })
    
    s1_data = []
    s1_file = '/var/www/all_signals.json'
    if os.path.exists(s1_file):
        with open(s1_file, 'r', encoding='utf-8') as f:
            s1_data = json.load(f)
    
    return render_template('index.html', strategies=strategies, s1_data=s1_data, current_view=view)


@app.route('/report/<strategy>')
def report(strategy):
    report = get_latest_report(strategy)
    if report is None:
        return "报告不存在", 404
    return jsonify(report)


@app.route('/api/reports')
def api_reports():
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
    report = get_latest_report(strategy)
    if report is None:
        return jsonify({'code': 1, 'msg': '报告不存在'})
    return jsonify({'code': 0, 'data': report})


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'time': datetime.now().isoformat()})


@app.route('/api/strategy1')
def api_strategy1():
    try:
        signal_file = '/var/www/all_signals.json'
        if os.path.exists(signal_file):
            with open(signal_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify({'code': 0, 'data': data, 'count': len(data)})
        else:
            return jsonify({'code': 1, 'msg': '信号文件不存在，请先运行扫描脚本'})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})


@app.route('/api/signals/<strategy>')
def api_signals(strategy):
    try:
        days = int(request.args.get('days', 7))
        signals = Database.get_latest_signals(strategy, limit=100)
        return jsonify({'code': 0, 'data': signals, 'count': len(signals)})
    except Exception as e:
        logger.error(f"获取信号失败: {e}")
        return jsonify({'code': 1, 'msg': str(e)})


@app.route('/chart/<symbol>')
def chart(symbol):
    try:
        interval = request.args.get('interval', '1h')
        limit = int(request.args.get('limit', 100))
        
        import ccxt
        exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        
        symbol_formatted = symbol.replace('USDT', '').replace('/USDT', '').replace(':USDT', '')
        full_symbol = f"{symbol_formatted}/USDT:USDT"
        
        ohlcv = exchange.fetch_ohlcv(full_symbol, timeframe=interval, limit=limit)
        
        if not ohlcv:
            return jsonify({'code': 1, 'msg': 'No data'})
        
        klines = []
        for row in ohlcv:
            klines.append({
                'timestamp': row[0],
                'open': row[1],
                'high': row[2],
                'low': row[3],
                'close': row[4],
                'volume': row[5]
            })
        
        return jsonify(klines)
    except Exception as e:
        logger.error(f"获取K线数据失败: {e}")
        return jsonify({'code': 1, 'msg': str(e)})


@app.route('/triple-chart/<symbol>')
def triple_chart(symbol):
    try:
        chart_data = ChartGenerator.generate_triple_chart(symbol)
        if chart_data is None:
            return "No data", 404
        
        buf = io.BytesIO(chart_data)
        buf.seek(0)
        return send_file(buf, mimetype='image/png')
    except Exception as e:
        logger.error(f"生成三合一图表失败: {e}")
        return f"Error: {str(e)}", 500


@app.route('/preload/<symbol>')
def preload(symbol):
    return chart(symbol)


@app.route('/api/account')
def api_account():
    from utils.binance_account import BinanceAccount
    
    try:
        BinanceAccount.clear_cache()
        info = BinanceAccount.get_account_info()
        return jsonify({'code': 0, 'data': info})
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        return jsonify({'code': -1, 'msg': str(e)})


@socketio.on('connect', namespace='/realtime')
def handle_connect():
    logger.info(f"客户端连接: {request.sid}")
    emit('connected', {'status': 'ok'})


@socketio.on('disconnect', namespace='/realtime')
def handle_disconnect():
    logger.info(f"客户端断开: {request.sid}")


@socketio.on('subscribe_positions', namespace='/realtime')
def handle_subscribe_positions(data):
    symbols = data.get('symbols', [])
    if symbols:
        ws_manager.update_subscriptions(symbols)
        logger.info(f"订阅持仓币种: {symbols}")
        emit('subscribed', {'symbols': symbols, 'count': len(symbols)})


@socketio.on('unsubscribe_all', namespace='/realtime')
def handle_unsubscribe_all():
    ws_manager.subscriptions.clear()
    logger.info("取消所有订阅")
    emit('unsubscribed', {'status': 'ok'})


@app.route('/api/realtime/status')
def realtime_status():
    return jsonify({
        'code': 0,
        'data': {
            'subscriptions': list(ws_manager.subscriptions),
            'count': len(ws_manager.subscriptions),
            'running': ws_manager.running
        }
    })


@app.route('/api/history/six-hour')
def api_history_six_hour():
    from utils.history_manager import HistoryManager
    
    try:
        days = int(request.args.get('days', 7))
        symbol = request.args.get('symbol', None)
        
        history = HistoryManager.get_history(days=days, symbol=symbol)
        stats = HistoryManager.get_stats()
        
        return jsonify({
            'code': 0,
            'data': {
                'history': history,
                'stats': stats
            }
        })
    except Exception as e:
        logger.error(f"获取历史记录失败: {e}")
        return jsonify({'code': -1, 'msg': str(e)})


@app.route('/api/surge/records')
def api_surge_records():
    from utils.surge_manager import SurgeManager
    
    try:
        days = int(request.args.get('days', 1))
        symbol = request.args.get('symbol', None)
        
        records = SurgeManager.get_records(days=days, symbol=symbol)
        stats = SurgeManager.get_today_stats()
        
        return jsonify({
            'code': 0,
            'data': {
                'records': records,
                'stats': stats
            }
        })
    except Exception as e:
        logger.error(f"获取暴涨记录失败: {e}")
        return jsonify({'code': -1, 'msg': str(e)})


@app.route('/api/surge/image/<symbol>/<timestamp>')
def api_surge_image(symbol, timestamp):
    from utils.surge_manager import SurgeManager
    
    try:
        image_data = SurgeManager.get_image(symbol, timestamp)
        
        if image_data:
            return send_file(
                io.BytesIO(image_data),
                mimetype='image/png'
            )
        else:
            return "Image not found", 404
    except Exception as e:
        logger.error(f"获取暴涨图片失败: {e}")
        return f"Error: {str(e)}", 500


if __name__ == '__main__':
    logger.info(f"启动Web服务: http://{config.WEB_HOST}:{config.WEB_PORT}")
    socketio.run(app, host=config.WEB_HOST, port=config.WEB_PORT, debug=False, allow_unsafe_werkzeug=True)
