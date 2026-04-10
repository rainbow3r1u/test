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
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from flask import Flask, render_template, jsonify, Response, send_file, request
from flask_socketio import SocketIO, emit
from flask_compress import Compress

import sys
sys.path.insert(0, str(Path(__file__).parent))
from configs import config
from utils.logger import get_logger
from core.data_loader import DataLoader
from core.chart_generator import ChartGenerator
from core.database import Database

logger = get_logger('web')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'crypto-scanner-secret-key-2024'
Compress(app)

@app.after_request
def add_no_cache_headers(response):
    if request.path.startswith('/api/'):
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response
app.template_folder = str(Path(__file__).parent / 'templates')
app.static_folder = str(config.STATIC_DIR)

socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

from utils.websocket_manager import ws_manager
ws_manager.set_socketio(socketio)


# 简单的内存缓存机制，避免频繁读取巨型JSON
_report_cache = {}
_report_mtime = {}

def get_latest_report(strategy_name: str) -> Dict[str, Any]:
    # 优先查找通用名称的最新报告
    target_file = str(config.OUTPUT_DIR / f'{strategy_name}.json')
    if not os.path.exists(target_file):
        pattern = str(config.OUTPUT_DIR / f'{strategy_name}_*.json')
        files = glob.glob(pattern)
        if not files:
            return None
        target_file = max(files, key=os.path.getmtime)
        
    mtime = os.path.getmtime(target_file)
    if strategy_name in _report_cache and _report_mtime.get(strategy_name) == mtime:
        return _report_cache[strategy_name]
        
    with open(target_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        _report_cache[strategy_name] = data
        _report_mtime[strategy_name] = mtime
        return data


def get_all_reports() -> List[Dict[str, Any]]:
    # 复用 get_latest_report 的缓存，避免读取和遍历所有历史文件
    reports = []
    strategies = ['strategy1', 'strategy1_pro', 'arc_bottom']
    for st in strategies:
        data = get_latest_report(st)
        if data:
            reports.append({
                'name': data.get('strategy_name', st),
                'title': data.get('title', '未命名'),
                'timestamp': data.get('timestamp', ''),
                'ts': datetime.strptime(data.get('timestamp', '2000-01-01 00:00:00'), '%Y-%m-%d %H:%M:%S') if data.get('timestamp') else datetime.min,
                'summary': data.get('summary', {}),
                'data': data
            })
            
    reports.sort(key=lambda x: x['ts'], reverse=True)
    return [{'name': r['name'], 'title': r['title'], 'timestamp': r['timestamp'], 'summary': r['summary'], 'data': r['data']} for r in reports]


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
    if view == 'strategy1_pro':
        s1_file = '/var/www/all_signals_pro.json'
    elif view == 'arc_bottom':
        s1_file = '/var/www/all_signals_arc.json'
    else:
        s1_file = '/var/www/all_signals.json'
        
    if not os.path.exists(s1_file):
        # Fallback to local data dir if /var/www doesn't exist
        fallback_map = {
            'strategy1_pro': 'all_signals_pro.json',
            'arc_bottom': 'all_signals_arc.json'
        }
        s1_file = str(config.DATA_DIR / fallback_map.get(view, 'all_signals.json'))
        
    if os.path.exists(s1_file):
        try:
            with open(s1_file, 'r', encoding='utf-8') as f:
                s1_data = json.load(f)
                # 仅保留所需精简字段，减轻HTML负载
                s1_data = [{'symbol': item.get('symbol')} for item in s1_data]
        except:
            pass
    
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


@app.route('/api/<strategy>')
def api_strategy(strategy):
    if strategy not in ['strategy1', 'strategy1_pro', 'arc_bottom']:
        return jsonify({'code': 1, 'msg': '无效的策略'})
        
    try:
        file_map = {
            'strategy1': '/var/www/all_signals.json',
            'strategy1_pro': '/var/www/all_signals_pro.json',
            'arc_bottom': '/var/www/all_signals_arc.json'
        }
        signal_file = file_map.get(strategy)
        if not os.path.exists(signal_file):
            fallback_map = {
                'strategy1': 'all_signals.json',
                'strategy1_pro': 'all_signals_pro.json',
                'arc_bottom': 'all_signals_arc.json'
            }
            signal_file = str(config.DATA_DIR / fallback_map.get(strategy))

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
        is_live = request.args.get('live', '0') == '1'
        
        if is_live:
            chart_data = ChartGenerator.generate_triple_chart_live(symbol)
        else:
            cutoff_str = request.args.get('cutoff')
            cutoff = None
            if cutoff_str:
                try:
                    cutoff = pd.to_datetime(cutoff_str)
                except Exception:
                    pass
            chart_data = ChartGenerator.generate_triple_chart_from_cos(symbol, cutoff=cutoff)
            
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



is_scanning = False
scan_lock = threading.Lock()

@app.route('/api/strategy1/scan', methods=['POST'])
def api_strategy1_scan():
    global is_scanning
    with scan_lock:
        if is_scanning:
            return jsonify({'code': 1, 'msg': '扫描正在进行中，请稍后再试'})
        is_scanning = True
        
    strategy_id = request.args.get('strategy', 'strategy1').strip()
    custom_params = request.get_json(silent=True) or {}
    
    def run_scan():
        global is_scanning
        try:
            if strategy_id == 'strategy1_pro':
                from strategies.strategy1_pro import Strategy1Pro
                strategy = Strategy1Pro(**custom_params)
                target_report = str(config.OUTPUT_DIR / 'strategy1_pro.json')
                target_signals = '/var/www/all_signals_pro.json'
                fallback_signals = str(config.DATA_DIR / 'all_signals_pro.json')
            elif strategy_id == 'arc_bottom':
                from strategies.arc_bottom import ArcBottomStrategy
                strategy = ArcBottomStrategy(**custom_params)
                target_report = str(config.OUTPUT_DIR / 'arc_bottom.json')
                target_signals = '/var/www/all_signals_arc.json'
                fallback_signals = str(config.DATA_DIR / 'all_signals_arc.json')
            else:
                from strategies.strategy1 import Strategy1
                strategy = Strategy1(**custom_params)
                target_report = str(config.OUTPUT_DIR / 'strategy1.json')
                target_signals = '/var/www/all_signals.json'
                fallback_signals = str(config.DATA_DIR / 'all_signals.json')
                
            report = strategy.run(generate_charts=False)
            
            # 手动更新前端需要的文件
            if report:
                report_dict = report.to_dict()
                with open(target_report, 'w', encoding='utf-8') as f:
                    json.dump(report_dict, f, ensure_ascii=False, indent=2)
                
                signals = [item.to_dict() if hasattr(item, 'to_dict') else item for item in report.items]
                try:
                    with open(target_signals, 'w', encoding='utf-8') as f:
                        json.dump(signals, f, ensure_ascii=False, indent=2)
                except Exception:
                    with open(fallback_signals, 'w', encoding='utf-8') as f:
                        json.dump(signals, f, ensure_ascii=False, indent=2)
                        
        except Exception as e:
            logger.exception(f"扫描执行失败: {e}")
        finally:
            with scan_lock:
                is_scanning = False

    thread = threading.Thread(target=run_scan)
    thread.start()
    return jsonify({'code': 0, 'msg': '触发策略实时扫描（后台运行）'})

@app.route('/api/status')
def api_status():
    global is_scanning
    return jsonify({'code': 0, 'data': {'is_scanning': is_scanning}})

@app.route('/api/strategy1/debug')
def api_strategy1_debug():
    symbol = request.args.get('symbol', '').strip().upper()
    strategy_id = request.args.get('strategy', 'strategy1').strip()
    
    if not symbol:
        return jsonify({'code': 1, 'msg': '缺少 symbol 参数'})
    if not symbol.endswith('USDT'):
        symbol += 'USDT'
        
    try:
        report_data = get_latest_report(strategy_id)
        if not report_data:
            return jsonify({'code': 1, 'msg': f'没有找到 {strategy_id} 报告数据'})
            
        report_source = f'api/report/{strategy_id}'
        report_time = report_data.get('timestamp', '')
        
        all_symbols_bars = report_data.get('metadata', {}).get('all_symbols_bars', [])
        target = None
        for item in all_symbols_bars:
            if item['symbol'] == symbol:
                target = item
                break
                
        if not target:
            return jsonify({'code': 1, 'msg': f'在最新报告({report_time})中未找到 {symbol} 的扫描数据，可能是日线不满足条件（前两日非阳线或涨幅过大）。'})
            
        bars = target.get('bars', [])
        details = []
        consecutive = 0
        eliminated_at = None
        reason = ""
        
        is_pro = (strategy_id == 'strategy1_pro')
        
        for idx, bar in enumerate(bars, 1):
            t = bar['timestamp']
            o = float(bar['open'])
            c = float(bar['close'])
            h = float(bar['high'])
            l = float(bar['low'])
            v = float(bar['volume'])
            
            time_str = t[5:16]
            
            if c < o:
                eliminated_at = time_str
                reason = f"{time_str} 为阴线 (连涨在倒数第{idx}个小时中断)"
                details.append({
                    'step': f"往前推第{idx}小时",
                    'time': time_str,
                    'status': '淘汰',
                    'desc': reason
                })
                break
                
            if is_pro:
                body = c - o
                total = h - l
                body_ratio = body / total if total > 0 else 0
                if body_ratio < 0.4:
                    eliminated_at = time_str
                    reason = f"{time_str} 实体比例不足 ({body_ratio:.1%} < 40%)"
                    details.append({'step': f"往前推第{idx}小时", 'time': time_str, 'status': '淘汰', 'desc': reason})
                    break
                    
                gain = (c - o) / o
                if gain > 0.08:
                    eliminated_at = time_str
                    reason = f"{time_str} 单根涨幅过大 ({gain:.1%} > 8%)"
                    details.append({'step': f"往前推第{idx}小时", 'time': time_str, 'status': '淘汰', 'desc': reason})
                    break
            
            consecutive += 1
            
            if idx == 1:
                desc = f"倒数第1根(最新)为阳线，low={l}"
            else:
                desc = f"往前推第{idx}小时为阳线，low={l}"
                
            details.append({
                'step': f"往前推第{idx}小时",
                'time': time_str,
                'status': '通过',
                'desc': desc
            })
            
        is_signal = consecutive >= 1
        max_passed = f"连续{consecutive}小时连涨" if consecutive > 0 else "0"
        
        summary = f"报告时间: {report_time} | 来源: {report_source}\\n"
        summary += f"最高通过: {max_passed} | 是否成信号: {'是' if is_signal and not eliminated_at else '否'}\\n"
        if eliminated_at:
            summary += f"原因: {reason}"
            
        return jsonify({
            'code': 0,
            'data': {
                'symbol': symbol,
                'summary': summary,
                'details': details
            }
        })
        
    except Exception as e:
        logger.error(f"调试接口出错: {e}")
        return jsonify({'code': 1, 'msg': str(e)})

@app.route('/api/arc_bottom/debug')
def api_arc_bottom_debug():
    symbol = request.args.get('symbol', '').strip().upper()
    if not symbol:
        return jsonify({'code': 1, 'msg': '缺少 symbol 参数'})
    if not symbol.endswith('USDT'):
        symbol += 'USDT'
        
    try:
        report_data = get_latest_report('arc_bottom')
        if not report_data:
            return jsonify({'code': 1, 'msg': '没有找到 arc_bottom 报告数据'})
            
        report_time = report_data.get('timestamp', '')
        
        # 实时拉取最新数据，替代过时的 all_symbols_bars
        from core.data_loader import DataLoader
        import pandas as pd
        loader = DataLoader()
        df_all = loader.get_klines()
        df = df_all[df_all['symbol'] == symbol].copy()
        if df.empty:
            return jsonify({'code': 1, 'msg': f'未找到 {symbol} 的数据。'})
            
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        def get_param(name, default, type_func):
            val = request.args.get(name)
            if val is not None and val != '': 
                try:
                    return type_func(val)
                except ValueError:
                    pass
            return report_data.get('summary', {}).get('params', {}).get(name, default)
            
        PARAMS = {
            'lookback_hours': 120,
            'min_drop_pct': get_param('min_drop_pct', 0.01, float),
            'max_drop_pct': get_param('max_drop_pct', 0.10, float),
            'left_max_bulls': get_param('left_max_bulls', 1, int),
            'box_max_amp': get_param('box_max_amp', 0.05, float),
            'box_min_bars': get_param('box_min_bars', 3, int),
            'right_bull_bars': get_param('right_bull_bars', 2, int)
        }
        
        recent = df.iloc[-PARAMS['lookback_hours']:].reset_index(drop=True)
        n = len(recent)
        if n < PARAMS['box_min_bars'] + PARAMS['right_bull_bars']: 
            return jsonify({'code': 1, 'msg': f'{symbol} 历史K线不足'})
            
        details = []
        summary = f"报告时间: {report_time} | 来源: api/report/arc_bottom\n"
        
        right_bars = recent.iloc[-PARAMS['right_bull_bars']:]
        is_breakout = all(right_bars['close'] > right_bars['open'])
        
        if not is_breakout:
            details.append({'step': '右侧突破', 'time': f"{right_bars.iloc[0]['timestamp'].strftime('%H:%M')}~{right_bars.iloc[-1]['timestamp'].strftime('%H:%M')}", 'status': '淘汰', 'desc': f"最新 {PARAMS['right_bull_bars']} 小时未能全部收阳线"})
            summary += f"状态: 未通过 | 原因: 未满足右侧突破条件\n"
            return jsonify({'code': 0, 'data': {'symbol': symbol, 'summary': summary, 'details': details}})
            
        details.append({'step': '右侧突破', 'time': f"{right_bars.iloc[0]['timestamp'].strftime('%H:%M')}~{right_bars.iloc[-1]['timestamp'].strftime('%H:%M')}", 'status': '通过', 'desc': f"最新 {PARAMS['right_bull_bars']} 小时连阳突破"})
        
        found = False
        for box_len in range(PARAMS['box_min_bars'], 24):
            box_end_idx = n - PARAMS['right_bull_bars'] - 1
            box_start_idx = box_end_idx - box_len + 1
            if box_start_idx < 0: break
            
            box_segment = recent.iloc[box_start_idx : box_end_idx + 1]
            box_high = box_segment['high'].max()
            box_low = box_segment['low'].min()
            box_amp = (box_high - box_low) / box_low if box_low > 0 else 0
            
            if box_amp > PARAMS['box_max_amp']:
                continue
                
            for left_len in range(3, 24):
                left_start_idx = box_start_idx - left_len
                left_end_idx = box_start_idx - 1
                if left_start_idx < 0: break
                
                left_segment = recent.iloc[left_start_idx : left_end_idx + 1]
                bullish_count = sum(left_segment['close'] > left_segment['open'])
                if bullish_count > PARAMS['left_max_bulls']:
                    continue
                    
                start_body_high = max(left_segment.iloc[0]['open'], left_segment.iloc[0]['close'])
                end_body_low = min(left_segment.iloc[-1]['open'], left_segment.iloc[-1]['close'])
                if start_body_high <= 0: continue
                
                drop_pct = (start_body_high - end_body_low) / start_body_high
                
                if PARAMS['min_drop_pct'] <= drop_pct <= PARAMS['max_drop_pct']:
                    details.append({'step': '底部盘整', 'time': f"{box_segment.iloc[0]['timestamp'].strftime('%H:%M')}~{box_segment.iloc[-1]['timestamp'].strftime('%H:%M')}", 'status': '通过', 'desc': f"盘整 {box_len} 小时, 振幅 {box_amp*100:.2f}%"})
                    details.append({'step': '左侧下跌', 'time': f"{left_segment.iloc[0]['timestamp'].strftime('%H:%M')}~{left_segment.iloc[-1]['timestamp'].strftime('%H:%M')}", 'status': '通过', 'desc': f"下跌 {left_len} 小时, 跌幅 {drop_pct*100:.2f}%, 反抽 {bullish_count} 根"})
                    summary += f"状态: 完全通过 | 满足圆弧底形态\n"
                    found = True
                    break
            if found: break
            
        if not found:
            details.append({'step': '左侧及盘整', 'time': '-', 'status': '淘汰', 'desc': "虽有突破，但未能找到匹配的 [左侧下跌+底部箱体] 形态（振幅过大或跌幅不符）"})
            summary += f"状态: 未通过 | 原因: 左侧及盘整形态不满足条件\n"
            
        return jsonify({'code': 0, 'data': {'symbol': symbol, 'summary': summary, 'details': details}})
        
    except Exception as e:
        logger.exception(f"圆弧底调试接口出错: {e}")
        return jsonify({'code': 1, 'msg': str(e)})


if __name__ == '__main__':
    logger.info(f"启动Web服务: http://{config.WEB_HOST}:{config.WEB_PORT}")
    socketio.run(app, host=config.WEB_HOST, port=5002, debug=False, allow_unsafe_werkzeug=True)
