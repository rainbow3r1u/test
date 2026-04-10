"""
WebSocket管理器 - 订阅持仓币种的实时K线数据
"""
import asyncio
import json
import threading
from typing import Set, Dict, Optional
import websockets
from utils.logger import get_logger

logger = get_logger('websocket_manager')


class BinanceWebSocketManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.subscriptions: Set[str] = set()
            self.sub_lock = threading.Lock()
            self.ws = None
            self.loop = None
            self.thread = None
            self.running = False
            self.socketio = None
            self.kline_cache: Dict[str, list] = {}
    
    def set_socketio(self, socketio):
        self.socketio = socketio
    
    def subscribe_symbol(self, symbol: str):
        symbol = symbol.upper()
        should_start = False
        with self.sub_lock:
            if symbol not in self.subscriptions:
                self.subscriptions.add(symbol)
                logger.info(f"订阅币种: {symbol}, 当前订阅数: {len(self.subscriptions)}")
                should_start = not self.running
        if should_start:
            self.start()
    
    def unsubscribe_symbol(self, symbol: str):
        symbol = symbol.upper()
        should_stop = False
        with self.sub_lock:
            if symbol in self.subscriptions:
                self.subscriptions.discard(symbol)
                logger.info(f"取消订阅: {symbol}, 剩余订阅数: {len(self.subscriptions)}")
                should_stop = self.running and len(self.subscriptions) == 0
        if should_stop:
            self.stop()
    
    def update_subscriptions(self, symbols: list):
        new_symbols = set(s.upper() for s in symbols)
        with self.sub_lock:
            old_symbols = set(self.subscriptions)
            to_add = new_symbols - old_symbols
            to_remove = old_symbols - new_symbols
            self.subscriptions = new_symbols
            running = self.running

        logger.info(f"更新订阅: 新增{len(to_add)}, 移除{len(to_remove)}, 当前{len(new_symbols)}")

        if running and len(new_symbols) == 0:
            self.stop()
            return
        if running and (to_add or to_remove):
            self.restart()
            return
        if (not running) and len(new_symbols) > 0:
            self.start()
    
    def start(self):
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        logger.info("WebSocket管理器启动")

    def restart(self):
        self.stop()
        with self.sub_lock:
            has_subscriptions = len(self.subscriptions) > 0
        if has_subscriptions:
            self.start()
    
    def stop(self):
        self.running = False
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)
        if self.thread:
            self.thread.join(timeout=2)
        self.ws = None
        self.loop = None
        self.thread = None
        logger.info("WebSocket管理器停止")
    
    def _run_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._connect_binance())
        except Exception as e:
            logger.error(f"WebSocket循环异常: {e}")
        finally:
            self.running = False
            self.loop.close()
    
    async def _connect_binance(self):
        with self.sub_lock:
            subscriptions = set(self.subscriptions)
        if not subscriptions:
            logger.warning("没有订阅任何币种，等待订阅...")
            await asyncio.sleep(1)
            return
        
        streams = [f"{symbol.lower()}@kline_1h" for symbol in subscriptions]
        stream_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
        
        logger.info(f"连接币安WebSocket: {len(streams)} 个数据流")
        
        try:
            async with websockets.connect(stream_url, ping_interval=20, ping_timeout=10) as ws:
                self.ws = ws
                logger.info("WebSocket连接成功")
                
                async for message in ws:
                    if not self.running:
                        break
                    
                    try:
                        data = json.loads(message)
                        await self._handle_message(data)
                    except Exception as e:
                        logger.error(f"处理消息失败: {e}")
        
        except Exception as e:
            logger.error(f"WebSocket连接失败: {e}")
            if self.running:
                logger.info("5秒后重连...")
                await asyncio.sleep(5)
                await self._connect_binance()
    
    async def _handle_message(self, data: dict):
        if 'stream' not in data or 'data' not in data:
            return
        
        kline_data = data['data']
        kline = kline_data.get('k', {})
        
        if not kline:
            return
        
        symbol = kline.get('s', '')
        is_closed = kline.get('x', False)
        
        kline_info = {
            'symbol': symbol,
            'interval': kline.get('i', '1h'),
            'openTime': kline.get('t', 0),
            'closeTime': kline.get('T', 0),
            'open': float(kline.get('o', 0)),
            'high': float(kline.get('h', 0)),
            'low': float(kline.get('l', 0)),
            'close': float(kline.get('c', 0)),
            'volume': float(kline.get('v', 0)),
            'quoteVolume': float(kline.get('q', 0)),
            'isClosed': is_closed,
            'trades': kline.get('n', 0)
        }
        
        if symbol not in self.kline_cache:
            self.kline_cache[symbol] = []
        
        if is_closed:
            self.kline_cache[symbol].append(kline_info)
            if len(self.kline_cache[symbol]) > 100:
                self.kline_cache[symbol] = self.kline_cache[symbol][-100:]
        
        if self.socketio:
            self.socketio.emit('kline_update', kline_info, namespace='/realtime')
    
    def get_kline_cache(self, symbol: str) -> list:
        return self.kline_cache.get(symbol.upper(), [])


ws_manager = BinanceWebSocketManager()
