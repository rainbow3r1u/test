#!/usr/bin/env python3
"""
测试WebSocket实时K线推送
"""
import sys
import time
sys.path.insert(0, '/root/crypto-scanner/source')

from utils.websocket_manager import ws_manager
from utils.binance_account import BinanceAccount

print("=== 测试WebSocket实时K线 ===\n")

print("1. 获取当前持仓...")
positions = BinanceAccount.get_positions(use_cache=False)
print(f"   持仓数量: {len(positions)}")

if positions:
    symbols = [p['symbol'] for p in positions]
    print(f"   持仓币种: {symbols}")
    
    print("\n2. 订阅持仓币种...")
    for symbol in symbols:
        ws_manager.subscribe_symbol(symbol)
    
    print(f"   当前订阅: {ws_manager.subscriptions}")
    print(f"   运行状态: {ws_manager.running}")
    
    print("\n3. 等待实时数据推送 (10秒)...")
    time.sleep(10)
    
    print("\n4. 检查缓存数据...")
    for symbol in symbols:
        cache = ws_manager.get_kline_cache(symbol)
        print(f"   {symbol}: {len(cache)} 条K线")
    
    print("\n5. 停止WebSocket...")
    ws_manager.stop()
    print("   已停止")
else:
    print("   当前没有持仓，无法测试")
