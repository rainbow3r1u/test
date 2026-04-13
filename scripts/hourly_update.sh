#!/bin/bash
# 每小时自动更新所有策略数据
# 用法: 添加到crontab: 2 * * * * root /root/crypto-scanner/scripts/hourly_update.sh

cd /home/ubuntu/crypto-scanner

LOG_FILE="/home/ubuntu/crypto-scanner/logs/hourly_update.log"
echo "========================================" >> $LOG_FILE
echo "$(date '+%Y-%m-%d %H:%M:%S') 开始每小时更新" >> $LOG_FILE

# 1. 采集最新K线数据
echo "$(date '+%Y-%m-%d %H:%M:%S') 采集K线数据..." >> $LOG_FILE
python3 main.py collect >> $LOG_FILE 2>&1

# 2. 清除缓存
python3 -c "from core.data_loader import DataLoader; DataLoader.clear_cache()" >> $LOG_FILE 2>&1

# 3. 运行所有策略
echo "$(date '+%Y-%m-%d %H:%M:%S') 运行所有策略..." >> $LOG_FILE
python3 main.py all >> $LOG_FILE 2>&1

# 4. 更新数据文件并预生成图表缓存
python3 << 'EOF' >> $LOG_FILE 2>&1
import json
import glob
import shutil
from core.chart_generator import ChartGenerator

# 更新strategy1
files = [f for f in glob.glob('output/strategy1_*.json') if 'strategy1_pro' not in f]
if files:
    latest = max(files)
    with open(latest, 'r') as f:
        data = json.load(f)
    
    shutil.copy(latest, '/var/www/strategy1.json')
    
    items = data.get('items', [])
    by_symbol = {}
    for item in items:
        sym = item.get('symbol', '')
        end_h = item.get('endHour', 0)
        if sym not in by_symbol or end_h > by_symbol[sym].get('endHour', 0):
            by_symbol[sym] = item
    
    unique_items = list(by_symbol.values())
    unique_items.sort(key=lambda x: (-x.get('endHour', 0), -x.get('vol', 0)))
    
    with open('/var/www/all_signals.json', 'w') as f:
        json.dump(unique_items, f, ensure_ascii=False)

# 更新strategy1_pro
files_pro = glob.glob('output/strategy1_pro_*.json')
if files_pro:
    latest_pro = max(files_pro)
    with open(latest_pro, 'r') as f:
        data_pro = json.load(f)
    
    shutil.copy(latest_pro, '/var/www/strategy1_pro.json')
    
    items_pro = data_pro.get('items', [])
    by_symbol_pro = {}
    for item in items_pro:
        sym = item.get('symbol', '')
        end_h = item.get('endHour', 0)
        if sym not in by_symbol_pro or end_h > by_symbol_pro[sym].get('endHour', 0):
            by_symbol_pro[sym] = item
    
    unique_items_pro = list(by_symbol_pro.values())
    unique_items_pro.sort(key=lambda x: (-x.get('endHour', 0), -x.get('vol', 0)))
    
    with open('/var/www/all_signals_pro.json', 'w') as f:
        json.dump(unique_items_pro, f, ensure_ascii=False)

# 更新arc_bottom
files_arc = glob.glob('output/arc_bottom_*.json')
if files_arc:
    latest_arc = max(files_arc)
    with open(latest_arc, 'r') as f:
        data_arc = json.load(f)
    
    shutil.copy(latest_arc, '/var/www/arc_bottom.json')
    
    items_arc = data_arc.get('items', [])
    by_symbol_arc = {}
    for item in items_arc:
        sym = item.get('symbol', '')
        end_h = item.get('endHour', 0)
        if sym not in by_symbol_arc or end_h > by_symbol_arc[sym].get('endHour', 0):
            by_symbol_arc[sym] = item
    
    unique_items_arc = list(by_symbol_arc.values())
    unique_items_arc.sort(key=lambda x: (-x.get('drop_pct', 0)))
    
    with open('/var/www/all_signals_arc.json', 'w') as f:
        json.dump(unique_items_arc, f, ensure_ascii=False)

# 收集所有策略的币种并预生成图表
all_symbols = set()
for strategy in ['strategy1', 'strategy1_pro', 'arc_bottom', 'coin_quality', 'deepseek']:
    files = glob.glob(f'output/{strategy}_*.json')
    if not files:
        if strategy == 'deepseek':
            files = glob.glob('output/deepseek_strategy_*.json')
    if files:
        latest = max(files)
        with open(latest, 'r') as f:
            data = json.load(f)
        for item in data.get('items', []):
            sym = item.get('symbol', '')
            if sym:
                all_symbols.add(sym)

if all_symbols:
    symbols_list = list(all_symbols)
    print(f'预生成 {len(symbols_list)} 个币种的三合一图表缓存...')
    success = ChartGenerator.generate_triple_charts_batch(symbols_list)
    print(f'成功生成 {success} 个图表缓存')
EOF

echo "$(date '+%Y-%m-%d %H:%M:%S') 记录连续6小时信号..." >> $LOG_FILE
python3 scripts/record_six_hour.py >> $LOG_FILE 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') 运行暴涨监控..." >> $LOG_FILE
python3 scripts/monitor_surge.py >> $LOG_FILE 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') 每小时更新完成" >> $LOG_FILE
echo "========================================" >> $LOG_FILE

# 复制最新报告到 /var/www/
cp output/strategy1_$(date +%Y-%m-%d)_*.json /var/www/strategy1.json 2>/dev/null || true
cp output/strategy1_pro_$(date +%Y-%m-%d)_*.json /var/www/strategy1_pro.json 2>/dev/null || true
cp output/arc_bottom_$(date +%Y-%m-%d)_*.json /var/www/arc_bottom.json 2>/dev/null || true
