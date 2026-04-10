#!/bin/bash
# 每小时自动更新所有策略数据
# 用法: 添加到crontab: 2 * * * * root /root/crypto-scanner/source/scripts/hourly_update.sh

cd /root/crypto-scanner/source

LOG_FILE="/var/log/crypto_scanner.log"
echo "========================================" >> $LOG_FILE
echo "$(date '+%Y-%m-%d %H:%M:%S') 开始每小时更新" >> $LOG_FILE

# 1. 采集最新K线数据
echo "$(date '+%Y-%m-%d %H:%M:%S') 采集K线数据..." >> $LOG_FILE
python3 main.py collect >> $LOG_FILE 2>&1

# 2. 清除缓存
python3 -c "from utils.data_loader import DataLoader; DataLoader.clear_cache()" >> $LOG_FILE 2>&1

# 3. 运行所有策略
echo "$(date '+%Y-%m-%d %H:%M:%S') 运行所有策略..." >> $LOG_FILE
python3 main.py all >> $LOG_FILE 2>&1

# 4. 更新数据文件并预生成图表缓存
python3 << 'EOF' >> $LOG_FILE 2>&1
import json
import glob
import shutil
from utils.chart_generator import ChartGenerator

# 更新strategy1 - 同时更新完整报告和信号列表
files = glob.glob('output/strategy1_*.json')
if files:
    latest = max(files)
    with open(latest, 'r') as f:
        data = json.load(f)
    
    # 复制完整报告到 /var/www/strategy1.json
    shutil.copy(latest, '/var/www/strategy1.json')
    print(f'更新strategy1完整报告: {latest}')
    
    # 提取信号列表到 all_signals.json
    items = data.get('items', [])
    by_symbol = {}
    for item in items:
        sym = item['symbol']
        end_h = item.get('endHour', 0)
        if sym not in by_symbol or end_h > by_symbol[sym]['endHour']:
            by_symbol[sym] = item
    
    unique_items = list(by_symbol.values())
    unique_items.sort(key=lambda x: (-x.get('endHour', 0), -x.get('vol', 0)))
    
    with open('/var/www/all_signals.json', 'w') as f:
        json.dump(unique_items, f, ensure_ascii=False)
    
    print(f'更新strategy1数据: {len(unique_items)}个信号')

# 收集所有策略的币种
all_symbols = set()

for strategy in ['strategy1', 'coin_quality', 'bollinger', 'deepseek']:
    files = glob.glob(f'output/{strategy}_*.json')
    if files:
        latest = max(files)
        with open(latest, 'r') as f:
            data = json.load(f)
        
        items = data.get('items', [])
        for item in items:
            sym = item.get('symbol', '')
            if sym:
                all_symbols.add(sym)

# 预生成图表缓存
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
