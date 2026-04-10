#!/bin/bash
# 每小时自动更新所有策略数据
# 用法: 添加到crontab: 2 * * * * ubuntu /home/ubuntu/crypto-scanner/scripts/hourly_update_home.sh

cd /home/ubuntu/crypto-scanner

LOG_FILE="/home/ubuntu/crypto-scanner/logs/hourly_update.log"
mkdir -p /home/ubuntu/crypto-scanner/logs

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

# 4. 更新数据文件
python3 << 'EOF' >> $LOG_FILE 2>&1
import json
import glob
from pathlib import Path

output_dir = Path('/home/ubuntu/crypto-scanner/output')
data_dir = Path('/home/ubuntu/crypto-scanner/data')

# 更新strategy1信号文件
files = list(output_dir.glob('strategy1_*.json'))
if files:
    latest = max(files, key=lambda x: x.stat().st_mtime)
    with open(latest, 'r') as f:
        data = json.load(f)
    
    items = data.get('items', [])
    by_symbol = {}
    for item in items:
        sym = item.get('symbol', '')
        end_h = item.get('endHour', 0)
        if sym not in by_symbol or end_h > by_symbol[sym].get('endHour', 0):
            by_symbol[sym] = item
    
    unique_items = list(by_symbol.values())
    unique_items.sort(key=lambda x: (-x.get('endHour', 0), -x.get('vol', 0)))
    
    signal_file = data_dir / 'all_signals.json'
    with open(signal_file, 'w') as f:
        json.dump(unique_items, f, ensure_ascii=False)
    
    print(f'更新strategy1数据: {len(unique_items)}个信号 -> {signal_file}')

print('数据更新完成')
EOF

echo "$(date '+%Y-%m-%d %H:%M:%S') 记录连续6小时信号..." >> $LOG_FILE
python3 scripts/record_six_hour.py >> $LOG_FILE 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') 运行暴涨监控..." >> $LOG_FILE
python3 scripts/monitor_surge.py >> $LOG_FILE 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') 每小时更新完成" >> $LOG_FILE
echo "========================================" >> $LOG_FILE
