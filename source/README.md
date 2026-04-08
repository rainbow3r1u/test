# 加密货币策略扫描平台 - 源代码

## 目录结构

```
source/
├── bian1k/                    # 数据采集层
│   └── bian1k.py              # 币安K线采集器
├── strategy1_scan/            # 策略1
│   └── strategy1_scan_fast.py # 稳步抬升信号扫描
├── flask_app/                 # Web服务层
│   ├── app.py                # Flask主应用
│   └── index.html            # 前端页面模板
└── report_scripts/            # 报告生成层
    ├── coin_quality_report.py   # 币种质量评分
    ├── bollinger_report.py      # 布林带收敛
    ├── volume_report.py        # 合约交易量
    └── deepseek_strategy_report.py  # DeepSeek策略
```

## 快速开始

```bash
# 1. 采集K线数据
python3 bian1k.py

# 2. 生成所有报告
python3 report_scripts/coin_quality_report.py
python3 report_scripts/bollinger_report.py
python3 report_scripts/volume_report.py

# 3. 扫描策略1信号
python3 strategy1_scan_fast.py

# 4. 启动网站
python3 flask_app/app.py
```

## 依赖

```
pandas
numpy
qcloud-cos
matplotlib
flask
requests
```
