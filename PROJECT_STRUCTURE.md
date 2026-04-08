# 加密货币策略扫描平台 - 项目结构

## 服务器信息
- 公网IP: 43.133.253.208
- 内网IP: 172.21.0.16
- 系统: Ubuntu (Linux 6.8.0)
- 用户: root

---

## 一、项目文件结构

```
/root/
├── bian1k.py                          # 币安K线采集器（直连币安API）
├── strategy1_scan_fast.py              # 策略1信号扫描脚本（稳步抬升）
├── coin_quality_report.py             # 币种质量评分报告脚本
├── bollinger_report.py                # 布林带收敛报告脚本
└── volume_report.py                   # 合约交易量日报脚本

/root/crypto-scanner/
├── app.py                             # Flask主应用（端口5000）
├── configs/
│   └── config.py                      # 配置文件（COS密钥、桶名等）
├── scripts/                            # 各策略报告生成脚本
│   ├── coin_quality_report.py         # 币种质量评分
│   ├── deepseek_strategy_report.py    # DeepSeek策略
│   ├── bollinger_report.py             # 布林带收敛反弹
│   └── volume_report.py                # 合约交易量日报
├── templates/
│   ├── index.html                    # 主页模板（所有策略聚合）
│   └── strategy1.html                 # 策略1独立页面（已废弃，集成到index.html）
├── output/                            # 报告输出目录
│   ├── coin_quality_*.json
│   ├── deepseek_strategy_*.json
│   ├── bollinger_converge_*.json
│   └── volume_daily_*.json
│   └── all_signals.json               # 策略1信号汇总（已移至/var/www）
└── static/                            # 静态文件（如有）

/var/www/                              # 网站根目录
├── index.html                         # 静态首页（由Flask渲染）
├── all_signals.json                  # 策略1信号数据（实时）
└── strategy1_output.txt              # 策略1原始文本输出

/etc/cron.d/
└── crypto_cron                         # 定时任务配置
    ├── bian1k: 每小时第50分钟采集上传COS
    ├── coin_quality: 每小时05分生成币种质量报告
    ├── bollinger: 每小时10分生成布林带报告
    ├── volume: 每小时15分生成交易量报告
    └── deepseek: 每小时20分生成DeepSeek策略报告

/etc/nginx/
├── nginx.conf                        # Nginx主配置
└── conf.d/
    └── openclaw.conf                 # OpenClaw反代配置（端口8888）
```

---

## 二、核心文件说明

### 1. bian1k.py（数据采集）
- **作用**：直连币安API采集所有USDT永续合约小时K线，上传COS
- **COS路径**：`klines/futures_latest.parquet`
- **COS桶**：`lhsj-1h-1314017643`（ap-seoul）
- **触发**：每小时50分自动执行（cron）
- **关联**：采集完成后调用scripts目录下所有报告脚本

### 2. strategy1_scan_fast.py（策略1信号扫描）
- **作用**：扫描"稳步抬升"形态（连续3h+震幅0.5~2.5%最低价逐步抬高）
- **扫描窗口**：当前北京时间往前推3~6小时结束的信号
- **数据源**：COS的futures_latest.parquet（Top100成交额币种）
- **输出**：`/var/www/all_signals.json`
- **触发**：bian1k.py完成后自动调用（已集成）

### 3. app.py（Flask网站）
- **端口**：5000
- **路由**：
  - `/` — 主页（所有策略聚合）
  - `/api/reports` — 所有策略报告JSON
  - `/api/report/<name>` — 单个策略报告JSON
  - `/api/strategy1` — 策略1信号JSON
  - `/strategy1` — 策略1独立页面（已废弃）
  - `/chart/<symbol>` — K线图
- **启动命令**：`bash /tmp/run_flask.sh` 或 `python3 /root/crypto-scanner/app.py`

### 4. 各策略报告脚本
| 脚本 | 策略名 | cron时间 | 输出文件 |
|------|--------|---------|---------|
| coin_quality_report.py | coin_quality | 每小时05分 | output/coin_quality_*.json |
| bollinger_report.py | bollinger_converge | 每小时10分 | output/bollinger_*.json |
| volume_report.py | volume_daily | 每小时15分 | output/volume_*.json |
| deepseek_strategy_report.py | deepseek_strategy | 每小时20分 | output/deepseek_*.json |

---

## 三、网站访问地址

| 地址 | 说明 |
|------|------|
| http://43.133.253.208:5000/ | Flask主站 |
| http://43.133.253.208:5000/?view=strategy1 | 策略1：稳步抬升 |
| http://43.133.253.208:5000/?view=deepseek_strategy | 策略2：DeepSeek策略 |
| http://43.133.253.208:5000/?view=bollinger_converge | 策略3：布林带收敛反弹 |
| http://43.133.253.208:5000/?view=volume_daily | 策略4：合约交易量日报 |
| http://43.133.253.208:5000/?view=coin_quality | 策略5：币种质量评分 |

---

## 四、定时任务（Cron）

```
# /etc/cron.d/crypto_cron
50 * * * * root python3 /root/bian1k.py >> /var/log/bian1k.log 2>&1
05 * * * * root python3 /root/crypto-scanner/scripts/coin_quality_report.py >> /var/log/coin_quality.log 2>&1
10 * * * * root python3 /root/crypto-scanner/scripts/bollinger_report.py >> /var/log/bollinger.log 2>&1
15 * * * * root python3 /root/crypto-scanner/scripts/volume_report.py >> /var/log/volume.log 2>&1
20 * * * * root python3 /root/crypto-scanner/scripts/deepseek_strategy_report.py >> /var/log/deepseek.log 2>&1
```

---

## 五、数据流程

```
币安API (每分钟)
    ↓
bian1k.py (每小时50分)
    ↓ 上传 futures_latest.parquet 到 COS
COS: lhsj-1h-1314017643/klines/futures_latest.parquet
    ↓
各报告脚本 (bian1k完成后自动调用)
    ↓
/root/crypto-scanner/output/*.json
    ↓
Flask网站 app.py (端口5000)
    ↓
http://43.133.253.208:5000/

同时:
bian1k完成后 → strategy1_scan_fast.py
    ↓
/var/www/all_signals.json → Flask
```

---

## 六、数据库/COS信息

- **COS桶**：lhsj-1h-1314017643
- **地域**：ap-seoul
- **Endpoint**：cos.ap-seoul.myqcloud.com
- **数据Key**：`klines/futures_latest.parquet`（约56MB，656个币种）
- **K线格式**：UTC毫秒时间戳，19天历史（456条小时K线/币种）

---

## 七、已知问题/修复记录

### 1. MUSDT等币漏采（已修复）
- **原因**：ccxt版本symbol格式与币安API不一致，isInverse字段判断错误
- **修复**：重写bian1k.py直连币安API，isInverse判断改为`is not True`
- **日期**：2026-04-06

### 2. BIG_COINS格式错误（已修复）
- **原因**：BIG_COINS用BTC/ETH格式，但COS数据用BTCUSDT格式，永远匹配不上
- **修复**：coin_quality_report.py中BIG_COINS改为USDT格式
- **日期**：2026-04-06

### 3. full_sym路径错误（已修复）
- **原因**：coin_quality_report.py构建full_sym时用了错误的路径格式
- **修复**：直接使用symbol，不加/USDT:USDT后缀
- **日期**：2026-04-06

---

## 八、操作命令

```bash
# 重启Flask网站
bash /tmp/run_flask.sh
pkill -f app.py; python3 /root/crypto-scanner/app.py &

# 手动跑策略1扫描
python3 /root/strategy1_scan_fast.py

# 查看Flask日志
cat /tmp/flask.log

# 查看bian1k日志
tail -20 /var/log/bian1k.log

# 查看cron任务
cat /etc/cron.d/crypto_cron

# 测试API
curl http://127.0.0.1:5000/api/reports
curl http://127.0.0.1:5000/api/strategy1
curl http://127.0.0.1:5000/api/report/coin_quality
```

---

## 九、源代码位置

所有核心源代码已整理到 `/root/crypto-scanner/source/` 目录：

```
/root/crypto-scanner/source/
├── bian1k/
│   └── bian1k.py              # 币安K线采集器
├── strategy1_scan/
│   └── strategy1_scan_fast.py # 策略1信号扫描
├── flask_app/
│   ├── app.py                 # Flask主应用
│   ├── index.html             # 主页模板
│   └── config.py.bak          # 配置备份
└── report_scripts/
    ├── coin_quality_report.py   # 币种质量评分
    ├── bollinger_report.py      # 布林带收敛
    ├── volume_report.py        # 交易量日报
    └── deepseek_strategy_report.py  # DeepSeek策略
```

## 十、技术栈

- **数据采集**：Python3 + requests（直连币安API）
- **数据存储**：腾讯云COS（Parquet格式）
- **网站框架**：Flask + Jinja2模板
- **前端**：原生HTML/CSS/JS（暗色主题）
- **图表**：Flask动态生成K线图（matplotlib）
- **定时任务**：cron（/etc/cron.d/crypto_cron）
- **Web服务器**：Flask内置服务器（端口5000）
