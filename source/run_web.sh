#!/bin/bash
# 启动脚本

cd /root/crypto-scanner/source

# 设置环境变量（可选）
# export COS_SECRET_ID="your_secret_id"
# export COS_SECRET_KEY="your_secret_key"
# export DEEPSEEK_API_KEY="your_api_key"

# 启动Web服务
python3 main.py web
