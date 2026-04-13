#!/bin/bash
cd /home/ubuntu/crypto-scanner
python3 -c "
import eventlet
eventlet.monkey_patch()
from app import app, socketio
from utils.binance_account import BinanceAccountWS
BinanceAccountWS.start()
socketio.run(app, host='0.0.0.0', port=5002, allow_unsafe_werkzeug=True)
"
