#!/usr/bin/env python3
"""
加密货币策略扫描平台 - 主入口
"""
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from utils.logger import setup_logger, get_logger
from configs import STRATEGY_PARAMS


def run_strategy(strategy_id: str):
    from strategies.strategy1 import Strategy1
    from strategies.bollinger import BollingerStrategy
    from strategies.coin_quality import CoinQualityStrategy
    from strategies.deepseek import DeepSeekStrategy
    from strategies.volume import VolumeStrategy
    
    strategies = {
        'strategy1': Strategy1,
        'bollinger': BollingerStrategy,
        'coin_quality': CoinQualityStrategy,
        'deepseek': DeepSeekStrategy,
        'volume': VolumeStrategy
    }
    
    if strategy_id not in strategies:
        print(f"未知策略: {strategy_id}")
        print(f"可用策略: {list(strategies.keys())}")
        return None
    
    strategy_class = strategies[strategy_id]
    strategy = strategy_class()
    return strategy.run()


def run_all_strategies():
    strategy_ids = ['strategy1', 'coin_quality', 'bollinger', 'volume', 'deepseek']
    
    results = {}
    for sid in strategy_ids:
        try:
            result = run_strategy(sid)
            results[sid] = 'success' if result else 'failed'
        except Exception as e:
            results[sid] = f'error: {str(e)}'
    
    return results


def run_web():
    from flask_app.app_new import app
    from configs import config
    
    logger = get_logger('main')
    logger.info(f"启动Web服务: http://{config.WEB_HOST}:{config.WEB_PORT}")
    app.run(host=config.WEB_HOST, port=config.WEB_PORT, debug=False)


def run_collector():
    import subprocess
    import sys
    
    result = subprocess.run([sys.executable, 'bian1k/bian1k.py'], cwd='/root/crypto-scanner/source')
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description='加密货币策略扫描平台')
    parser.add_argument('command', choices=[
        'run', 'web', 'collect', 'all'
    ], help='命令: run=运行策略, web=启动Web服务, collect=采集数据, all=运行所有策略')
    parser.add_argument('--strategy', '-s', help='策略ID (run命令专用)')
    parser.add_argument('--log-file', help='日志文件路径')
    
    args = parser.parse_args()
    
    setup_logger('crypto_scanner', log_file=args.log_file)
    logger = get_logger('main')
    
    if args.command == 'run':
        if args.strategy:
            run_strategy(args.strategy)
        else:
            print("请指定策略ID: --strategy <strategy_id>")
            print(f"可用策略: {list(STRATEGY_PARAMS.keys())}")
    
    elif args.command == 'web':
        run_web()
    
    elif args.command == 'collect':
        run_collector()
    
    elif args.command == 'all':
        results = run_all_strategies()
        logger.info("所有策略运行结果:")
        for sid, status in results.items():
            logger.info(f"  {sid}: {status}")


if __name__ == '__main__':
    main()
