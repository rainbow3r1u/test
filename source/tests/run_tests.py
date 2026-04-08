"""
简单测试脚本 - 不依赖pytest
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_format_volume():
    from strategies.base import format_volume
    
    assert format_volume(1e9) == "1.0B", "format_volume 1B failed"
    assert format_volume(1e6) == "1.0M", "format_volume 1M failed"
    assert format_volume(1e3) == "1.0K", "format_volume 1K failed"
    assert format_volume(100) == "100", "format_volume 100 failed"
    print("✓ test_format_volume passed")


def test_parse_volume():
    from strategies.base import parse_volume
    
    assert parse_volume("1.0B") == 1e9, "parse_volume 1B failed"
    assert parse_volume("1.0M") == 1e6, "parse_volume 1M failed"
    assert parse_volume("1.0K") == 1e3, "parse_volume 1K failed"
    assert parse_volume(1000) == 1000, "parse_volume int failed"
    print("✓ test_parse_volume passed")


def test_signal():
    from models import Signal
    from datetime import datetime
    
    signal = Signal(
        strategy='test',
        symbol='BTCUSDT',
        timestamp=datetime.now(),
        price=50000.0,
        volume=1e6,
        change=2.5
    )
    
    d = signal.to_dict()
    assert d['strategy'] == 'test', "signal strategy failed"
    assert d['symbol'] == 'BTCUSDT', "signal symbol failed"
    assert d['price'] == 50000.0, "signal price failed"
    print("✓ test_signal passed")


def test_strategy_report():
    from models import StrategyReport
    from datetime import datetime
    
    report = StrategyReport(
        strategy_name='test_strategy',
        title='测试策略',
        timestamp=datetime.now(),
        conditions=['条件1', '条件2'],
        items=[{'symbol': 'BTCUSDT'}]
    )
    
    d = report.to_dict()
    assert d['strategy_name'] == 'test_strategy', "report strategy_name failed"
    assert d['title'] == '测试策略', "report title failed"
    assert len(d['conditions']) == 2, "report conditions failed"
    assert len(d['items']) == 1, "report items failed"
    print("✓ test_strategy_report passed")


def test_config():
    from configs import config, STRATEGY_PARAMS
    
    assert config.OUTPUT_DIR is not None, "OUTPUT_DIR not set"
    assert len(STRATEGY_PARAMS) == 5, f"STRATEGY_PARAMS count wrong: {len(STRATEGY_PARAMS)}"
    print("✓ test_config passed")


def test_database():
    from models import Database
    
    Database._init_tables()
    print("✓ test_database passed")


def main():
    print("="*50)
    print("运行测试...")
    print("="*50)
    
    tests = [
        test_format_volume,
        test_parse_volume,
        test_signal,
        test_strategy_report,
        test_config,
        test_database,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"✗ {test.__name__} failed: {e}")
            failed += 1
    
    print("="*50)
    print(f"测试完成: {passed} passed, {failed} failed")
    print("="*50)
    
    return failed == 0


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
