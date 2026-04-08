"""
单元测试
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class TestDataLoader:
    def test_format_volume(self):
        from strategies.base import format_volume
        
        assert format_volume(1e9) == "1.0B"
        assert format_volume(1e6) == "1.0M"
        assert format_volume(1e3) == "1.0K"
        assert format_volume(100) == "100"
    
    def test_parse_volume(self):
        from strategies.base import parse_volume
        
        assert parse_volume("1.0B") == 1e9
        assert parse_volume("1.0M") == 1e6
        assert parse_volume("1.0K") == 1e3
        assert parse_volume(1000) == 1000


class TestSignal:
    def test_signal_to_dict(self):
        from models import Signal
        
        signal = Signal(
            strategy='test',
            symbol='BTCUSDT',
            timestamp=datetime.now(),
            price=50000.0,
            volume=1e6,
            change=2.5
        )
        
        d = signal.to_dict()
        assert d['strategy'] == 'test'
        assert d['symbol'] == 'BTCUSDT'
        assert d['price'] == 50000.0
    
    def test_signal_from_dict(self):
        from models import Signal
        
        data = {
            'strategy': 'test',
            'symbol': 'ETHUSDT',
            'timestamp': '2026-01-01T00:00:00',
            'price': 3000.0,
            'extra_field': 'value'
        }
        
        signal = Signal.from_dict(data)
        assert signal.strategy == 'test'
        assert signal.symbol == 'ETHUSDT'
        assert signal.extra['extra_field'] == 'value'


class TestStrategyReport:
    def test_report_to_dict(self):
        from models import StrategyReport
        
        report = StrategyReport(
            strategy_name='test_strategy',
            title='测试策略',
            timestamp=datetime.now(),
            conditions=['条件1', '条件2'],
            items=[{'symbol': 'BTCUSDT'}]
        )
        
        d = report.to_dict()
        assert d['strategy_name'] == 'test_strategy'
        assert d['title'] == '测试策略'
        assert len(d['conditions']) == 2
        assert len(d['items']) == 1


class TestBollingerStrategy:
    def test_check_converge_breakout_empty(self):
        from strategies.bollinger import BollingerStrategy
        
        strategy = BollingerStrategy()
        strategy.df = pd.DataFrame()
        
        result = strategy.check_converge_breakout(pd.DataFrame())
        assert result is None


class TestCoinQualityStrategy:
    def test_calc_score_insufficient_data(self):
        from strategies.coin_quality import CoinQualityStrategy
        
        strategy = CoinQualityStrategy()
        
        df = pd.DataFrame({'timestamp': [datetime.now()] * 10})
        result = strategy.calc_score(df)
        assert result is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
