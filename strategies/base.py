"""
策略基类 - 所有策略的统一接口
"""
import json
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config, STRATEGY_PARAMS
from utils.logger import get_logger
from core.data_loader import DataLoader
from core.chart_generator import ChartGenerator
from core.database import Database


class BaseStrategy(ABC):
    strategy_id: str = 'base'
    strategy_name: str = '基础策略'

    def __init__(self):
        self.logger = get_logger(f'strategy.{self.strategy_id}')
        self.params = STRATEGY_PARAMS.get(self.strategy_id, {})
        self.df: Optional[pd.DataFrame] = None

    def load_data(self, use_cache: bool = True) -> pd.DataFrame:
        self.df = DataLoader.get_klines(use_cache=use_cache)
        return self.df

    @abstractmethod
    def scan(self) -> List[Dict[str, Any]]:
        pass

    def generate_charts(self, symbols: List[str]) -> int:
        return ChartGenerator.generate_charts_batch(symbols)

    def save_report(self, report, save_to_db: bool = True) -> Path:
        # 兼容 StrategyReport 对象和 dict
        from models.signal import StrategyReport
        if isinstance(report, StrategyReport):
            report_dict = report.to_dict()
        elif isinstance(report, dict):
            report_dict = report
        else:
            report_dict = report

        ts_str = report_dict['timestamp']
        if isinstance(ts_str, datetime):
            ts_str = ts_str.strftime('%Y-%m-%d %H:%M:%S')
        json_file = config.OUTPUT_DIR / f"{self.strategy_id}_{ts_str.replace(':', '').replace(' ', '_')}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(report_dict, f, ensure_ascii=False, indent=2)

        self.logger.info(f"报告已保存: {json_file}")

        if save_to_db and report_dict.get('items'):
            Database.save_signals_batch(
                strategy=self.strategy_id,
                signals=report_dict['items'],
                timestamp=datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S') if isinstance(ts_str, str) else ts_str
            )

        return json_file

    def create_report(self, items: List[Dict[str, Any]],
                      conditions: List[str] = None,
                      summary: Dict[str, Any] = None,
                      raw_analysis: str = None) -> Dict[str, Any]:
        beijing_now = datetime.utcnow() + timedelta(hours=8)

        return {
            'strategy_name': self.strategy_id,
            'title': self.params.get('name', self.strategy_name),
            'timestamp': beijing_now.strftime('%Y-%m-%d %H:%M:%S'),
            'conditions': conditions or [],
            'summary': summary or {},
            'items': items,
            'raw_analysis': raw_analysis
        }

    def run(self, generate_charts: bool = True, save_to_db: bool = True) -> Dict[str, Any]:
        self.logger.info(f"{'='*60}")
        self.logger.info(f"{self.strategy_name} 扫描开始")
        self.logger.info(f"{'='*60}")

        if self.df is None:
            self.load_data()

        items = self.scan()

        if generate_charts and items:
            symbols = [item.get('symbol', '') for item in items[:30]]
            symbols = [s for s in symbols if s]
            if symbols:
                self.generate_charts(symbols)

        report = self.create_report(items)

        self.save_report(report, save_to_db=save_to_db)

        self.logger.info(f"找到 {len(items)} 个符合条件的币")
        return report


def format_volume(v: float) -> str:
    if v >= 1e9:
        return f"{v/1e9:.1f}B"
    elif v >= 1e6:
        return f"{v/1e6:.1f}M"
    elif v >= 1e3:
        return f"{v/1e3:.1f}K"
    return f"{v:.0f}"


def parse_volume(v: Any) -> float:
    if isinstance(v, (int, float)):
        return float(v)
    v = str(v)
    if 'B' in v:
        return float(v.replace('B', '')) * 1e9
    elif 'M' in v:
        return float(v.replace('M', '')) * 1e6
    elif 'K' in v:
        return float(v.replace('K', '')) * 1e3
    return float(v)
