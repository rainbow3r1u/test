"""
信号数据模型
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, List, Dict, Any


@dataclass
class Signal:
    strategy: str
    symbol: str
    timestamp: datetime
    price: Optional[float] = None
    volume: Optional[float] = None
    change: Optional[float] = None
    indicator: Optional[str] = None
    note: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            'strategy': self.strategy,
            'symbol': self.symbol,
            'timestamp': self.timestamp.isoformat() if isinstance(self.timestamp, datetime) else self.timestamp,
            'price': self.price,
            'volume': self.volume,
            'change': self.change,
            'indicator': self.indicator,
            'note': self.note
        }
        result.update(self.extra)
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Signal':
        extra = {k: v for k, v in data.items() 
                 if k not in ['strategy', 'symbol', 'timestamp', 'price', 'volume', 'change', 'indicator', 'note']}
        
        ts = data.get('timestamp')
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        
        return cls(
            strategy=data.get('strategy', ''),
            symbol=data.get('symbol', ''),
            timestamp=ts or datetime.now(),
            price=data.get('price'),
            volume=data.get('volume'),
            change=data.get('change'),
            indicator=data.get('indicator'),
            note=data.get('note'),
            extra=extra
        )


@dataclass
class StrategyReport:
    strategy_name: str
    title: str
    timestamp: datetime
    conditions: List[str] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    items: List[Dict[str, Any]] = field(default_factory=list)
    raw_analysis: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'strategy_name': self.strategy_name,
            'title': self.title,
            'timestamp': self.timestamp.strftime('%Y-%m-%d %H:%M:%S') if isinstance(self.timestamp, datetime) else self.timestamp,
            'conditions': self.conditions,
            'summary': self.summary,
            'items': self.items,
            'raw_analysis': self.raw_analysis
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StrategyReport':
        ts = data.get('timestamp')
        if isinstance(ts, str):
            try:
                ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
            except:
                ts = datetime.now()
        
        return cls(
            strategy_name=data.get('strategy_name', ''),
            title=data.get('title', ''),
            timestamp=ts or datetime.now(),
            conditions=data.get('conditions', []),
            summary=data.get('summary', {}),
            items=data.get('items', []),
            raw_analysis=data.get('raw_analysis')
        )
