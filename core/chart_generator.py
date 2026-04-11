"""
K线图表生成器 - 统一管理所有图表生成逻辑
"""
import os
import io
import time
from datetime import datetime, timezone
from typing import Optional, List
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import ccxt

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from configs import config
from utils.logger import get_logger

logger = get_logger('chart_generator')


class ChartGenerator:
    _exchange = None

    @classmethod
    def _get_exchange(cls):
        if cls._exchange is None:
            cls._exchange = ccxt.binance({
                'enableRateLimit': True,
                'options': {'defaultType': 'future'}
            })
        return cls._exchange

    @classmethod
    def _fetch_ohlcv(cls, symbol: str, timeframe: str = '1h', limit: int = 24,
                     filter_incomplete: bool = False) -> Optional[pd.DataFrame]:
        exchange = cls._get_exchange()
        try:
            symbol = symbol.replace('USDT', '').replace('/USDT', '').replace(':USDT', '')
            full_symbol = f"{symbol}/USDT:USDT"

            ohlcv = exchange.fetch_ohlcv(full_symbol, timeframe=timeframe, limit=limit)
            if not ohlcv:
                return None

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

            if filter_incomplete and len(df) > 0:
                now_ms = int(time.time() * 1000)
                last_candle_ts = df.iloc[-1]['timestamp']

                candle_durations = {
                    '1h': 60 * 60 * 1000,
                    '4h': 4 * 60 * 60 * 1000,
                    '1d': 24 * 60 * 60 * 1000,
                }
                candle_duration_ms = candle_durations.get(timeframe, 60 * 60 * 1000)

                if now_ms - last_candle_ts < candle_duration_ms:
                    df = df.iloc[:-1]

            return df
        except Exception as e:
            logger.warning(f"获取 {symbol} {timeframe} K线失败: {e}")
            return None

    @staticmethod
    def _calculate_bollinger_bands(df: pd.DataFrame, period: int = 20, std_dev: float = 2.0):
        df = df.copy()
        df['ma'] = df['close'].rolling(window=period).mean()
        df['std'] = df['close'].rolling(window=period).std()
        df['upper'] = df['ma'] + std_dev * df['std']
        df['lower'] = df['ma'] - std_dev * df['std']
        return df

    @classmethod
    def _draw_candlestick(cls, ax, df: pd.DataFrame, title: str, linewidth: float = 1, timeframe: str = '1h'):
        if df is None or len(df) == 0:
            ax.text(0.5, 0.5, '无数据', ha='center', va='center', fontsize=14, color='#666')
            ax.set_title(title, fontsize=11, color='#fff', pad=8)
            return

        # 布林通道 - 用完整数据计算
        bb_period = min(20, len(df))
        if bb_period < 5:
            bb_period = len(df)
        bb_std = 2.0
        df_bb = cls._calculate_bollinger_bands(df, bb_period, bb_std)

        if timeframe == '1h':
            display_count = 24
        elif timeframe == '4h':
            display_count = 12
        elif timeframe == '1d':
            display_count = 12
        else:
            display_count = len(df)

        df_display = df.tail(display_count).reset_index(drop=True)
        df_bb_display = df_bb.tail(display_count).reset_index(drop=True)

        ax.set_facecolor('#1a1a1a')
        ax.grid(True, alpha=0.2, color='#333')
        ax.set_title(title, fontsize=11, color='#fff', pad=8)
        ax.tick_params(colors='#999', labelsize=8)

        x = range(len(df_bb_display))
        ax.plot(x, df_bb_display['upper'], color='#FFD700', linewidth=1.2, label=f'BB({bb_period},{bb_std})')
        ax.plot(x, df_bb_display['ma'], color='#DA70D6', linewidth=1.2)
        ax.plot(x, df_bb_display['lower'], color='#4169E1', linewidth=1.2)

        for i in range(len(df_display)):
            o = df_display.iloc[i]['open']
            h = df_display.iloc[i]['high']
            l = df_display.iloc[i]['low']
            c = df_display.iloc[i]['close']
            color = '#00a854' if c >= o else '#eb3c3c'

            ax.plot([i, i], [l, h], color=color, linewidth=0.5)
            width = 0.4 if linewidth == 1 else 0.3
            if c >= o:
                ax.bar([i], [c - o], width=width, bottom=[o], color=color, edgecolor=color)
            else:
                ax.bar([i], [o - c], width=width, bottom=[c], color=color, edgecolor=color)

        ax.autoscale_view()
        price_min = min(df_display['low'].min(), df_bb_display['lower'].min())
        price_max = max(df_display['high'].max(), df_bb_display['upper'].max())
        padding = (price_max - price_min) * 0.05
        ax.set_ylim(price_min - padding, price_max + padding)
        ax.set_xlim(-0.5, len(df_display) - 0.5)
        for line in ax.get_lines():
            line.set_clip_on(True)
            line.set_clip_box(ax.bbox)

    @classmethod
    def _aggregate_to_daily(cls, df_4h: pd.DataFrame) -> pd.DataFrame:
        df_sorted = df_4h.sort_values('timestamp').reset_index(drop=True)
        daily_data = []
        for i in range(0, len(df_sorted) - 3, 4):
            group = df_sorted.iloc[i:i+4]
            daily_data.append({
                'open': group.iloc[0]['open'],
                'high': group['high'].max(),
                'low': group['low'].min(),
                'close': group.iloc[-1]['close'],
                'volume': group['volume'].sum()
            })
        return pd.DataFrame(daily_data).tail(10)

    @classmethod
    def _aggregate_timeframe(cls, df: pd.DataFrame, freq: str) -> pd.DataFrame:
        if df is None or len(df) == 0:
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        x = df.copy()
        x['bucket'] = x['timestamp'].dt.floor(freq)
        agg = x.groupby('bucket').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).reset_index().rename(columns={'bucket': 'timestamp'})
        return agg.sort_values('timestamp').reset_index(drop=True)

    @classmethod
    def generate_triple_chart_from_cos(cls, symbol: str, cutoff: Optional[datetime] = None) -> Optional[bytes]:
        from core.data_loader import DataLoader

        cache_dir = config.CHARTS_DIR
        cache_key = f"{symbol}_triple_cos"
        
        if cutoff is None:
            cache_file = cache_dir / f"{cache_key}.png"
            if cache_file.exists():
                cache_age = (time.time() - cache_file.stat().st_mtime) / 3600
                if cache_age < config.CHART_CACHE_HOURS:
                    with open(cache_file, 'rb') as f:
                        return f.read()
        else:
            cutoff_tag = cutoff.strftime('%Y%m%d%H')
            cache_file = cache_dir / f"{cache_key}_{cutoff_tag}.png"
            if cache_file.exists():
                with open(cache_file, 'rb') as f:
                    return f.read()

        df_symbol = DataLoader.get_symbol_data(symbol, use_cache=False)
        if df_symbol is None or len(df_symbol) == 0:
            return None

        df_symbol = df_symbol.sort_values('timestamp').copy()
        if cutoff is None:
            cutoff = df_symbol['timestamp'].max()
        df_symbol = df_symbol[df_symbol['timestamp'] <= cutoff].copy()
        if len(df_symbol) == 0:
            return None

        df_1h = df_symbol.tail(720).copy()
        df_4h = cls._aggregate_timeframe(df_1h, '4h')
        df_1d = cls._aggregate_timeframe(df_1h, '1d')

        plt.style.use('dark_background')
        fig = plt.figure(figsize=(18, 14))
        gs = fig.add_gridspec(3, 1, hspace=0.3)
        ax1 = fig.add_subplot(gs[0])
        ax2 = fig.add_subplot(gs[1])
        ax3 = fig.add_subplot(gs[2])
        fig.patch.set_facecolor('#1a1a1a')

        cls._draw_candlestick(ax1, df_1h, f'{symbol} - 1H (COS snapshot)', linewidth=1, timeframe='1h')
        cls._draw_candlestick(ax2, df_4h, f'{symbol} - 4H (COS snapshot)', linewidth=1.5, timeframe='4h')
        cls._draw_candlestick(ax3, df_1d, f'{symbol} - Daily (COS snapshot)', linewidth=2, timeframe='1d')

        plt.tight_layout(pad=1.5)
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=120, facecolor='#1a1a1a', bbox_inches='tight', pad_inches=0.1)
        buf.seek(0)
        plt.close(fig)

        cache_dir.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'wb') as f:
            f.write(buf.getvalue())
        return buf.read()

    @classmethod
    def generate_triple_chart_live(cls, symbol: str) -> Optional[bytes]:
        """通过 REST API 获取最新行情生成图表"""
        df_1h = cls._fetch_ohlcv(symbol, '1h', 60, filter_incomplete=False)
        df_4h = cls._fetch_ohlcv(symbol, '4h', 40, filter_incomplete=False)
        df_1d = cls._fetch_ohlcv(symbol, '1d', 30, filter_incomplete=False)

        if df_1h is None or df_4h is None or df_1d is None:
            return None

        plt.style.use('dark_background')
        fig = plt.figure(figsize=(18, 14))
        gs = fig.add_gridspec(3, 1, hspace=0.3)
        ax1 = fig.add_subplot(gs[0])
        ax2 = fig.add_subplot(gs[1])
        ax3 = fig.add_subplot(gs[2])
        fig.patch.set_facecolor('#1a1a1a')

        cls._draw_candlestick(ax1, df_1h, f'{symbol} - 1H (实时)', linewidth=1, timeframe='1h')
        cls._draw_candlestick(ax2, df_4h, f'{symbol} - 4H (实时)', linewidth=1.5, timeframe='4h')
        cls._draw_candlestick(ax3, df_1d, f'{symbol} - Daily (实时)', linewidth=2, timeframe='1d')

        plt.tight_layout(pad=1.5)
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=120, facecolor='#1a1a1a', bbox_inches='tight', pad_inches=0.1)
        buf.seek(0)
        plt.close(fig)
        return buf.read()

    @classmethod
    def generate_triple_chart(cls, symbol: str) -> Optional[bytes]:
        return cls.generate_triple_chart_from_cos(symbol, cutoff=None)

    @classmethod
    def generate_triple_charts_batch(cls, symbols: List[str]) -> int:
        success_count = 0
        for i, symbol in enumerate(symbols):
            try:
                result = cls.generate_triple_chart(symbol)
                if result:
                    success_count += 1
                    logger.info(f"[{i+1}/{len(symbols)}] {symbol} 三合一图表 OK")
                else:
                    logger.warning(f"[{i+1}/{len(symbols)}] {symbol} 无数据")
                time.sleep(0.15)
            except Exception as e:
                logger.error(f"[{i+1}/{len(symbols)}] {symbol} 错误: {e}")

        return success_count
