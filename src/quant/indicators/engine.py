"""指标引擎：环形窗口管理 + pandas-ta 计算（RSI/MA/量均）。

设计要点（对应难点 D4）：
- 每币种一个 :class:`~quant.indicators.buffers.RingBuffer`（已收盘
  1min K 线，容量见 ``indicators.candle_window``）；
- 每有新收盘 K 线即重建 pandas DataFrame 调 pandas-ta 计算，换取
  「指标值 = pandas-ta 参考实现」的强一致性；
- 仅接受 ``is_closed=True`` 的 K 线；窗口不足所需最长周期时返回
  ``None``（预热后不会发生）。
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
import pandas_ta as ta

from quant.config import IndicatorSettings
from quant.indicators.buffers import RingBuffer
from quant.models import Candle, IndicatorSnapshot

logger = logging.getLogger("quant.indicators.engine")


class IndicatorEngine:
    """指标引擎：窗口管理 + RSI/MA/量能计算 + 预热。"""

    def __init__(self, config: Optional[IndicatorSettings] = None) -> None:
        """初始化引擎。

        参数:
            config: 指标配置（窗口容量与各周期）；``None`` 时用默认配置。
        """
        self.config = config if config is not None else IndicatorSettings()
        #: 每币种已收盘 K 线环形窗口
        self.windows: dict[str, RingBuffer[Candle]] = {}
        #: 每币种最近一次指标快照
        self.last_snapshots: dict[str, IndicatorSnapshot] = {}

    def warmup(self, symbol: str, candles: list[Candle]) -> None:
        """用历史已收盘 K 线灌窗口（启动预热，仅 ``is_closed``）。

        预热后立即计算一次快照，使 ``latest()`` 冷启动即有有效值。
        """
        window = self._window(symbol)
        for candle in candles:
            if candle.is_closed:
                window.append(candle)
        if len(window) >= self.required_length():
            snapshot = self._compute(symbol)
            if snapshot is not None:
                self.last_snapshots[symbol] = snapshot
            logger.info(
                "指标预热完成：%s 灌入 %d 根已收盘 K 线",
                symbol,
                len(window),
            )

    def update(self, symbol: str, candle: Candle) -> Optional[IndicatorSnapshot]:
        """接收一根新收盘 K 线，更新窗口并返回指标快照。

        参数:
            symbol: 币种。
            candle: 新 K 线；``is_closed=False`` 时忽略并返回 ``None``。

        返回:
            指标快照；窗口不足所需最长周期时返回 ``None``。
        """
        if not candle.is_closed:
            return None
        window = self._window(symbol)
        window.append(candle)
        if len(window) < self.required_length():
            return None
        snapshot = self._compute(symbol)
        if snapshot is not None:
            self.last_snapshots[symbol] = snapshot
        return snapshot

    def latest(self, symbol: str) -> Optional[IndicatorSnapshot]:
        """返回该币种最近一次指标快照（无则 ``None``）。"""
        return self.last_snapshots.get(symbol)

    def required_length(self) -> int:
        """产出有效快照所需的最少 K 线根数。

        RSI 首值需 ``rsi_length + 1`` 根；SMA 需对应周期根数。
        """
        return max(
            self.config.rsi_length + 1,
            self.config.ma_short,
            self.config.ma_long,
            self.config.vol_ma_length,
        )

    # ------------------------------------------------------------------
    # 内部实现
    # ------------------------------------------------------------------

    def _window(self, symbol: str) -> RingBuffer[Candle]:
        """获取（按需创建）该币种的环形窗口。"""
        window = self.windows.get(symbol)
        if window is None:
            window = RingBuffer(maxlen=self.config.candle_window)
            self.windows[symbol] = window
        return window

    def _compute(self, symbol: str) -> Optional[IndicatorSnapshot]:
        """基于当前窗口重建 DataFrame 并计算指标快照。"""
        candles = self._window(symbol).to_list()
        if not candles:
            return None

        # 重建 DataFrame 调 pandas-ta（.ta 访问器在 pandas 3 返回 Series）
        frame = pd.DataFrame(
            {
                "close": [candle.close for candle in candles],
                "volume": [candle.volume for candle in candles],
            },
            dtype="float64",
        )
        rsi_series = frame.ta.rsi(length=self.config.rsi_length)
        ma_short_series = frame.ta.sma(length=self.config.ma_short)
        ma_long_series = frame.ta.sma(length=self.config.ma_long)
        vol_ma_series = ta.sma(frame["volume"], length=self.config.vol_ma_length)

        previous = self.last_snapshots.get(symbol)
        return IndicatorSnapshot(
            symbol=symbol,
            close=float(candles[-1].close),
            rsi=_last_valid(rsi_series),
            ma_short=_last_valid(ma_short_series),
            ma_long=_last_valid(ma_long_series),
            vol_ma=_last_valid(vol_ma_series),
            volume=float(candles[-1].volume),
            ts_ms=int(candles[-1].close_time_ms),
            prev_ma_short=previous.ma_short if previous is not None else None,
            prev_ma_long=previous.ma_long if previous is not None else None,
        )


def _last_valid(series: pd.Series) -> Optional[float]:
    """取序列最后一个值；NaN 时返回 ``None``。"""
    if len(series) == 0:
        return None
    value = float(series.iloc[-1])
    if pd.isna(value):
        return None
    return value
