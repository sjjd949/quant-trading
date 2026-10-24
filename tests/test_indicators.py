"""指标引擎测试：RingBuffer / pandas-ta 一致性 / 窗口管理 / 预热路径。

指标值与 pandas-ta 参考实现对照（误差 1e-9 内）：
- 引擎经 ``df.ta.rsi()/df.ta.sma()``（DataFrame 访问器）计算；
- 参考实现经 ``pandas_ta.rsi(series)/pandas_ta.sma(series)``（直接函数）
  与 ``Series.rolling().mean()``（独立实现）双重对照。
"""

from __future__ import annotations

import pandas as pd
import pandas_ta as ta
import pytest

from quant.config import IndicatorSettings
from quant.indicators import IndicatorEngine, RingBuffer
from quant.models import Candle, IndicatorSnapshot
from quant.utils.timeutils import datetime_to_ms, ms_to_datetime

#: 固定基准开盘时间（2025-10-24T06:20:00Z）
BASE_OPEN_MS = 1_761_287_200_000
#: 一分钟毫秒数
MINUTE_MS = 60_000


def make_candles(
    symbol: str,
    closes: list[float],
    volumes: list[float] | None = None,
    is_closed: bool = True,
    start_open_ms: int = BASE_OPEN_MS,
) -> list[Candle]:
    """构造测试用 1min K 线序列（时间固定、分钟对齐）。"""
    candles: list[Candle] = []
    for index, close in enumerate(closes):
        open_ms = start_open_ms + index * MINUTE_MS
        candles.append(
            Candle(
                symbol=symbol,
                open_time_ms=open_ms,
                close_time_ms=open_ms + MINUTE_MS - 1,
                open=close - 0.5,
                high=close + 0.5,
                low=close - 1.0,
                close=close,
                volume=float(volumes[index]) if volumes is not None else 10.0,
                quote_volume=close * 10.0,
                trades=10,
                is_closed=is_closed,
            )
        )
    return candles


def reference_snapshot(candles: list[Candle], config: IndicatorSettings) -> dict:
    """pandas-ta 参考实现（直接函数调用）计算指标末值。"""
    closes = pd.Series([c.close for c in candles], dtype="float64")
    volumes = pd.Series([c.volume for c in candles], dtype="float64")
    return {
        "rsi": float(ta.rsi(closes, length=config.rsi_length).iloc[-1]),
        "ma_short": float(ta.sma(closes, length=config.ma_short).iloc[-1]),
        "ma_long": float(ta.sma(closes, length=config.ma_long).iloc[-1]),
        "vol_ma": float(ta.sma(volumes, length=config.vol_ma_length).iloc[-1]),
    }


@pytest.fixture()
def engine() -> IndicatorEngine:
    """默认参数指标引擎（required_length=25）。"""
    return IndicatorEngine(IndicatorSettings())


# ---------------------------------------------------------------------------
# RingBuffer
# ---------------------------------------------------------------------------


def test_ring_buffer_basic() -> None:
    """append/to_list/__len__/clear 基本语义（从旧到新）。"""
    buffer: RingBuffer[int] = RingBuffer(maxlen=3)
    assert len(buffer) == 0
    buffer.append(1)
    buffer.append(2)
    buffer.append(3)
    assert len(buffer) == 3
    assert buffer.to_list() == [1, 2, 3]


def test_ring_buffer_eviction() -> None:
    """超出容量时淘汰最旧元素。"""
    buffer: RingBuffer[int] = RingBuffer(maxlen=3)
    for value in range(5):
        buffer.append(value)
    assert buffer.to_list() == [2, 3, 4]
    assert len(buffer) == 3


def test_ring_buffer_clear() -> None:
    """clear 清空全部元素。"""
    buffer: RingBuffer[int] = RingBuffer(maxlen=3)
    buffer.append(1)
    buffer.clear()
    assert len(buffer) == 0
    assert buffer.to_list() == []


def test_ring_buffer_invalid_maxlen() -> None:
    """非正 maxlen 抛 ValueError。"""
    with pytest.raises(ValueError, match="maxlen 必须为正整数"):
        RingBuffer(maxlen=0)


def test_ring_buffer_snapshot_is_copy() -> None:
    """to_list 返回浅拷贝，后续 append 不影响已取列表。"""
    buffer: RingBuffer[int] = RingBuffer(maxlen=3)
    buffer.append(1)
    snapshot = buffer.to_list()
    buffer.append(2)
    assert snapshot == [1]


# ---------------------------------------------------------------------------
# 指标引擎：pandas-ta 一致性
# ---------------------------------------------------------------------------


def test_warmup_matches_reference(engine: IndicatorEngine) -> None:
    """预热 30 根 K 线：指标值与参考实现误差 1e-9 内一致。"""
    closes = [100.0 + (i % 7) * 1.5 - (i % 3) * 0.8 for i in range(30)]
    volumes = [10.0 + (i % 5) * 3.0 for i in range(30)]
    candles = make_candles("BTCUSDT", closes, volumes)
    engine.warmup("BTCUSDT", candles)

    snapshot = engine.latest("BTCUSDT")
    assert snapshot is not None
    reference = reference_snapshot(candles, engine.config)
    assert snapshot.close == pytest.approx(closes[-1])
    assert snapshot.volume == pytest.approx(volumes[-1])
    assert snapshot.ts_ms == candles[-1].close_time_ms
    assert snapshot.rsi == pytest.approx(reference["rsi"], abs=1e-9)
    assert snapshot.ma_short == pytest.approx(reference["ma_short"], abs=1e-9)
    assert snapshot.ma_long == pytest.approx(reference["ma_long"], abs=1e-9)
    assert snapshot.vol_ma == pytest.approx(reference["vol_ma"], abs=1e-9)


def test_sma_matches_rolling_mean(engine: IndicatorEngine) -> None:
    """SMA 与 pandas rolling 独立实现一致（第二参照系）。"""
    closes = [50.0 + i * 2.0 for i in range(30)]
    candles = make_candles("BTCUSDT", closes)
    engine.warmup("BTCUSDT", candles)
    snapshot = engine.latest("BTCUSDT")
    assert snapshot is not None
    series = pd.Series(closes, dtype="float64")
    assert snapshot.ma_short == pytest.approx(float(series.rolling(5).mean().iloc[-1]), abs=1e-9)
    assert snapshot.ma_long == pytest.approx(float(series.rolling(25).mean().iloc[-1]), abs=1e-9)


def test_rsi_monotonic_up_is_100(engine: IndicatorEngine) -> None:
    """单调上涨序列 RSI 末值为 100（已知行情语义）。"""
    closes = [100.0 + i for i in range(30)]
    candles = make_candles("BTCUSDT", closes)
    engine.warmup("BTCUSDT", candles)
    snapshot = engine.latest("BTCUSDT")
    assert snapshot is not None
    assert snapshot.rsi == pytest.approx(100.0, abs=1e-9)


def test_update_returns_snapshot_and_matches_reference(engine: IndicatorEngine) -> None:
    """逐根 update：快照与「全量参考」一致。"""
    closes = [100.0 + (i % 9) * 2.0 for i in range(40)]
    volumes = [8.0 + (i % 4) * 5.0 for i in range(40)]
    candles = make_candles("ETHUSDT", closes, volumes)
    engine.warmup("ETHUSDT", candles[:30])

    snapshot = engine.update("ETHUSDT", candles[30])
    assert snapshot is not None
    reference = reference_snapshot(candles[:31], engine.config)
    assert snapshot.rsi == pytest.approx(reference["rsi"], abs=1e-9)
    assert snapshot.ma_short == pytest.approx(reference["ma_short"], abs=1e-9)
    assert snapshot.ma_long == pytest.approx(reference["ma_long"], abs=1e-9)
    assert snapshot.vol_ma == pytest.approx(reference["vol_ma"], abs=1e-9)


def test_update_ignores_unclosed_candle(engine: IndicatorEngine) -> None:
    """未收盘 K 线被忽略（返回 None，窗口不变）。"""
    closes = [100.0 + i for i in range(30)]
    candles = make_candles("BTCUSDT", closes)
    engine.warmup("BTCUSDT", candles)
    before = len(engine.windows["BTCUSDT"])
    unclosed = make_candles("BTCUSDT", [130.0], is_closed=False)[0]
    assert engine.update("BTCUSDT", unclosed) is None
    assert len(engine.windows["BTCUSDT"]) == before


def test_update_returns_none_when_window_insufficient(engine: IndicatorEngine) -> None:
    """窗口不足 required_length 时返回 None；第 25 根起产出快照。"""
    assert engine.required_length() == 25
    candles = make_candles("BTCUSDT", [100.0 + i for i in range(24)])
    for candle in candles:
        assert engine.update("BTCUSDT", candle) is None
    assert engine.latest("BTCUSDT") is None
    # 第 25 根（下一分钟）起有有效快照
    twenty_fifth = make_candles("BTCUSDT", [124.0], start_open_ms=BASE_OPEN_MS + 24 * MINUTE_MS)[0]
    snapshot = engine.update("BTCUSDT", twenty_fifth)
    assert snapshot is not None


def test_window_eviction_with_small_capacity() -> None:
    """环形窗口按 candle_window 淘汰最旧 K 线。"""
    config = IndicatorSettings(candle_window=5, rsi_length=3, ma_short=2, ma_long=3, vol_ma_length=2)
    engine = IndicatorEngine(config)
    closes = [100.0 + i for i in range(8)]
    engine.warmup("BTCUSDT", make_candles("BTCUSDT", closes))
    window = engine.windows["BTCUSDT"]
    assert len(window) == 5
    assert [c.close for c in window.to_list()] == closes[-5:]
    # 指标按窗口内最近 5 根计算
    snapshot = engine.latest("BTCUSDT")
    assert snapshot is not None
    reference = reference_snapshot(make_candles("BTCUSDT", closes[-5:]), config)
    assert snapshot.ma_long == pytest.approx(reference["ma_long"], abs=1e-9)


def test_prev_ma_propagation(engine: IndicatorEngine) -> None:
    """第二次快照的 prev_ma_* 等于第一次快照的均线值。"""
    closes = [100.0 + (i % 6) * 3.0 for i in range(30)]
    candles = make_candles("BTCUSDT", closes)
    engine.warmup("BTCUSDT", candles)
    first = engine.latest("BTCUSDT")
    assert first is not None
    assert first.prev_ma_short is None  # 首帧无前值

    extra = make_candles("BTCUSDT", [130.0], start_open_ms=BASE_OPEN_MS + 30 * MINUTE_MS)[0]
    second = engine.update("BTCUSDT", extra)
    assert second is not None
    assert second.prev_ma_short == pytest.approx(first.ma_short, abs=1e-12)
    assert second.prev_ma_long == pytest.approx(first.ma_long, abs=1e-12)


def test_multi_symbol_isolation(engine: IndicatorEngine) -> None:
    """多币种窗口与快照互不干扰。"""
    btc_closes = [100.0 + i for i in range(30)]
    eth_closes = [2000.0 + (i % 5) * 10.0 for i in range(30)]
    engine.warmup("BTCUSDT", make_candles("BTCUSDT", btc_closes))
    engine.warmup("ETHUSDT", make_candles("ETHUSDT", eth_closes))
    btc = engine.latest("BTCUSDT")
    eth = engine.latest("ETHUSDT")
    assert btc is not None and eth is not None
    assert btc.close == pytest.approx(129.0)
    assert eth.close == pytest.approx(2040.0)
    assert btc.rsi != eth.rsi


def test_latest_unknown_symbol_none(engine: IndicatorEngine) -> None:
    """未预热币种 latest 返回 None。"""
    assert engine.latest("SOLUSDT") is None


def test_warmup_ignores_unclosed(engine: IndicatorEngine) -> None:
    """预热仅灌入已收盘 K 线（末根未收盘被跳过）。"""
    candles = make_candles("BTCUSDT", [100.0 + i for i in range(30)])
    candles[-1].is_closed = False
    engine.warmup("BTCUSDT", candles)
    assert len(engine.windows["BTCUSDT"]) == 29
    snapshot = engine.latest("BTCUSDT")
    assert snapshot is not None  # 29 根仍满足 required_length
    assert snapshot.close == pytest.approx(128.0)


def test_snapshot_type_and_ts(engine: IndicatorEngine) -> None:
    """快照为 IndicatorSnapshot，ts_ms 取 K 线收盘时间。"""
    candles = make_candles("BTCUSDT", [100.0 + i for i in range(30)])
    engine.warmup("BTCUSDT", candles)
    snapshot = engine.latest("BTCUSDT")
    assert isinstance(snapshot, IndicatorSnapshot)
    assert snapshot.ts_ms == candles[-1].close_time_ms


def test_fixed_time_no_now_dependency(engine: IndicatorEngine) -> None:
    """全部断言基于固定 epoch ms，不依赖 now()。"""
    candles = make_candles("BTCUSDT", [100.0 + i for i in range(30)])
    engine.warmup("BTCUSDT", candles)
    snapshot = engine.latest("BTCUSDT")
    assert snapshot is not None
    assert snapshot.ts_ms == 1_761_287_200_000 + 29 * MINUTE_MS + MINUTE_MS - 1
    assert datetime_to_ms(ms_to_datetime(snapshot.ts_ms)) == snapshot.ts_ms
