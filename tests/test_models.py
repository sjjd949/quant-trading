"""数据模型测试：slots dataclass、默认值与字段语义。"""

from __future__ import annotations

from dataclasses import fields
from datetime import datetime

import pytest

from quant.models import Alert, Candle, IndicatorSnapshot, RuleResult, Tick


def test_tick_defaults_and_slots() -> None:
    """Tick：source 默认 binance；slots 禁止动态增字段。"""
    tick = Tick(
        symbol="BTCUSDT",
        price=68000.0,
        quantity=0.1,
        trade_time_ms=1_761_287_525_123,
        is_buyer_maker=False,
    )
    assert tick.source == "binance"
    with pytest.raises(AttributeError):
        tick.extra = 1  # type: ignore[attr-defined]


def test_candle_fields() -> None:
    """Candle：字段齐全且时间/收盘标记语义正确。"""
    candle = Candle(
        symbol="BTCUSDT",
        open_time_ms=1_761_287_200_000,
        close_time_ms=1_761_287_259_999,
        open=99.0,
        high=101.0,
        low=98.0,
        close=100.0,
        volume=12.5,
        quote_volume=1250.0,
        trades=42,
        is_closed=True,
    )
    assert candle.is_closed is True
    assert candle.close_time_ms - candle.open_time_ms == 59_999
    assert {f.name for f in fields(candle)} == {
        "symbol", "open_time_ms", "close_time_ms", "open", "high", "low",
        "close", "volume", "quote_volume", "trades", "is_closed",
    }


def test_indicator_snapshot_optional_and_prev_defaults() -> None:
    """IndicatorSnapshot：指标可空；prev_* 默认 None。"""
    snapshot = IndicatorSnapshot(
        symbol="BTCUSDT",
        close=100.0,
        rsi=None,
        ma_short=None,
        ma_long=None,
        vol_ma=None,
        volume=10.0,
        ts_ms=1_761_287_525_123,
    )
    assert snapshot.rsi is None
    assert snapshot.prev_ma_short is None
    assert snapshot.prev_ma_long is None
    snapshot2 = IndicatorSnapshot(
        symbol="BTCUSDT", close=100.0, rsi=55.0, ma_short=99.0, ma_long=98.0,
        vol_ma=10.0, volume=10.0, ts_ms=1_761_287_585_123,
        prev_ma_short=98.5, prev_ma_long=98.0,
    )
    assert snapshot2.prev_ma_short == 98.5


def test_alert_default_level() -> None:
    """Alert：level 默认 info。"""
    alert = Alert(
        ts=datetime(2025, 10, 24, 6, 32, 5),
        symbol="BTCUSDT",
        rule="price_move",
        message="价格异动",
        trigger_value=5.2,
        threshold=5.0,
    )
    assert alert.level == "info"
    assert alert.rule == "price_move"


def test_rule_result_defaults() -> None:
    """RuleResult：未触发时其余字段留空。"""
    result = RuleResult(triggered=False)
    assert result.triggered is False
    assert result.message == ""
    assert result.trigger_value == 0.0
    assert result.threshold == 0.0
