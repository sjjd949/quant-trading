"""规则插件加载测试：成功加载 / 错误报清晰 / 参与扫描（禁网）。"""

from __future__ import annotations

import pytest

from quant.models import Candle, IndicatorSnapshot
from quant.scanner.base import RuleContext
from quant.scanner.engine import RuleEngine

EXAMPLE_ENTRY = "quant.scanner.rules_custom_example:ExampleSurgeRule"


def _context(close: float, open_: float = 100.0) -> RuleContext:
    candle = Candle(
        symbol="BTCUSDT",
        open_time_ms=60_000,
        close_time_ms=119_999,
        open=open_,
        high=max(open_, close),
        low=min(open_, close),
        close=close,
        volume=10.0,
        quote_volume=1000.0,
        trades=5,
        is_closed=True,
    )
    snapshot = IndicatorSnapshot(
        symbol="BTCUSDT",
        close=close,
        rsi=50.0,
        ma_short=1.0,
        ma_long=1.0,
        vol_ma=1.0,
        volume=10.0,
        ts_ms=119_999,
    )
    return RuleContext(
        symbol="BTCUSDT", candle=candle, window=[candle], indicators=snapshot
    )


def test_load_custom_success_and_scan():
    engine = RuleEngine(rules=[])
    engine.load_custom([EXAMPLE_ENTRY])
    assert "example_surge" in engine.rule_names()

    alerts = engine.scan(_context(close=103.0))  # 涨 3% ≥ 1%
    assert len(alerts) == 1
    assert alerts[0].rule == "example_surge"
    assert alerts[0].direction == "long"


def test_custom_rule_not_triggered_below_threshold():
    engine = RuleEngine(rules=[])
    engine.load_custom([EXAMPLE_ENTRY])
    assert engine.scan(_context(close=100.5)) == []  # 涨 0.5% < 1%


def test_load_custom_bad_format():
    engine = RuleEngine(rules=[])
    with pytest.raises(ValueError, match="格式错误"):
        engine.load_custom(["bad_entry_without_colon"])


def test_load_custom_missing_module():
    engine = RuleEngine(rules=[])
    with pytest.raises(ValueError, match="导入模块失败"):
        engine.load_custom(["quant.no_such_module:SomeRule"])


def test_load_custom_missing_class():
    engine = RuleEngine(rules=[])
    with pytest.raises(ValueError, match="不存在类"):
        engine.load_custom(["quant.scanner.rules_custom_example:NoSuchRule"])


def test_load_custom_not_a_rule():
    engine = RuleEngine(rules=[])
    with pytest.raises(ValueError, match="未继承 Rule"):
        engine.load_custom(["quant.feeds.base:FeedStatus"])


def test_load_custom_duplicate_name():
    engine = RuleEngine(rules=[])
    engine.load_custom([EXAMPLE_ENTRY])
    with pytest.raises(ValueError, match="重复"):
        engine.load_custom([EXAMPLE_ENTRY])
