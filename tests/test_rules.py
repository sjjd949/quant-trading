"""规则测试：4 条规则的触发/不触发/边界 + 引擎开关与 Alert 产出。

每条规则均含：触发样例（注释注明对应真实行情场景）、不触发样例、
恰好等于阈值的边界样例。
"""

from __future__ import annotations

import pytest

from quant.config import RuleSettings, Settings
from quant.indicators import IndicatorEngine
from quant.models import Alert, Candle, IndicatorSnapshot
from quant.scanner import (
    MaCrossRule,
    PriceMoveRule,
    RsiRule,
    RuleEngine,
    VolumeSpikeRule,
    build_default_rules,
)
from quant.scanner.base import Rule, RuleContext
from quant.utils.timeutils import datetime_to_ms, ms_to_datetime

#: 固定基准开盘时间（2025-10-24T06:20:00Z）
BASE_OPEN_MS = 1_761_287_200_000
MINUTE_MS = 60_000


def make_candle(symbol: str, index: int, close: float, volume: float = 10.0) -> Candle:
    """构造第 index 分钟的测试 K 线（固定时间）。"""
    open_ms = BASE_OPEN_MS + index * MINUTE_MS
    return Candle(
        symbol=symbol,
        open_time_ms=open_ms,
        close_time_ms=open_ms + MINUTE_MS - 1,
        open=close - 0.5,
        high=close + 0.5,
        low=close - 1.0,
        close=close,
        volume=volume,
        quote_volume=close * volume,
        trades=10,
        is_closed=True,
    )


def make_window(closes: list[float], volumes: list[float] | None = None) -> list[Candle]:
    """由收盘价序列构造 K 线窗口（升序）。"""
    return [
        make_candle("BTCUSDT", i, close, volumes[i] if volumes else 10.0)
        for i, close in enumerate(closes)
    ]


def make_snapshot(
    close: float = 100.0,
    rsi: float | None = None,
    ma_short: float | None = None,
    ma_long: float | None = None,
    vol_ma: float | None = None,
    volume: float = 10.0,
    ts_ms: int = BASE_OPEN_MS + 5 * MINUTE_MS,
    prev_ma_short: float | None = None,
    prev_ma_long: float | None = None,
) -> IndicatorSnapshot:
    """构造指标快照（显式指定各指标值）。"""
    return IndicatorSnapshot(
        symbol="BTCUSDT",
        close=close,
        rsi=rsi,
        ma_short=ma_short,
        ma_long=ma_long,
        vol_ma=vol_ma,
        volume=volume,
        ts_ms=ts_ms,
        prev_ma_short=prev_ma_short,
        prev_ma_long=prev_ma_long,
    )


def make_ctx(
    window: list[Candle],
    snapshot: IndicatorSnapshot,
    prev: IndicatorSnapshot | None = None,
) -> RuleContext:
    """构造规则上下文（当前 K 线为窗口最后一根）。"""
    return RuleContext(
        symbol="BTCUSDT",
        candle=window[-1],
        window=window,
        indicators=snapshot,
        prev_indicators=prev,
    )


# ---------------------------------------------------------------------------
# 价格异动
# ---------------------------------------------------------------------------


def test_price_move_triggers_up() -> None:
    """触发样例：SOL 5 分钟上涨 6%（阈值 5%）→ 拉升告警。"""
    rule = PriceMoveRule(window_min=5, threshold_pct=5.0)
    window = make_window([100.0, 100.0, 100.0, 100.0, 100.0, 106.0])
    ctx = make_ctx(window, make_snapshot(close=106.0))
    result = rule.evaluate(ctx)
    assert result.triggered is True
    assert result.trigger_value == pytest.approx(6.0)
    assert result.threshold == 5.0
    assert "价格异动" in result.message
    assert "+6.00%" in result.message


def test_price_move_triggers_down() -> None:
    """触发样例：5 分钟急跌 6% → 砸盘告警。"""
    rule = PriceMoveRule(window_min=5, threshold_pct=5.0)
    window = make_window([100.0, 100.0, 100.0, 100.0, 100.0, 94.0])
    ctx = make_ctx(window, make_snapshot(close=94.0))
    result = rule.evaluate(ctx)
    assert result.triggered is True
    assert result.trigger_value == pytest.approx(6.0)
    assert "价格异动" in result.message


def test_price_move_not_triggered_small_move() -> None:
    """不触发样例：5 分钟仅涨 1%（低于阈值）。"""
    rule = PriceMoveRule(window_min=5, threshold_pct=5.0)
    window = make_window([100.0, 100.5, 100.2, 100.8, 100.3, 101.0])
    ctx = make_ctx(window, make_snapshot(close=101.0))
    result = rule.evaluate(ctx)
    assert result.triggered is False
    assert result.message == ""


def test_price_move_boundary_exact_threshold() -> None:
    """边界样例：恰好上涨 5%（等于阈值）→ 达到即触发。"""
    rule = PriceMoveRule(window_min=5, threshold_pct=5.0)
    window = make_window([100.0, 100.0, 100.0, 100.0, 100.0, 105.0])
    ctx = make_ctx(window, make_snapshot(close=105.0))
    result = rule.evaluate(ctx)
    assert result.triggered is True
    assert result.trigger_value == pytest.approx(5.0)


def test_price_move_insufficient_window() -> None:
    """窗口不足 window_min+1 根时不触发。"""
    rule = PriceMoveRule(window_min=5, threshold_pct=5.0)
    window = make_window([100.0, 100.0, 100.0, 100.0, 130.0])  # 仅 5 根
    ctx = make_ctx(window, make_snapshot(close=130.0))
    assert rule.evaluate(ctx).triggered is False


def test_price_move_custom_params() -> None:
    """自定义参数：window_min=2、threshold_pct=1。"""
    rule = PriceMoveRule(window_min=2, threshold_pct=1.0)
    window = make_window([100.0, 100.0, 102.0])
    ctx = make_ctx(window, make_snapshot(close=102.0))
    result = rule.evaluate(ctx)
    assert result.triggered is True
    assert "/2min" in result.message


# ---------------------------------------------------------------------------
# 成交量突增
# ---------------------------------------------------------------------------


def test_volume_spike_triggers() -> None:
    """触发样例：当前量 100、均量 20 → 5 倍（阈值 3 倍）。"""
    rule = VolumeSpikeRule(multiplier=3.0)
    window = make_window([100.0] * 6, volumes=[20.0] * 5 + [100.0])
    ctx = make_ctx(window, make_snapshot(vol_ma=20.0))
    result = rule.evaluate(ctx)
    assert result.triggered is True
    assert result.trigger_value == pytest.approx(5.0)
    assert result.threshold == 3.0
    assert "成交量突增" in result.message


def test_volume_spike_not_triggered() -> None:
    """不触发样例：当前量 50、均量 20 → 2.5 倍（低于阈值）。"""
    rule = VolumeSpikeRule(multiplier=3.0)
    window = make_window([100.0] * 6, volumes=[20.0] * 5 + [50.0])
    ctx = make_ctx(window, make_snapshot(vol_ma=20.0))
    assert rule.evaluate(ctx).triggered is False


def test_volume_spike_boundary_exact_multiplier() -> None:
    """边界样例：恰好 3 倍 → 严格大于才触发，故不触发。"""
    rule = VolumeSpikeRule(multiplier=3.0)
    window = make_window([100.0] * 6, volumes=[20.0] * 5 + [60.0])
    ctx = make_ctx(window, make_snapshot(vol_ma=20.0))
    assert rule.evaluate(ctx).triggered is False


def test_volume_spike_without_vol_ma() -> None:
    """量均未就绪（None 或 0）时不触发。"""
    rule = VolumeSpikeRule(multiplier=3.0)
    window = make_window([100.0] * 6, volumes=[20.0] * 5 + [100.0])
    assert rule.evaluate(make_ctx(window, make_snapshot(vol_ma=None))).triggered is False
    assert rule.evaluate(make_ctx(window, make_snapshot(vol_ma=0.0))).triggered is False


# ---------------------------------------------------------------------------
# RSI 极值
# ---------------------------------------------------------------------------


def test_rsi_overbought_triggers() -> None:
    """触发样例：RSI14=75 ≥ 70 → 超买告警。"""
    rule = RsiRule(overbought=70.0, oversold=30.0)
    window = make_window([100.0 + i for i in range(6)])
    ctx = make_ctx(window, make_snapshot(rsi=75.0))
    result = rule.evaluate(ctx)
    assert result.triggered is True
    assert result.trigger_value == pytest.approx(75.0)
    assert result.threshold == 70.0
    assert "RSI超买" in result.message


def test_rsi_oversold_triggers() -> None:
    """触发样例：RSI14=25 ≤ 30 → 超卖告警。"""
    rule = RsiRule(overbought=70.0, oversold=30.0)
    window = make_window([100.0 + i for i in range(6)])
    ctx = make_ctx(window, make_snapshot(rsi=25.0))
    result = rule.evaluate(ctx)
    assert result.triggered is True
    assert "RSI超卖" in result.message


def test_rsi_neutral_not_triggered() -> None:
    """不触发样例：RSI14=50 处于中性区。"""
    rule = RsiRule(overbought=70.0, oversold=30.0)
    window = make_window([100.0 + i for i in range(6)])
    ctx = make_ctx(window, make_snapshot(rsi=50.0))
    assert rule.evaluate(ctx).triggered is False


def test_rsi_boundary_exact_thresholds() -> None:
    """边界样例：恰好 70 / 恰好 30 → 达到即触发。"""
    rule = RsiRule(overbought=70.0, oversold=30.0)
    window = make_window([100.0 + i for i in range(6)])
    assert rule.evaluate(make_ctx(window, make_snapshot(rsi=70.0))).triggered is True
    assert rule.evaluate(make_ctx(window, make_snapshot(rsi=30.0))).triggered is True


def test_rsi_missing_value_not_triggered() -> None:
    """RSI 未就绪（None）时不触发。"""
    rule = RsiRule()
    window = make_window([100.0 + i for i in range(6)])
    assert rule.evaluate(make_ctx(window, make_snapshot(rsi=None))).triggered is False


# ---------------------------------------------------------------------------
# 均线交叉
# ---------------------------------------------------------------------------


def test_ma_cross_golden() -> None:
    """金叉样例：MA5 由 98 上穿至 101，MA25=100。"""
    rule = MaCrossRule(ma_short=5, ma_long=25)
    window = make_window([100.0 + i for i in range(6)])
    prev = make_snapshot(ma_short=98.0, ma_long=100.0)
    ctx = make_ctx(window, make_snapshot(ma_short=101.0, ma_long=100.0), prev=prev)
    result = rule.evaluate(ctx)
    assert result.triggered is True
    assert "均线金叉" in result.message
    assert result.trigger_value == pytest.approx(1.0)


def test_ma_cross_death() -> None:
    """死叉样例：MA5 由 101 下穿至 99，MA25=100。"""
    rule = MaCrossRule(ma_short=5, ma_long=25)
    window = make_window([100.0 + i for i in range(6)])
    prev = make_snapshot(ma_short=101.0, ma_long=100.0)
    ctx = make_ctx(window, make_snapshot(ma_short=99.0, ma_long=100.0), prev=prev)
    result = rule.evaluate(ctx)
    assert result.triggered is True
    assert "均线死叉" in result.message


def test_ma_cross_no_cross_when_parallel() -> None:
    """不触发样例：双线保持原有上下关系（均在长线之上/之下）。"""
    rule = MaCrossRule()
    window = make_window([100.0 + i for i in range(6)])
    # 持续金叉后：前值已在上，当前仍在上
    prev = make_snapshot(ma_short=101.0, ma_long=100.0)
    ctx = make_ctx(window, make_snapshot(ma_short=102.0, ma_long=100.0), prev=prev)
    assert rule.evaluate(ctx).triggered is False
    # 持续死叉后：前值已在下，当前仍在下
    prev = make_snapshot(ma_short=98.0, ma_long=100.0)
    ctx = make_ctx(window, make_snapshot(ma_short=97.0, ma_long=100.0), prev=prev)
    assert rule.evaluate(ctx).triggered is False


def test_ma_cross_boundary_prev_equal() -> None:
    """边界样例：前值双线相等、当前短线上穿 → 金叉。"""
    rule = MaCrossRule()
    window = make_window([100.0 + i for i in range(6)])
    prev = make_snapshot(ma_short=100.0, ma_long=100.0)
    ctx = make_ctx(window, make_snapshot(ma_short=100.5, ma_long=100.0), prev=prev)
    assert rule.evaluate(ctx).triggered is True


def test_ma_cross_without_prev_snapshot() -> None:
    """无上一快照且快照无 prev_ma_* 时不触发（首帧）。"""
    rule = MaCrossRule()
    window = make_window([100.0 + i for i in range(6)])
    ctx = make_ctx(window, make_snapshot(ma_short=101.0, ma_long=100.0), prev=None)
    assert rule.evaluate(ctx).triggered is False


def test_ma_cross_fallback_to_snapshot_prev_ma() -> None:
    """回退路径：无 prev_indicators 时用快照自带 prev_ma_* 判断。"""
    rule = MaCrossRule()
    window = make_window([100.0 + i for i in range(6)])
    snapshot = make_snapshot(ma_short=101.0, ma_long=100.0, prev_ma_short=98.0, prev_ma_long=100.0)
    ctx = make_ctx(window, snapshot, prev=None)
    result = rule.evaluate(ctx)
    assert result.triggered is True
    assert "均线金叉" in result.message


def test_ma_cross_missing_ma_not_triggered() -> None:
    """均线值缺失时不触发。"""
    rule = MaCrossRule()
    window = make_window([100.0 + i for i in range(6)])
    prev = make_snapshot(ma_short=98.0, ma_long=100.0)
    ctx = make_ctx(window, make_snapshot(ma_short=None, ma_long=100.0), prev=prev)
    assert rule.evaluate(ctx).triggered is False


# ---------------------------------------------------------------------------
# 规则引擎
# ---------------------------------------------------------------------------


@pytest.fixture()
def base_window() -> list[Candle]:
    """6 根 K 线的窗口（价格异动触发场景）。"""
    return make_window([100.0, 100.0, 100.0, 100.0, 100.0, 106.0])


def test_engine_scan_produces_alert(base_window: list[Candle]) -> None:
    """scan 产出 Alert：字段完整，ts 取 K 线收盘时间 UTC。"""
    engine = RuleEngine([PriceMoveRule()])
    ctx = make_ctx(base_window, make_snapshot(close=106.0))
    alerts = engine.scan(ctx)
    assert len(alerts) == 1
    alert = alerts[0]
    assert isinstance(alert, Alert)
    assert alert.rule == "price_move"
    assert alert.symbol == "BTCUSDT"
    assert alert.level == "warning"
    assert alert.trigger_value == pytest.approx(6.0)
    assert alert.threshold == 5.0
    assert alert.ts.tzinfo is not None
    assert datetime_to_ms(alert.ts) == base_window[-1].close_time_ms
    assert ms_to_datetime(datetime_to_ms(alert.ts)) == alert.ts


def test_engine_scan_multiple_rules() -> None:
    """多条规则同时触发时产出多条告警。"""
    engine = RuleEngine(
        [PriceMoveRule(), VolumeSpikeRule(multiplier=3.0), RsiRule()]
    )
    # 价格异动 + 量突增 + RSI 超买同时成立的窗口
    window = make_window(
        [100.0, 100.0, 100.0, 100.0, 100.0, 106.0],
        volumes=[20.0, 20.0, 20.0, 20.0, 20.0, 100.0],
    )
    ctx = make_ctx(window, make_snapshot(close=106.0, rsi=75.0, vol_ma=20.0))
    alerts = engine.scan(ctx)
    assert {a.rule for a in alerts} == {"price_move", "volume_spike", "rsi"}


def test_engine_set_enabled(base_window: list[Candle]) -> None:
    """set_enabled 禁用后零产出；返回 False 表示无此规则。"""
    engine = RuleEngine([PriceMoveRule()])
    ctx = make_ctx(base_window, make_snapshot(close=106.0))
    assert len(engine.scan(ctx)) == 1
    assert engine.set_enabled("price_move", False) is True
    assert engine.scan(ctx) == []
    assert engine.set_enabled("price_move", True) is True
    assert len(engine.scan(ctx)) == 1
    assert engine.set_enabled("nonexistent", True) is False


def test_engine_rule_names() -> None:
    """rule_names 返回注册顺序的全部规则名。"""
    engine = RuleEngine()
    assert engine.rule_names() == ["price_move", "volume_spike", "rsi", "ma_cross"]


def test_engine_all_disabled_zero_output(base_window: list[Candle]) -> None:
    """全部禁用时 scan 返回空列表。"""
    engine = RuleEngine()
    for name in engine.rule_names():
        engine.set_enabled(name, False)
    ctx = make_ctx(base_window, make_snapshot(close=106.0, rsi=80.0, vol_ma=1.0))
    assert engine.scan(ctx) == []


def test_engine_default_rules_from_settings() -> None:
    """build_default_rules 按配置构造 4 条规则（开关与参数生效）。"""
    settings = Settings()
    rules = build_default_rules(settings)
    assert len(rules) == 4
    by_name = {rule.name: rule for rule in rules}
    assert by_name["price_move"].threshold_pct == 5.0
    assert by_name["price_move"].window_min == 5
    assert by_name["volume_spike"].multiplier == 3.0
    assert by_name["rsi"].overbought == 70.0
    assert by_name["rsi"].oversold == 30.0
    assert all(rule.enabled for rule in rules)

    # 配置关闭某规则 → 构造即禁用
    settings.rules = RuleSettings(
        price_move=settings.rules.price_move.__class__(enabled=False),
    )
    rules = build_default_rules(settings)
    by_name = {rule.name: rule for rule in rules}
    assert by_name["price_move"].enabled is False


def test_engine_rule_exception_isolated(base_window: list[Candle]) -> None:
    """单规则评估异常被记录并跳过，不影响其他规则。"""

    class BrokenRule(Rule):
        name = "broken"
        enabled = True

        def evaluate(self, ctx: RuleContext):
            raise RuntimeError("模拟规则内部错误")

    engine = RuleEngine([BrokenRule(), PriceMoveRule()])
    ctx = make_ctx(base_window, make_snapshot(close=106.0))
    alerts = engine.scan(ctx)
    assert len(alerts) == 1
    assert alerts[0].rule == "price_move"


def test_engine_reset_calls_rule_reset() -> None:
    """reset 调用每条规则的 reset（有状态规则可清理）。"""

    class StatefulRule(Rule):
        name = "stateful"
        enabled = True

        def __init__(self) -> None:
            self.reset_called = False

        def evaluate(self, ctx: RuleContext):
            from quant.models import RuleResult

            return RuleResult(triggered=False)

        def reset(self) -> None:
            self.reset_called = True

    rule = StatefulRule()
    engine = RuleEngine([rule])
    engine.reset()
    assert rule.reset_called is True


def test_rules_are_rule_subclasses() -> None:
    """4 条内置规则均为 Rule 子类且名字稳定。"""
    for rule in (PriceMoveRule(), VolumeSpikeRule(), RsiRule(), MaCrossRule()):
        assert isinstance(rule, Rule)
        assert rule.name in {"price_move", "volume_spike", "rsi", "ma_cross"}
        assert rule.level in {"info", "warning"}


def test_indicator_engine_driven_rule_trigger() -> None:
    """端到端小样例：真实指标引擎产出快照 → 规则触发（未收盘不触发）。"""
    engine = IndicatorEngine()
    closes = [100.0] * 25 + [106.0]
    candles = [
        Candle(
            symbol="BTCUSDT",
            open_time_ms=BASE_OPEN_MS + i * MINUTE_MS,
            close_time_ms=BASE_OPEN_MS + i * MINUTE_MS + MINUTE_MS - 1,
            open=close - 0.5,
            high=close + 0.5,
            low=close - 1.0,
            close=close,
            volume=10.0,
            quote_volume=close * 10.0,
            trades=10,
            is_closed=True,
        )
        for i, close in enumerate(closes)
    ]
    snapshot = None
    for candle in candles:
        snapshot = engine.update("BTCUSDT", candle)
    assert snapshot is not None
    rule_engine = RuleEngine([PriceMoveRule(window_min=5, threshold_pct=5.0)])
    ctx = RuleContext(
        symbol="BTCUSDT",
        candle=candles[-1],
        window=engine.windows["BTCUSDT"].to_list(),
        indicators=snapshot,
        prev_indicators=None,
    )
    alerts = rule_engine.scan(ctx)
    assert len(alerts) == 1
    assert alerts[0].rule == "price_move"
