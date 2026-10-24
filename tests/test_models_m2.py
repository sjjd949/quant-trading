"""M2 模型层测试：新增 5 个数据类构造/字段 + market_from_symbol + RuleResult.direction。"""

from __future__ import annotations

from datetime import datetime, timezone

from quant.models import (
    Account,
    AccountView,
    Alert,
    Fill,
    Order,
    PositionEvent,
    RuleResult,
    SignalRecord,
    market_from_symbol,
)


def test_signal_record_construction() -> None:
    """SignalRecord 字段完整且默认可选字段正确。"""
    sig = SignalRecord(
        id=None,
        ts_ms=1_700_000_000_000,
        market="crypto",
        symbol="BTCUSDT",
        rule="price_move",
        direction="long",
        mode="auto",
        status="pending",
        trigger_value=68000.0,
    )
    assert sig.id is None
    assert sig.market == "crypto"
    assert sig.symbol == "BTCUSDT"
    assert sig.direction == "long"
    assert sig.mode == "auto"
    assert sig.status == "pending"
    assert sig.trigger_value == 68000.0
    assert sig.order_id is None  # 可选字段默认值


def test_signal_record_from_alert() -> None:
    """from_alert 正确派生市场/方向/模式/参考价。"""
    alert = Alert(
        ts=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        symbol="600519.SH",
        rule="rsi",
        message="RSI超卖",
        trigger_value=28.0,
        threshold=30.0,
        level="warning",
        direction="long",
    )
    sig = SignalRecord.from_alert(alert, ref_price=1500.0, market="a_share", mode="manual")
    assert sig.market == "a_share"  # 由调用方派生
    assert sig.symbol == "600519.SH"
    assert sig.rule == "rsi"
    assert sig.direction == "long"  # 由 RuleResult.direction 透传
    assert sig.mode == "manual"
    assert sig.status == "pending"
    assert sig.trigger_value == 1500.0  # 参考价 = 最新价


def test_order_fill_position_event_construction() -> None:
    """Order / Fill / PositionEvent 字段完整。"""
    order = Order(
        id=None,
        ts_ms=1,
        signal_id=2,
        symbol="BTCUSDT",
        side="buy",
        qty=0.1,
        ref_price=68000.0,
        status="filled",
    )
    assert order.side == "buy"
    assert order.status == "filled"
    assert order.reason is None

    fill = Fill(
        id=None,
        order_id=7,
        ts_ms=1,
        symbol="BTCUSDT",
        qty=0.1,
        fill_price=68034.0,
        fee=6.8,
        amount=6803.4,
    )
    assert fill.order_id == 7
    assert fill.fee == 6.8

    event = PositionEvent(
        id=None, ts_ms=1, symbol="BTCUSDT", event="open", qty=0.1, price=68034.0
    )
    assert event.event == "open"
    assert event.realized_pnl == 0.0  # 可选字段默认

    closed = PositionEvent(
        id=1,
        ts_ms=2,
        symbol="BTCUSDT",
        event="close",
        qty=0.1,
        price=70000.0,
        realized_pnl=120.0,
    )
    assert closed.realized_pnl == 120.0


def test_account_and_view() -> None:
    """Account / AccountView 字段。"""
    account = Account(market="crypto", cash=95000.0, realized_pnl=123.0)
    assert account.market == "crypto"
    assert account.cash == 95000.0
    assert account.realized_pnl == 123.0

    view = AccountView(market="crypto", cash=95000.0, open_positions=1)
    assert view.position_market_value == 0.0
    assert view.floating_pnl == 0.0
    assert view.open_positions == 1


def test_market_from_symbol() -> None:
    """symbol 后缀派生市场。"""
    assert market_from_symbol("BTCUSDT") == "crypto"
    assert market_from_symbol("ETHUSDT") == "crypto"
    assert market_from_symbol("600519.SH") == "a_share"
    assert market_from_symbol("000001.SZ") == "a_share"
    assert market_from_symbol("0700.HK") == "hk"
    assert market_from_symbol("AAPL.US") == "us"
    # 大小写不敏感
    assert market_from_symbol("btcusdt") == "crypto"
    assert market_from_symbol("0700.hk") == "hk"


def test_rule_result_direction_default_none() -> None:
    """RuleResult.direction 默认为 None（不进入模拟盘）。"""
    result = RuleResult(triggered=False)
    assert result.direction is None
    triggered = RuleResult(triggered=True, message="x", direction="long")
    assert triggered.direction == "long"
