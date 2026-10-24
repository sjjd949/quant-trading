"""看板测试：DashboardState 快照语义 + rich 四区渲染内容。"""

from __future__ import annotations

import asyncio
import io
from datetime import datetime, timezone

import pytest
from rich.console import Console

from quant.cli.dashboard import (
    NO_SIGNAL,
    DashboardRenderer,
    DashboardState,
    SymbolView,
)
from quant.feeds.base import FeedStatus
from quant.models import Alert, Candle, Tick

#: 固定时间
BASE_MS = 1_761_287_525_123  # 2025-10-24T06:32:05.123Z


def make_tick(price: float = 68000.0) -> Tick:
    return Tick(
        symbol="BTCUSDT",
        price=price,
        quantity=0.01,
        trade_time_ms=BASE_MS,
        is_buyer_maker=False,
        source="binance_ws",
    )


def make_candle(close: float = 68100.0, volume: float = 12.5) -> Candle:
    return Candle(
        symbol="BTCUSDT",
        open_time_ms=BASE_MS - 60_000,
        close_time_ms=BASE_MS,
        open=68000.0,
        high=close + 10,
        low=67990.0,
        close=close,
        volume=volume,
        quote_volume=close * volume,
        trades=42,
        is_closed=True,
    )


def make_alert(rule: str = "price_move", symbol: str = "BTCUSDT") -> Alert:
    return Alert(
        ts=datetime(2025, 10, 24, 6, 32, 5, 123000, tzinfo=timezone.utc),
        symbol=symbol,
        rule=rule,
        message=f"{symbol} 触发 {rule}(+6.00%/5min) 阈值5%",
        trigger_value=6.0,
        threshold=5.0,
        level="warning",
    )


def render_text(renderable) -> str:
    """将 rich 可渲染对象渲染为纯文本（固定宽度，测试断言用）。"""
    console = Console(file=io.StringIO(), width=100, force_terminal=False, no_color=True)
    console.print(renderable)
    return console.file.getvalue()


@pytest.fixture()
def state() -> DashboardState:
    dashboard = DashboardState(symbols=["BTCUSDT", "ETHUSDT"])
    dashboard.rules_enabled = {"price_move": True, "volume_spike": True, "rsi": False, "ma_cross": True}
    return dashboard


# ---------------------------------------------------------------------------
# DashboardState
# ---------------------------------------------------------------------------


def test_symbol_view_defaults() -> None:
    """SymbolView 默认值：零价格/零涨跌/无信号。"""
    view = SymbolView(symbol="BTCUSDT")
    assert view.last_price == 0.0
    assert view.change_pct == 0.0
    assert view.volume == 0.0
    assert view.last_signal == NO_SIGNAL


def test_update_tick_only_price(state: DashboardState) -> None:
    """update_tick 仅更新最新价与时间（不动涨跌幅/成交量）。"""
    state.update_candle(make_candle(close=68100.0, volume=12.5))
    state.update_tick(make_tick(price=68200.0))
    view = state.symbols["BTCUSDT"]
    assert view.last_price == 68200.0
    assert view.last_update_ms == BASE_MS
    assert view.volume == 12.5  # 成交量仍来自 K 线
    assert view.change_pct == pytest.approx((68100.0 - 68000.0) / 68000.0 * 100.0)


def test_update_candle_change_pct(state: DashboardState) -> None:
    """update_candle 更新价格/成交量/涨跌幅（按当根 K 线开收计）。"""
    state.update_candle(make_candle(close=68068.0, volume=99.0))
    view = state.symbols["BTCUSDT"]
    assert view.last_price == 68068.0
    assert view.volume == 99.0
    assert view.change_pct == pytest.approx(0.1, abs=1e-9)


def test_update_candle_zero_open_guard(state: DashboardState) -> None:
    """开盘价为 0 时涨跌幅保持 0（防御除零）。"""
    candle = make_candle()
    candle.open = 0.0
    state.update_candle(candle)
    assert state.symbols["BTCUSDT"].change_pct == 0.0


def test_push_alert_sets_signal(state: DashboardState) -> None:
    """push_alert 入滚动日志并更新币种信号列。"""
    state.push_alert(make_alert(rule="volume_spike"))
    assert len(state.alerts) == 1
    assert state.symbols["BTCUSDT"].last_signal == "volume_spike"


def test_alert_log_capacity() -> None:
    """滚动日志按 maxlen 淘汰最旧告警。"""
    dashboard = DashboardState(symbols=["BTCUSDT"], max_alerts=3)
    for index in range(5):
        dashboard.push_alert(make_alert(rule=f"rule_{index}"))
    assert len(dashboard.alerts) == 3
    assert [a.rule for a in dashboard.alerts] == ["rule_2", "rule_3", "rule_4"]


def test_snapshot_is_isolated_copy(state: DashboardState) -> None:
    """snapshot 返回浅拷贝：改快照不影响原状态。"""
    state.update_tick(make_tick(price=68000.0))
    state.push_alert(make_alert())
    snapshot = state.snapshot()
    snapshot.symbols["BTCUSDT"].last_price = 1.0
    snapshot.alerts.append(make_alert(rule="rsi"))
    snapshot.rules_enabled["price_move"] = False
    snapshot.paused = True
    assert state.symbols["BTCUSDT"].last_price == 68000.0
    assert len(state.alerts) == 1
    assert state.rules_enabled["price_move"] is True
    assert state.paused is False


def test_snapshot_alerts_is_list(state: DashboardState) -> None:
    """快照 alerts 为 list（deque→list）。"""
    state.push_alert(make_alert())
    snapshot = state.snapshot()
    assert isinstance(snapshot.alerts, list)
    assert len(snapshot.alerts) == 1


def test_ensure_creates_unknown_symbol(state: DashboardState) -> None:
    """未知币种行情到达时自动补建视图。"""
    state.update_tick(make_tick())
    state.update_tick(
        Tick(symbol="SOLUSDT", price=172.0, quantity=1.0, trade_time_ms=BASE_MS, is_buyer_maker=True)
    )
    assert "SOLUSDT" in state.symbols
    assert state.symbols["SOLUSDT"].last_price == 172.0


# ---------------------------------------------------------------------------
# DashboardRenderer
# ---------------------------------------------------------------------------


@pytest.fixture()
def renderer() -> DashboardRenderer:
    return DashboardRenderer(refresh_interval_sec=0.5)


def test_status_bar_connected_and_rules(renderer: DashboardRenderer, state: DashboardState) -> None:
    """状态栏：已连接状态、重连次数、规则开关计数。"""
    state.set_feed_status(FeedStatus(connected=True, reconnect_count=3, detail="已连接"))
    text = render_text(renderer.build_status_bar(state.snapshot()))
    assert "已连接" in text
    assert "重连 3 次" in text
    assert "规则 3/4 开" in text


def test_status_bar_disconnected_and_paused(renderer: DashboardRenderer, state: DashboardState) -> None:
    """状态栏：未连接 + 暂停标记。"""
    state.set_feed_status(FeedStatus(connected=False))
    state.paused = True
    text = render_text(renderer.build_status_bar(state.snapshot()))
    assert "未连接" in text
    assert "已暂停" in text


def test_quote_table_contents(renderer: DashboardRenderer, state: DashboardState) -> None:
    """行情表：币种/最新价/涨跌幅/成交量/信号列齐全。"""
    state.update_candle(make_candle(close=68100.0, volume=12.5))
    state.push_alert(make_alert(rule="price_move"))
    text = render_text(renderer.build_quote_table(state.snapshot()))
    assert "BTCUSDT" in text
    assert "68,100.00" in text
    assert "12.50" in text
    assert "price_move" in text
    assert "实时行情" in text


def test_quote_table_empty(renderer: DashboardRenderer) -> None:
    """无行情时行情表有占位行。"""
    text = render_text(renderer.build_quote_table(DashboardState().snapshot()))
    assert "—" in text


def test_alert_log_contents(renderer: DashboardRenderer, state: DashboardState) -> None:
    """告警日志：时间戳/币种/规则名/消息。"""
    state.push_alert(make_alert())
    text = render_text(renderer.build_alert_log(state.snapshot()))
    assert "06:32:05" in text
    assert "BTCUSDT" in text
    assert "price_move" in text
    assert "触发" in text


def test_alert_log_empty(renderer: DashboardRenderer, state: DashboardState) -> None:
    """无告警时显示占位文案。"""
    text = render_text(renderer.build_alert_log(state.snapshot()))
    assert "暂无告警" in text


def test_rule_panel_on_off(renderer: DashboardRenderer, state: DashboardState) -> None:
    """规则面板：序号/规则名/开关状态。"""
    text = render_text(renderer.build_rule_panel(state.snapshot()))
    assert "price_move" in text
    assert "rsi" in text
    assert "规则开关" in text


def test_renderable_includes_rule_panel_only_when_open(
    renderer: DashboardRenderer, state: DashboardState
) -> None:
    """四区组合：规则面板仅在 rule_panel_open 时出现。"""
    state.rule_panel_open = False
    text_closed = render_text(renderer.build_renderable(state.snapshot()))
    assert "规则开关" not in text_closed
    state.rule_panel_open = True
    text_open = render_text(renderer.build_renderable(state.snapshot()))
    assert "规则开关" in text_open
    assert "状态栏" in text_open
    assert "实时行情" in text_open
    assert "告警日志" in text_open


def test_render_loop_runs_and_stops(renderer: DashboardRenderer, state: DashboardState) -> None:
    """render_loop 按周期刷新并在 stop_event 置位后退出。"""

    async def _run() -> int:
        stop_event = asyncio.Event()
        task = asyncio.create_task(renderer.render_loop(state, stop_event))
        await asyncio.sleep(0.2)  # 至少经历一个刷新周期
        stop_event.set()
        await asyncio.wait_for(task, timeout=2.0)
        return len(renderer.console.file.getvalue()) if hasattr(renderer.console, "file") else 1

    # 使用字符串控制台，避免污染测试终端
    renderer.console = Console(file=io.StringIO(), width=100, force_terminal=False)
    asyncio.run(_run())
    output = renderer.console.file.getvalue()
    assert "实时行情" in output  # Live 至少渲染过一次
