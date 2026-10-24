"""M2 看板扩展测试：市场列 / 延迟标注 / 模拟盘区 / 待确认区 / 逐适配器状态。"""

from __future__ import annotations

import io

from rich.console import Console

from quant.cli.dashboard import DashboardRenderer, DashboardState
from quant.feeds.base import FeedStatus
from quant.models import AccountView, SignalRecord


def render_text(renderable) -> str:
    console = Console(file=io.StringIO(), width=120, force_terminal=False, no_color=True)
    console.print(renderable)
    return console.file.getvalue()


def test_quote_table_market_column_and_latency():
    state = DashboardState(symbols=["BTCUSDT", "600519.SH", "AAPL.US", "0700.HK"])
    state.mark_latency("AAPL.US", "~15min")
    text = render_text(DashboardRenderer().build_quote_table(state.snapshot()))
    assert "市场" in text
    assert "加密" in text and "A股" in text and "美股" in text and "港股" in text
    assert "⏱~15min" in text


def test_status_bar_paper_mode_and_adapter_statuses():
    state = DashboardState(symbols=["BTCUSDT"])
    state.paper_mode = "manual"
    state.set_adapter_status("binance_ws", FeedStatus(connected=True))
    state.set_adapter_status("tencent", FeedStatus(connected=False))
    text = render_text(DashboardRenderer().build_status_bar(state.snapshot()))
    assert "模拟盘[manual]" in text
    assert "binance_ws●" in text
    assert "tencent○" in text


def test_paper_panel_contents():
    state = DashboardState()
    state.paper_accounts = {
        "crypto": AccountView(
            market="crypto", cash=90000.0, position_market_value=10000.0,
            floating_pnl=120.0, realized_pnl=30.0, open_positions=1,
        )
    }
    text = render_text(DashboardRenderer().build_paper_panel(state.snapshot()))
    assert "模拟盘账户" in text
    assert "加密" in text
    assert "10,000.00" in text


def test_pending_panel_contents():
    state = DashboardState()
    state.pending = [
        SignalRecord(
            id=1, ts_ms=0, market="crypto", symbol="BTCUSDT", rule="rsi",
            direction="long", mode="manual", status="pending", trigger_value=50000.0,
        )
    ]
    text = render_text(DashboardRenderer().build_pending_panel(state.snapshot()))
    assert "BTCUSDT" in text
    assert "rsi" in text
    assert "long" in text
    assert "y 下单" in text


def test_renderable_includes_paper_and_pending_when_present():
    state = DashboardState(symbols=["BTCUSDT"])
    state.paper_accounts = {"crypto": AccountView(market="crypto", cash=1.0)}
    state.pending = [
        SignalRecord(
            id=1, ts_ms=0, market="crypto", symbol="BTCUSDT", rule="rsi",
            direction="long", mode="manual", status="pending", trigger_value=1.0,
        )
    ]
    text = render_text(DashboardRenderer().build_renderable(state.snapshot()))
    assert "模拟盘账户" in text
    assert "待确认信号" in text


def test_snapshot_isolates_new_fields():
    state = DashboardState()
    state.paper_mode = "auto"
    state.set_adapter_status("x", FeedStatus(connected=True))
    snapshot = state.snapshot()
    snapshot.paper_mode = "manual"
    snapshot.adapter_statuses["x"].connected = False
    assert state.paper_mode == "auto"
    assert state.adapter_statuses["x"].connected is True
