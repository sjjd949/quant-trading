"""CLI 看板：DashboardState（纯数据快照）+ DashboardRenderer（rich Live 四区）。

设计要点（对应难点 D3）：
- 数据流与渲染彻底分离：``DashboardState`` 为纯数据，渲染任务以
  ``refresh_interval_sec``（默认 0.5s）周期独立刷新；
- ``snapshot()`` 返回浅拷贝（deque→list、dict→copy），渲染任务只读
  快照，与行情消费任务无锁协作；
- 四区：状态栏 / 行情表 / 告警日志 / 规则面板（``r`` 键开关）。
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, replace
from typing import Optional

from rich.console import Console, RenderableType
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from quant.feeds.base import FeedStatus
from quant.models import AccountView, Alert, Candle, Tick, market_from_symbol
from quant.utils.timeutils import datetime_to_ms, to_iso

logger = logging.getLogger("quant.cli.dashboard")

#: 告警滚动日志容量
ALERT_LOG_MAXLEN = 100

#: 无信号占位符
NO_SIGNAL = "—"


@dataclass(slots=True)
class SymbolView:
    """单币种看板视图数据。"""

    symbol: str
    last_price: float = 0.0
    change_pct: float = 0.0
    volume: float = 0.0
    last_update_ms: int = 0
    last_signal: str = NO_SIGNAL
    #: 市场标识（crypto/a_share/us/hk；空则按 symbol 后缀派生）
    market: str = ""
    #: 延迟数据标注（如 ``"~15min"``；实时源为空）
    latency_tag: str = ""


class DashboardState:
    """看板状态：行情消费任务写、渲染任务读快照。"""

    def __init__(self, symbols: Optional[list[str]] = None, max_alerts: int = ALERT_LOG_MAXLEN) -> None:
        """初始化状态。

        参数:
            symbols: 预置币种列表（可为空，行情到达时自动补建）。
            max_alerts: 告警滚动日志容量。
        """
        self.symbols: dict[str, SymbolView] = {
            symbol: SymbolView(symbol=symbol) for symbol in (symbols or [])
        }
        self.alerts: deque[Alert] = deque(maxlen=max_alerts)
        self.feed_status: FeedStatus = FeedStatus()
        self.paused: bool = False
        self.rules_enabled: dict[str, bool] = {}
        self.rule_panel_open: bool = False
        #: 逐适配器连接状态（M2 多适配器）
        self.adapter_statuses: dict[str, FeedStatus] = {}
        #: 模拟盘账户视图（按市场）
        self.paper_accounts: dict[str, AccountView] = {}
        #: 待确认信号队列快照
        self.pending: list = []
        #: 模拟盘模式（auto/manual；空表示未启用）
        self.paper_mode: str = ""

    def set_adapter_status(self, name: str, status: FeedStatus) -> None:
        """记录某适配器的连接状态（看板逐适配器展示）。"""
        self.adapter_statuses[name] = status

    def mark_latency(self, symbol: str, tag: str) -> None:
        """标记某标的为延迟数据源（行情表行级标注）。"""
        if tag:
            self._ensure(symbol).latency_tag = tag

    def update_tick(self, tick: Tick) -> None:
        """用成交流水更新最新价与更新时间（不改动涨跌幅/成交量）。"""
        view = self._ensure(tick.symbol)
        view.last_price = tick.price
        view.last_update_ms = tick.trade_time_ms

    def update_candle(self, candle: Candle) -> None:
        """用 K 线更新最新价/成交量/涨跌幅（涨跌幅按当根 K 线开收计算）。"""
        view = self._ensure(candle.symbol)
        view.last_price = candle.close
        view.volume = candle.volume
        view.last_update_ms = candle.close_time_ms
        if candle.open > 0:
            view.change_pct = (candle.close - candle.open) / candle.open * 100.0

    def push_alert(self, alert: Alert) -> None:
        """推送一条告警到滚动日志，并更新对应币种信号列。"""
        self.alerts.append(alert)
        view = self._ensure(alert.symbol)
        view.last_signal = alert.rule

    def set_feed_status(self, status: FeedStatus) -> None:
        """更新连接状态（由编排器周期性同步）。"""
        self.feed_status = status

    def snapshot(self) -> "DashboardState":
        """返回浅拷贝快照（deque→list、dict→copy），渲染任务只读。"""
        clone = DashboardState()
        clone.symbols = {
            symbol: replace(view) for symbol, view in self.symbols.items()
        }
        clone.alerts = list(self.alerts)
        clone.feed_status = replace(self.feed_status)
        clone.paused = self.paused
        clone.rules_enabled = dict(self.rules_enabled)
        clone.rule_panel_open = self.rule_panel_open
        clone.adapter_statuses = {
            name: replace(status) for name, status in self.adapter_statuses.items()
        }
        clone.paper_accounts = {
            market: replace(view) for market, view in self.paper_accounts.items()
        }
        clone.pending = list(self.pending)
        clone.paper_mode = self.paper_mode
        return clone

    def _ensure(self, symbol: str) -> SymbolView:
        """获取（按需创建）币种视图。"""
        view = self.symbols.get(symbol)
        if view is None:
            view = SymbolView(symbol=symbol)
            self.symbols[symbol] = view
        return view


class DashboardRenderer:
    """rich Live 看板渲染器：四区布局，0.5s 周期刷新。"""

    def __init__(
        self,
        refresh_interval_sec: float = 0.5,
        console: Optional[Console] = None,
    ) -> None:
        """初始化渲染器。

        参数:
            refresh_interval_sec: 刷新周期（秒，配置默认 0.5）。
            console: 可注入的 rich ``Console``（测试用）。
        """
        self.refresh_interval_sec = refresh_interval_sec
        self.console = console if console is not None else Console()
        self._live: Optional[Live] = None

    async def render_loop(self, state: DashboardState, stop_event: asyncio.Event) -> None:
        """渲染循环：以刷新周期读快照并更新 Live，直到 ``stop_event`` 置位。"""
        with Live(
            self.build_renderable(state.snapshot()),
            console=self.console,
            refresh_per_second=max(1.0, 1.0 / self.refresh_interval_sec),
            transient=False,
        ) as live:
            self._live = live
            try:
                while not stop_event.is_set():
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=self.refresh_interval_sec
                        )
                    except asyncio.TimeoutError:
                        pass  # 正常刷新节拍
                    if stop_event.is_set():
                        break
                    live.update(self.build_renderable(state.snapshot()))
            finally:
                self._live = None

    def build_renderable(self, snapshot: DashboardState) -> RenderableType:
        """组合各区块为单一可渲染对象（规则面板/模拟盘/待确认按需包含）。"""
        from rich.console import Group

        parts: list[RenderableType] = [
            self.build_status_bar(snapshot),
            self.build_quote_table(snapshot),
            self.build_alert_log(snapshot),
        ]
        if snapshot.paper_accounts:
            parts.append(self.build_paper_panel(snapshot))
        if snapshot.pending:
            parts.append(self.build_pending_panel(snapshot))
        if snapshot.rule_panel_open:
            parts.append(self.build_rule_panel(snapshot))
        return Group(*parts)

    def build_status_bar(self, snapshot: DashboardState) -> Panel:
        """状态栏：连接状态 / 重连次数 / 规则开关数 / 暂停标记 / 逐适配器状态。"""
        status = snapshot.feed_status
        connected = "●已连接" if status.connected else "○未连接"
        enabled_count = sum(1 for on in snapshot.rules_enabled.values() if on)
        total = len(snapshot.rules_enabled)
        rules_text = f"规则 {enabled_count}/{total} 开" if total else "规则 —"
        paused_text = " [已暂停]" if snapshot.paused else ""
        text = Text.assemble(
            (f"WS {connected}", "green" if status.connected else "red"),
            f" 重连 {status.reconnect_count} 次",
            f" | {rules_text}",
            paused_text,
        )
        if snapshot.paper_mode:
            text.append(f" | 模拟盘[{snapshot.paper_mode}]", style="magenta")
        if snapshot.adapter_statuses:
            segments = [
                f"{name}{'●' if st.connected else '○'}"
                for name, st in snapshot.adapter_statuses.items()
            ]
            text.append(" | " + " ".join(segments), style="dim")
        return Panel(text, title="状态栏", border_style="blue")

    def build_quote_table(self, snapshot: DashboardState) -> Table:
        """行情表：币种 / 市场 / 最新价 / 涨跌幅% / 成交量 / 信号。"""
        table = Table(title="实时行情", expand=True)
        table.add_column("币种", style="cyan", no_wrap=True)
        table.add_column("市场", no_wrap=True)
        table.add_column("最新价", justify="right")
        table.add_column("涨跌幅%", justify="right")
        table.add_column("成交量", justify="right")
        table.add_column("信号", style="yellow")
        for view in snapshot.symbols.values():
            change_style = "green" if view.change_pct >= 0 else "red"
            signal = f"⚡{view.last_signal}" if view.last_signal != NO_SIGNAL else NO_SIGNAL
            symbol_cell = view.symbol
            if view.latency_tag:
                symbol_cell = f"{view.symbol} ⏱{view.latency_tag}"
            market = view.market or market_from_symbol(view.symbol)
            table.add_row(
                symbol_cell,
                _market_label(market),
                f"{view.last_price:,.2f}",
                Text(f"{view.change_pct:+.2f}", style=change_style),
                f"{view.volume:,.2f}",
                signal,
            )
        if not snapshot.symbols:
            table.add_row("—", "—", "—", "—", "—", "—")
        return table

    def build_alert_log(self, snapshot: DashboardState) -> Panel:
        """告警日志（滚动）：时间 / 币种 / 规则 / 详情。"""
        lines: list[Text] = []
        for alert in list(snapshot.alerts)[-10:]:
            stamp = to_iso(datetime_to_ms(alert.ts))[11:19]
            style = "yellow" if alert.level == "warning" else "white"
            lines.append(
                Text.assemble(
                    (f"[{stamp}] ", "dim"),
                    (f"{alert.symbol} ", "cyan"),
                    (f"触发 {alert.rule} ", style),
                    alert.message,
                )
            )
        if not lines:
            lines.append(Text("暂无告警", style="dim"))
        from rich.console import Group

        return Panel(Group(*lines), title="告警日志", border_style="yellow")

    def build_paper_panel(self, snapshot: DashboardState) -> Panel:
        """模拟盘账户区：分市场 现金 / 持仓市值 / 浮动盈亏 / 已实现盈亏 / 未平仓数。"""
        table = Table(expand=True)
        table.add_column("市场")
        table.add_column("现金", justify="right")
        table.add_column("持仓市值", justify="right")
        table.add_column("浮动盈亏", justify="right")
        table.add_column("已实现盈亏", justify="right")
        table.add_column("未平仓", justify="right")
        for market, view in snapshot.paper_accounts.items():
            floating_style = "green" if view.floating_pnl >= 0 else "red"
            realized_style = "green" if view.realized_pnl >= 0 else "red"
            table.add_row(
                _market_label(market),
                f"{view.cash:,.2f}",
                f"{view.position_market_value:,.2f}",
                Text(f"{view.floating_pnl:+,.2f}", style=floating_style),
                Text(f"{view.realized_pnl:+,.2f}", style=realized_style),
                str(view.open_positions),
            )
        if not snapshot.paper_accounts:
            table.add_row("—", "—", "—", "—", "—", "—")
        title = f"模拟盘账户 [{snapshot.paper_mode}]" if snapshot.paper_mode else "模拟盘账户"
        return Panel(table, title=title, border_style="green")

    def build_pending_panel(self, snapshot: DashboardState) -> Panel:
        """待确认区：manual 模式 FIFO 队列（y 下单 / n 忽略）。"""
        table = Table(expand=True)
        table.add_column("标的")
        table.add_column("规则")
        table.add_column("方向")
        table.add_column("触发价", justify="right")
        for signal in snapshot.pending:
            table.add_row(
                str(getattr(signal, "symbol", "—")),
                str(getattr(signal, "rule", "—")),
                str(getattr(signal, "direction", "—")),
                f"{float(getattr(signal, 'trigger_value', 0.0)):,.2f}",
            )
        if not snapshot.pending:
            table.add_row("—", "—", "—", "—")
        return Panel(table, title="待确认信号（y 下单 / n 忽略）", border_style="cyan")

    def build_rule_panel(self, snapshot: DashboardState) -> Panel:
        """规则面板：规则序号 / 名称 / 开关状态（1-4 键切换）。"""
        table = Table(expand=True)
        table.add_column("#", justify="right")
        table.add_column("规则")
        table.add_column("状态", justify="center")
        for index, (name, enabled) in enumerate(snapshot.rules_enabled.items(), start=1):
            table.add_row(
                str(index),
                name,
                Text("开", style="green") if enabled else Text("关", style="red"),
            )
        if not snapshot.rules_enabled:
            table.add_row("—", "—", "—")
        return Panel(table, title="规则开关（1-9 切换，r 关闭面板）", border_style="magenta")


#: 市场标识 → 中文标签
_MARKET_LABELS = {
    "crypto": "加密",
    "a_share": "A股",
    "us": "美股",
    "hk": "港股",
}


def _market_label(market: str) -> str:
    """市场标识转中文标签（未知原样返回）。"""
    return _MARKET_LABELS.get(market, market or "—")
