"""模拟盘引擎：虚拟账户 + 滑点/费率模型 + auto/manual 双模式。

设计要点（对应 M2-D4/D5 与 PRD-M2 §5.4）：
- 虚拟账户：每市场 100,000 计价货币（可配置），现金与持仓隔离核算，重启可恢复；
- 诚实成交：市价单成交价 = 最新价 ± 滑点（bps，不利于成交方）；
- 费率按市场费率表精确计算（加密/A股/美股/港股）；
- 信号方向 ``long``（开仓买入）/ ``close``（平仓卖出），仅多头；
- 下单规模 = 可用现金 ``position_pct%``，A股向下取整到 100 股；
- 单标的单仓位；long 已有持仓→ignored，close 无持仓→no_position；
- auto 模式信号即下单；manual 模式入 FIFO 待确认队列，``y``/``n``/超时处理；
- 每笔成交事务更新账户与当前持仓，事件落 signals/orders/fills/positions 四表。
"""

from __future__ import annotations

import logging
import math
from collections import deque
from typing import Callable, Optional

from quant.config import PaperSettings
from quant.models import (
    Account,
    AccountView,
    Fill,
    Order,
    PositionEvent,
    SignalRecord,
)
from quant.storage.sqlite_store import Storage
from quant.utils.timeutils import now_ms

logger = logging.getLogger("quant.paper.engine")

#: 支持的市场（计价货币：USDT/CNY/USD/HKD）
MARKETS = ("crypto", "a_share", "us", "hk")

#: 浮点比较容差（金额/数量）
_EPS = 1e-6


class PaperEngine:
    """模拟盘引擎：账户、成交模拟、双模式与待确认队列。"""

    def __init__(
        self,
        settings: PaperSettings,
        storage: Storage,
        clock_ms: Optional[Callable[[], int]] = None,
    ) -> None:
        """初始化引擎并恢复账户/持仓当前态。

        参数:
            settings: 模拟盘配置（资金/滑点/费率/仓位约束/模式）。
            storage: SQLite 存储（账户/持仓当前态与四表事件）。
            clock_ms: 当前 epoch ms 函数（测试注入固定时钟）。
        """
        self.settings = settings
        self.store = storage
        self.mode = settings.mode
        self._clock_ms = clock_ms if clock_ms is not None else now_ms
        self.accounts: dict[str, Account] = {}
        self.positions: dict[str, dict] = {}
        self.pending: deque[SignalRecord] = deque()
        self._last_prices: dict[str, float] = {}
        self._load_state()

    # ------------------------------------------------------------------
    # 状态恢复
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        """从存储恢复账户与当前持仓；缺失账户按初始资金建账。"""
        for market in MARKETS:
            account = self.store.get_account(market)
            if account is None:
                account = Account(market=market, cash=self._initial_cash(market), realized_pnl=0.0)
                self.store.save_account(account)
            self.accounts[market] = account
        for row in self.store.get_positions_current():
            self.positions[str(row["symbol"])] = dict(row)

    def _initial_cash(self, market: str) -> float:
        """该市场初始虚拟资金（计价货币）。"""
        return float(getattr(self.settings.initial_cash, market, 0.0))

    # ------------------------------------------------------------------
    # 信号入口
    # ------------------------------------------------------------------

    def on_signal(self, signal: SignalRecord) -> None:
        """接收一条规则信号：auto 直接下单，manual 入待确认队列。"""
        self.store.save_signal(signal)
        if self.mode == "manual":
            signal.status = "pending"
            self.store.update_signal_status(signal.id, "pending")
            self.pending.append(signal)
            logger.info("模拟盘待确认：%s %s(%s)", signal.symbol, signal.rule, signal.direction)
            return
        self._execute(signal)

    def confirm_head(self) -> None:
        """``y``：对队首（FIFO）待确认信号执行下单。"""
        if not self.pending:
            return
        signal = self.pending.popleft()
        self._execute(signal)

    def ignore_head(self) -> None:
        """``n``：忽略队首待确认信号（记 ignored）。"""
        if not self.pending:
            return
        signal = self.pending.popleft()
        self._set_status(signal, "ignored")

    def expire_pending(self) -> None:
        """看门狗：将超时未确认的信号标记为 timeout（FIFO 早于超时线者）。"""
        if self.settings.confirm_timeout_sec <= 0:
            return
        now = self._clock_ms()
        timeout_ms = self.settings.confirm_timeout_sec * 1000
        while self.pending and now - self.pending[0].ts_ms >= timeout_ms:
            signal = self.pending.popleft()
            self._set_status(signal, "timeout")

    def toggle_mode(self) -> str:
        """``m``：运行时切换 auto/manual，返回切换后的模式。"""
        self.mode = "auto" if self.mode == "manual" else "manual"
        self.settings.mode = self.mode
        logger.info("模拟盘模式切换为 %s", self.mode)
        return self.mode

    def update_price(self, symbol: str, price: float) -> None:
        """更新最新价（供浮动盈亏与账户视图计算）。"""
        if price > 0:
            self._last_prices[symbol] = float(price)

    # ------------------------------------------------------------------
    # 视图
    # ------------------------------------------------------------------

    def pending_view(self) -> list[SignalRecord]:
        """待确认队列快照（看板待确认区）。"""
        return list(self.pending)

    def accounts_view(self) -> dict[str, AccountView]:
        """各市场账户视图（现金 / 持仓市值 / 浮动盈亏 / 已实现盈亏 / 未平仓数）。"""
        views: dict[str, AccountView] = {}
        for market, account in self.accounts.items():
            position_value = 0.0
            floating = 0.0
            count = 0
            for symbol, position in self.positions.items():
                if position.get("market") != market:
                    continue
                count += 1
                qty = float(position["qty"])
                avg_cost = float(position["avg_cost"])
                last = self._last_prices.get(symbol, avg_cost)
                market_value = qty * last
                position_value += market_value
                floating += market_value - qty * avg_cost
            views[market] = AccountView(
                market=market,
                cash=account.cash,
                position_market_value=position_value,
                floating_pnl=floating,
                realized_pnl=account.realized_pnl,
                open_positions=count,
            )
        return views

    # ------------------------------------------------------------------
    # 执行
    # ------------------------------------------------------------------

    def _execute(self, signal: SignalRecord) -> None:
        """执行一条信号（开仓或平仓）。"""
        if signal.direction not in ("long", "close"):
            self._set_status(signal, "ignored")
            return
        account = self.accounts.get(signal.market)
        if account is None:  # 未知市场：按初始资金建账
            account = Account(signal.market, self._initial_cash(signal.market), 0.0)
            self.accounts[signal.market] = account
            self.store.save_account(account)
        if signal.direction == "long":
            self._execute_long(signal, account)
        else:
            self._execute_close(signal, account)

    def _execute_long(self, signal: SignalRecord, account: Account) -> None:
        """开仓买入：计算滑点价/费用/数量，落订单/成交/持仓事件。"""
        symbol = signal.symbol
        if self.settings.one_position_per_symbol and symbol in self.positions:
            self._set_status(signal, "ignored")
            return

        ref_price = float(signal.trigger_value)
        fill_price = ref_price * (1.0 + self.settings.slippage_bps / 10_000.0)
        qty = self._size_qty(account.cash, fill_price, signal.market)
        if qty <= 0:
            self._reject(signal, symbol, "buy", qty, ref_price, "资金不足或下单数量为0")
            return

        amount = qty * fill_price
        fee = self._fee(signal.market, "buy", amount)
        total_cost = amount + fee
        if total_cost > account.cash + _EPS:
            self._reject(signal, symbol, "buy", qty, ref_price, "资金不足")

        now = self._clock_ms()
        order = Order(
            id=None, ts_ms=now, signal_id=signal.id, symbol=symbol,
            side="buy", qty=qty, ref_price=ref_price, status="filled",
        )
        self.store.save_order(order)
        fill = Fill(
            id=None, order_id=order.id, ts_ms=now, symbol=symbol,
            qty=qty, fill_price=fill_price, fee=fee, amount=amount,
        )
        self.store.save_fill(fill)

        account.cash -= total_cost
        self.store.save_account(account)

        avg_cost = total_cost / qty  # 成本含买入费用
        self.positions[symbol] = {
            "symbol": symbol, "market": signal.market,
            "qty": qty, "avg_cost": avg_cost, "opened_ts": now,
        }
        self.store.update_position_current(symbol, signal.market, qty, avg_cost, now)
        self.store.save_position(
            PositionEvent(id=None, ts_ms=now, symbol=symbol, event="open", qty=qty,
                          price=fill_price, realized_pnl=0.0)
        )
        self._set_status(signal, "ordered", order.id)
        logger.info("开仓成交：%s %s 量=%.6f 价=%.4f 费=%.4f", symbol, signal.rule, qty, fill_price, fee)

    def _execute_close(self, signal: SignalRecord, account: Account) -> None:
        """平仓卖出：按持仓全量卖出，计算已实现盈亏。"""
        symbol = signal.symbol
        position = self.positions.get(symbol)
        if position is None:
            self._set_status(signal, "no_position")
            return

        ref_price = float(signal.trigger_value)
        fill_price = ref_price * (1.0 - self.settings.slippage_bps / 10_000.0)
        qty = float(position["qty"])
        avg_cost = float(position["avg_cost"])
        amount = qty * fill_price
        fee = self._fee(signal.market, "sell", amount)
        realized = amount - fee - qty * avg_cost

        now = self._clock_ms()
        order = Order(
            id=None, ts_ms=now, signal_id=signal.id, symbol=symbol,
            side="sell", qty=qty, ref_price=ref_price, status="filled",
        )
        self.store.save_order(order)
        fill = Fill(
            id=None, order_id=order.id, ts_ms=now, symbol=symbol,
            qty=qty, fill_price=fill_price, fee=fee, amount=amount,
        )
        self.store.save_fill(fill)

        account.cash += amount - fee
        account.realized_pnl += realized
        self.store.save_account(account)

        del self.positions[symbol]
        self.store.update_position_current(symbol, signal.market, 0.0, 0.0, now)
        self.store.save_position(
            PositionEvent(id=None, ts_ms=now, symbol=symbol, event="close", qty=qty,
                          price=fill_price, realized_pnl=realized)
        )
        self._set_status(signal, "ordered", order.id)
        logger.info("平仓成交：%s 量=%.6f 价=%.4f 已实现盈亏=%.4f", symbol, qty, fill_price, realized)

    def _reject(
        self, signal: SignalRecord, symbol: str, side: str,
        qty: float, ref_price: float, reason: str,
    ) -> None:
        """落一条拒绝订单（含原因）并记录信号状态。"""
        now = self._clock_ms()
        order = Order(
            id=None, ts_ms=now, signal_id=signal.id, symbol=symbol,
            side=side, qty=qty, ref_price=ref_price, status="rejected", reason=reason,
        )
        self.store.save_order(order)
        self._set_status(signal, "rejected", order.id)
        logger.info("订单拒绝：%s %s 原因=%s", symbol, side, reason)

    # ------------------------------------------------------------------
    # 费用与规模
    # ------------------------------------------------------------------

    def _fee(self, market: str, side: str, amount: float) -> float:
        """按市场费率表计算单边费用（金额为成交额）。"""
        fees = self.settings.fees
        if market == "crypto":
            return amount * fees.crypto_taker
        if market == "a_share":
            fee = amount * fees.a_share_commission + amount * fees.a_share_transfer
            if side == "sell":
                fee += amount * fees.a_share_stamp_tax
            return fee
        if market == "us":
            return amount * fees.us_commission
        if market == "hk":
            return amount * fees.hk_commission + amount * fees.hk_stamp_tax
        return 0.0

    def _size_qty(self, cash: float, fill_price: float, market: str) -> float:
        """按 ``position_pct`` 计算下单数量；A股取整到 100 股，美港股取整。"""
        if fill_price <= 0:
            return 0.0
        raw = cash * self.settings.position_pct / 100.0 / fill_price
        if market == "a_share":
            return float(math.floor(raw / 100.0) * 100)
        if market in ("us", "hk"):
            return float(math.floor(raw))
        return math.floor(raw * 1_000_000) / 1_000_000  # crypto 六位小数

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    def _set_status(
        self, signal: SignalRecord, status: str, order_id: Optional[int] = None
    ) -> None:
        """更新信号状态（并同步存储），可选关联订单。"""
        signal.status = status
        if order_id is not None:
            signal.order_id = order_id
        if signal.id is not None:
            self.store.update_signal_status(signal.id, status, order_id)
