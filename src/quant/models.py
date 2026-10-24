"""统一数据模型：Tick / Candle / IndicatorSnapshot / Alert / RuleResult。

约定：
- 全部 ``@dataclass(slots=True)``，字段显式类型注解；
- 时间内部一律 ``int`` epoch 毫秒（UTC），展示层经
  :func:`quant.utils.timeutils.to_iso` 转 ISO 8601；
- 指标字段允许 ``None``（窗口不足时无有效值）。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from quant.utils.timeutils import datetime_to_ms


@dataclass(slots=True)
class Tick:
    """一笔成交流水（驱动秒级最新价展示）。"""

    symbol: str
    price: float
    quantity: float
    trade_time_ms: int
    is_buyer_maker: bool
    source: str = "binance"


@dataclass(slots=True)
class Candle:
    """一根 K 线（1min）；仅 ``is_closed=True`` 进入指标/规则链路。"""

    symbol: str
    open_time_ms: int
    close_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trades: int
    is_closed: bool


@dataclass(slots=True)
class IndicatorSnapshot:
    """某币种在某一已收盘 K 线时刻的指标快照。

    ``prev_ma_short``/``prev_ma_long`` 为上一快照的均线值，
    供 :class:`~quant.scanner.rules.MaCrossRule` 判断交叉方向。
    """

    symbol: str
    close: float
    rsi: float | None
    ma_short: float | None
    ma_long: float | None
    vol_ma: float | None
    volume: float
    ts_ms: int
    prev_ma_short: float | None = None
    prev_ma_long: float | None = None


@dataclass(slots=True)
class Alert:
    """一条规则告警（终端高亮 + JSONL + SQLite + CLI 滚动日志）。"""

    ts: datetime
    symbol: str
    rule: str
    message: str
    trigger_value: float
    threshold: float
    level: str = "info"
    #: 信号方向（由 RuleResult.direction 透传）：``"long"`` / ``"close"`` / ``""``
    direction: str = ""


@dataclass(slots=True)
class RuleResult:
    """单条规则的评估结果；``triggered=False`` 时其余字段留空。"""

    triggered: bool
    message: str = ""
    trigger_value: float = 0.0
    threshold: float = 0.0
    #: 信号方向：``"long"`` / ``"close"``；``None`` 表示不进入模拟盘
    direction: str | None = None


@dataclass(slots=True)
class SignalRecord:
    """一条信号记录（规则触发 → 模拟盘下单的纽带，落 signals 表）。

    ``trigger_value`` 为触发时刻的最新价（参考价），用于模拟成交价计算；
    与 :class:`Alert.trigger_value`（规则度量值，可能为涨跌幅/RSI 等）语义不同。
    """

    id: int | None
    ts_ms: int
    market: str
    symbol: str
    rule: str
    direction: str
    mode: str
    status: str
    trigger_value: float
    order_id: int | None = None

    @classmethod
    def from_alert(
        cls,
        alert: "Alert",
        *,
        ref_price: float,
        market: str,
        mode: str,
    ) -> "SignalRecord":
        """由 :class:`Alert` 派生信号记录（参考价取最新价）。

        参数:
            alert: 触发的告警（方向由其 ``direction`` 透传）。
            ref_price: 触发时刻最新价（作为模拟成交参考价）。
            market: 由 symbol 后缀派生的市场（crypto/a_share/us/hk）。
            mode: 模拟盘模式（``auto`` / ``manual``）。
        """
        return cls(
            id=None,
            ts_ms=datetime_to_ms(alert.ts),
            market=market,
            symbol=alert.symbol,
            rule=alert.rule,
            direction=alert.direction or "",
            mode=mode,
            status="pending",
            trigger_value=ref_price,
            order_id=None,
        )


@dataclass(slots=True)
class Order:
    """一笔模拟订单（落 orders 表）。"""

    id: int | None
    ts_ms: int
    signal_id: int | None
    symbol: str
    side: str
    qty: float
    ref_price: float
    status: str
    reason: str | None = None


@dataclass(slots=True)
class Fill:
    """一笔模拟成交（落 fills 表，成交价含滑点、含费用）。"""

    id: int | None
    order_id: int
    ts_ms: int
    symbol: str
    qty: float
    fill_price: float
    fee: float
    amount: float


@dataclass(slots=True)
class PositionEvent:
    """一个持仓事件（open/reduce/close，落 positions 表，含已实现盈亏）。"""

    id: int | None
    ts_ms: int
    symbol: str
    event: str
    qty: float
    price: float
    realized_pnl: float = 0.0


@dataclass(slots=True)
class Account:
    """某市场的模拟账户（落 accounts 表，当前态）。"""

    market: str
    cash: float
    realized_pnl: float = 0.0


@dataclass(slots=True)
class AccountView:
    """看板模拟盘区用的账户视图（聚合现金/持仓市值/浮动盈亏等）。"""

    market: str
    cash: float
    position_market_value: float = 0.0
    floating_pnl: float = 0.0
    realized_pnl: float = 0.0
    open_positions: int = 0


def market_from_symbol(symbol: str) -> str:
    """由 symbol 后缀派生市场标识。

    - 以 ``USDT`` 结尾 → ``crypto``；
    - 以 ``.SH`` / ``.SZ`` 结尾 → ``a_share``；
    - 以 ``.HK`` 结尾 → ``hk``；
    - 以 ``.US`` 结尾 → ``us``；
    - 其余回退为 ``crypto``。
    """
    s = symbol.upper()
    if s.endswith("USDT"):
        return "crypto"
    if s.endswith(".SH") or s.endswith(".SZ"):
        return "a_share"
    if s.endswith(".HK"):
        return "hk"
    if s.endswith(".US"):
        return "us"
    return "crypto"
