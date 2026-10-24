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


@dataclass(slots=True)
class RuleResult:
    """单条规则的评估结果；``triggered=False`` 时其余字段留空。"""

    triggered: bool
    message: str = ""
    trigger_value: float = 0.0
    threshold: float = 0.0
