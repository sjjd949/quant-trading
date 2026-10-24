"""规则引擎基座：Rule 抽象基类 + RuleContext + RuleResult 再导出。

约定：
- ``evaluate(ctx)`` 为纯函数（``MaCrossRule`` 读取快照中的上一均线值，
  无其他副作用），返回 :class:`~quant.models.RuleResult`；
- ``triggered=False`` 时其余字段留空；
- 新增规则（P1-5）只需继承 :class:`Rule` 并注册到
  :class:`~quant.scanner.engine.RuleEngine`。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from quant.models import Candle, IndicatorSnapshot, RuleResult

#: RuleResult 再导出（调用方无需跨层 import models）
__all__ = ["Rule", "RuleContext", "RuleResult"]


@dataclass(slots=True)
class RuleContext:
    """一条已收盘 K 线的规则评估上下文。"""

    symbol: str
    candle: Candle
    window: list[Candle]
    indicators: IndicatorSnapshot
    prev_indicators: Optional[IndicatorSnapshot] = None


class Rule(ABC):
    """规则抽象基类。

    类属性:
        name: 规则名（告警 ``Alert.rule`` 与配置开关键）。
        level: 告警级别（``info``/``warning``）。
        enabled: 是否启用（实例可在运行时经 ``RuleEngine.set_enabled`` 切换）。
    """

    name: str = ""
    level: str = "info"
    enabled: bool = True

    @abstractmethod
    def evaluate(self, ctx: RuleContext) -> RuleResult:
        """评估一条已收盘 K 线，返回规则结果。"""

    def reset(self) -> None:
        """重置规则内部状态（默认无状态，无需重置）。"""
