"""示例自定义规则：单根 K 线涨幅超过阈值即触发（演示插件化扩展）。

在 ``config.toml`` 的 ``[rules] custom`` 中登记
``"quant.scanner.rules_custom_example:ExampleSurgeRule"`` 即可加载，
``scanner/engine.py`` 核心无需任何改动。
"""

from __future__ import annotations

from quant.models import RuleResult
from quant.scanner.base import Rule, RuleContext


class ExampleSurgeRule(Rule):
    """单根 K 线涨幅 ≥ ``threshold_pct`` 时触发（多头信号）。"""

    name = "example_surge"
    level = "info"

    def __init__(self, threshold_pct: float = 1.0) -> None:
        """初始化示例规则。

        参数:
            threshold_pct: 单根 K 线涨幅阈值（百分比）。
        """
        self.threshold_pct = float(threshold_pct)

    def evaluate(self, ctx: RuleContext) -> RuleResult:
        """评估单根 K 线涨跌幅。"""
        candle = ctx.candle
        if candle.open <= 0:
            return RuleResult(triggered=False)
        change_pct = (candle.close - candle.open) / candle.open * 100.0
        if change_pct >= self.threshold_pct:
            return RuleResult(
                triggered=True,
                message=f"单根涨幅 {change_pct:.2f}% ≥ {self.threshold_pct:.2f}%",
                trigger_value=change_pct,
                threshold=self.threshold_pct,
                direction="long",
            )
        return RuleResult(triggered=False)
