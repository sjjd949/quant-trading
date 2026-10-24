"""规则引擎：注册 / 开关 / 扫描调度。

- ``scan(ctx)`` 遍历启用规则，产出 :class:`~quant.models.Alert` 列表；
- ``Alert.ts`` 取 ``ctx.candle.close_time_ms`` 对应 UTC 时间；
- 链路全同步无 IO，满足「规则触发到告警延迟 < 2s」。
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional

from quant.models import Alert
from quant.scanner.base import Rule, RuleContext
from quant.scanner.rules import build_default_rules
from quant.utils.timeutils import ms_to_datetime

logger = logging.getLogger("quant.scanner.engine")


class RuleEngine:
    """规则引擎：管理规则集合并调度扫描。"""

    def __init__(self, rules: Optional[Iterable[Rule]] = None) -> None:
        """初始化引擎。

        参数:
            rules: 规则列表；``None`` 时按默认配置构造 4 条内置规则
                （经 :func:`~quant.scanner.rules.build_default_rules`）。
        """
        self.rules: list[Rule] = list(rules) if rules is not None else build_default_rules(self._default_settings())

    @staticmethod
    def _default_settings():
        """构造默认配置（供无参初始化使用）。"""
        from quant.config import Settings

        return Settings()

    def scan(self, ctx: RuleContext) -> list[Alert]:
        """扫描一条已收盘 K 线，返回触发的告警列表（可能为空）。"""
        alerts: list[Alert] = []
        for rule in self.rules:
            if not rule.enabled:
                continue
            try:
                result = rule.evaluate(ctx)
            except Exception as exc:  # 单规则异常不得中断其他规则
                logger.warning("规则 %s 评估异常（已跳过）：%s", rule.name, exc)
                continue
            if result.triggered:
                alerts.append(
                    Alert(
                        ts=ms_to_datetime(ctx.candle.close_time_ms),
                        symbol=ctx.symbol,
                        rule=rule.name,
                        message=result.message,
                        trigger_value=result.trigger_value,
                        threshold=result.threshold,
                        level=rule.level,
                    )
                )
        return alerts

    def set_enabled(self, name: str, enabled: bool) -> bool:
        """设置指定名称规则的开关。

        返回:
            ``True`` 表示找到并设置；``False`` 表示无此规则。
        """
        for rule in self.rules:
            if rule.name == name:
                rule.enabled = enabled
                logger.info("规则 %s 开关已设置为 %s", name, enabled)
                return True
        logger.warning("尝试设置不存在的规则开关：%s", name)
        return False

    def rule_names(self) -> list[str]:
        """返回全部规则名（按注册顺序）。"""
        return [rule.name for rule in self.rules]

    def reset(self) -> None:
        """重置全部规则内部状态。"""
        for rule in self.rules:
            rule.reset()
