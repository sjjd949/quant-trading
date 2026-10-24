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
                        direction=result.direction or "",
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

    def load_custom(self, entries: list[str]) -> None:
        """动态加载自定义规则（``"模块路径:类名"``），引擎核心零改动。

        参数:
            entries: 自定义规则登记列表，元素形如
                ``"quant.scanner.rules_custom_example:ExampleSurgeRule"``。

        异常:
            ValueError: 登记格式错误、导入失败、实例化失败、未继承 :class:`Rule`
                或规则名与既有规则重复（消息为中文，便于直接展示）。
        """
        import importlib

        for entry in entries:
            if not isinstance(entry, str) or ":" not in entry:
                raise ValueError(
                    f"自定义规则登记格式错误：{entry!r}（应为 '模块路径:类名'）"
                )
            module_path, _, class_name = entry.partition(":")
            module_path, class_name = module_path.strip(), class_name.strip()
            if not module_path or not class_name:
                raise ValueError(f"自定义规则登记格式错误：{entry!r}（模块或类名为空）")
            try:
                module = importlib.import_module(module_path)
            except Exception as exc:
                raise ValueError(f"自定义规则 {entry} 导入模块失败：{exc}") from exc
            try:
                rule_cls = getattr(module, class_name)
            except AttributeError as exc:
                raise ValueError(
                    f"自定义规则 {entry} 模块中不存在类 {class_name}"
                ) from exc
            try:
                rule = rule_cls()
            except Exception as exc:
                raise ValueError(f"自定义规则 {entry} 实例化失败：{exc}") from exc
            if not isinstance(rule, Rule):
                raise ValueError(f"自定义规则 {entry} 未继承 Rule 基类")
            if any(existing.name == rule.name for existing in self.rules):
                raise ValueError(
                    f"自定义规则 {entry} 名称 {rule.name!r} 与既有规则重复"
                )
            self.rules.append(rule)
            logger.info("已加载自定义规则：%s（%s）", rule.name, entry)

    def reset(self) -> None:
        """重置全部规则内部状态。"""
        for rule in self.rules:
            rule.reset()
