"""4 条内置规则：价格异动 / 成交量突增 / RSI 极值 / 均线交叉。

每条规则的边界语义（单元测试逐一覆盖）：
- 价格异动：``|N 分钟涨跌幅| >= threshold_pct``（达到即触发）；
- 成交量突增：``当前量 / 量均 > multiplier``（严格大于）；
- RSI 极值：``rsi >= overbought`` 或 ``rsi <= oversold``（达到即触发）；
- 均线交叉：上一快照均线未金叉且当前短均线上穿长均线为金叉，反之为死叉。
"""

from __future__ import annotations

from quant.models import RuleResult
from quant.scanner.base import Rule, RuleContext


class PriceMoveRule(Rule):
    """价格异动：N 分钟涨跌幅超过阈值（上涨或下跌均触发）。

    真实行情场景：SOL 5 分钟内上涨 5.12%（阈值 5%）触发拉升告警；
    或急跌 6% 触发砸盘告警。
    """

    name = "price_move"
    level = "warning"

    def __init__(
        self,
        window_min: int = 5,
        threshold_pct: float = 5.0,
        enabled: bool = True,
    ) -> None:
        """初始化规则。

        参数:
            window_min: 回看窗口（分钟）。
            threshold_pct: 涨跌幅阈值（%，绝对值比较）。
            enabled: 是否启用。
        """
        self.window_min = window_min
        self.threshold_pct = threshold_pct
        self.enabled = enabled

    def evaluate(self, ctx: RuleContext) -> RuleResult:
        """比较当前收盘价与 ``window_min`` 分钟前收盘价的涨跌幅。"""
        window = ctx.window
        need = self.window_min + 1
        if len(window) < need:
            return RuleResult(triggered=False)
        past_close = window[-need].close
        current_close = ctx.candle.close
        if past_close <= 0:
            return RuleResult(triggered=False)
        change_pct = (current_close - past_close) / past_close * 100.0
        if abs(change_pct) < self.threshold_pct:
            return RuleResult(triggered=False)
        direction = "上涨" if change_pct > 0 else "下跌"
        return RuleResult(
            triggered=True,
            message=(
                f"价格异动({change_pct:+.2f}%/{self.window_min}min) "
                f"阈值{self.threshold_pct:g}%"
            ),
            trigger_value=round(abs(change_pct), 4),
            threshold=self.threshold_pct,
        )


class VolumeSpikeRule(Rule):
    """成交量突增：当前 K 线量超过量均的指定倍数。

    真实行情场景：BTC 突然放量至均量 3.4 倍（阈值 3 倍），
    常伴随价格突破或大单进出。
    """

    name = "volume_spike"
    level = "info"

    def __init__(self, multiplier: float = 3.0, enabled: bool = True) -> None:
        """初始化规则。

        参数:
            multiplier: 相对量均的倍数阈值（严格大于才触发）。
            enabled: 是否启用。
        """
        self.multiplier = multiplier
        self.enabled = enabled

    def evaluate(self, ctx: RuleContext) -> RuleResult:
        """比较当前 K 线成交量与指标快照中的量均基线。"""
        vol_ma = ctx.indicators.vol_ma
        if vol_ma is None or vol_ma <= 0:
            return RuleResult(triggered=False)
        current_volume = ctx.candle.volume
        ratio = current_volume / vol_ma
        if ratio <= self.multiplier:
            return RuleResult(triggered=False)
        return RuleResult(
            triggered=True,
            message=(
                f"成交量突增({ratio:.2f}x) 均量基线{vol_ma:.2f} "
                f"阈值{self.multiplier:g}x"
            ),
            trigger_value=round(ratio, 4),
            threshold=self.multiplier,
        )


class RsiRule(Rule):
    """RSI 极值：超买（``>= overbought``）或超卖（``<= oversold``）。

    真实行情场景：RSI14 升破 70 提示超买回调风险；
    跌破 30 提示超卖反弹机会。
    """

    name = "rsi"
    level = "warning"

    def __init__(
        self,
        overbought: float = 70.0,
        oversold: float = 30.0,
        enabled: bool = True,
    ) -> None:
        """初始化规则。

        参数:
            overbought: 超买阈值（达到即触发）。
            oversold: 超卖阈值（达到即触发）。
            enabled: 是否启用。
        """
        self.overbought = overbought
        self.oversold = oversold
        self.enabled = enabled

    def evaluate(self, ctx: RuleContext) -> RuleResult:
        """判断指标快照中的 RSI 是否进入极值区。"""
        rsi = ctx.indicators.rsi
        if rsi is None:
            return RuleResult(triggered=False)
        if rsi >= self.overbought:
            return RuleResult(
                triggered=True,
                message=f"RSI超买({rsi:.2f}) 阈值{self.overbought:g}",
                trigger_value=round(rsi, 4),
                threshold=self.overbought,
            )
        if rsi <= self.oversold:
            return RuleResult(
                triggered=True,
                message=f"RSI超卖({rsi:.2f}) 阈值{self.oversold:g}",
                trigger_value=round(rsi, 4),
                threshold=self.oversold,
            )
        return RuleResult(triggered=False)


class MaCrossRule(Rule):
    """均线交叉：短均线上穿长均线（金叉）或下穿（死叉）。

    依赖 :attr:`IndicatorSnapshot.prev_ma_short`/``prev_ma_long``
    （由 :class:`~quant.indicators.engine.IndicatorEngine` 在上一
    快照中记录），无自身状态，``reset()`` 即无操作。
    """

    name = "ma_cross"
    level = "info"

    def __init__(
        self,
        ma_short: int = 5,
        ma_long: int = 25,
        enabled: bool = True,
    ) -> None:
        """初始化规则。

        参数:
            ma_short: 短均线周期（用于告警文案展示）。
            ma_long: 长均线周期（用于告警文案展示）。
            enabled: 是否启用。
        """
        self.ma_short = ma_short
        self.ma_long = ma_long
        self.enabled = enabled

    def evaluate(self, ctx: RuleContext) -> RuleResult:
        """结合上一快照与当前快照的均线值判断交叉方向。

        前值来源优先取 ``ctx.prev_indicators``（引擎上一快照），
        缺失时回退到当前快照自带的 ``prev_ma_short/prev_ma_long``。
        """
        snapshot = ctx.indicators
        prev = ctx.prev_indicators
        if snapshot.ma_short is None or snapshot.ma_long is None:
            return RuleResult(triggered=False)

        cur_short, cur_long = snapshot.ma_short, snapshot.ma_long
        if prev is not None:
            prev_short, prev_long = prev.ma_short, prev.ma_long
        else:
            prev_short, prev_long = snapshot.prev_ma_short, snapshot.prev_ma_long
        if prev_short is None or prev_long is None:
            return RuleResult(triggered=False)

        if prev_short <= prev_long and cur_short > cur_long:
            return RuleResult(
                triggered=True,
                message=(
                    f"均线金叉(MA{self.ma_short} {cur_short:.2f} 上穿 "
                    f"MA{self.ma_long} {cur_long:.2f})"
                ),
                trigger_value=round(cur_short - cur_long, 4),
                threshold=0.0,
            )
        if prev_short >= prev_long and cur_short < cur_long:
            return RuleResult(
                triggered=True,
                message=(
                    f"均线死叉(MA{self.ma_short} {cur_short:.2f} 下穿 "
                    f"MA{self.ma_long} {cur_long:.2f})"
                ),
                trigger_value=round(cur_short - cur_long, 4),
                threshold=0.0,
            )
        return RuleResult(triggered=False)


def build_default_rules(settings) -> list[Rule]:
    """按配置构造 4 条内置规则（开关与参数来自 :class:`~quant.config.Settings`）。

    参数:
        settings: 全局配置（``settings.rules`` 与 ``settings.indicators``）。
    """
    rules_config = settings.rules
    return [
        PriceMoveRule(
            window_min=rules_config.price_move.window_min,
            threshold_pct=rules_config.price_move.threshold_pct,
            enabled=rules_config.price_move.enabled,
        ),
        VolumeSpikeRule(
            multiplier=rules_config.volume_spike.multiplier,
            enabled=rules_config.volume_spike.enabled,
        ),
        RsiRule(
            overbought=rules_config.rsi.overbought,
            oversold=rules_config.rsi.oversold,
            enabled=rules_config.rsi.enabled,
        ),
        MaCrossRule(
            ma_short=settings.indicators.ma_short,
            ma_long=settings.indicators.ma_long,
            enabled=rules_config.ma_cross.enabled,
        ),
    ]
