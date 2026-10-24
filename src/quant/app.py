"""QuantApp 编排器：初始化 → 预热 → 三任务编排 → 监督重启 → 优雅停止。

任务模型（对应设计 1.3）：
- ``_feed_consumer``：驱动 ``adapter.events()``，分发 Tick→State、
  Candle→State；已收盘 Candle → IndicatorEngine.update → RuleEngine.scan
  → AlertManager.publish（全同步，无队列等待）；
- ``_command_consumer``：消费键盘命令 Queue（p 暂停 / r 规则面板 /
  1-4 规则开关 / q 退出）；
- ``render_loop``：0.5s 周期读 DashboardState.snapshot() 渲染；
- 监督策略：任一任务异常退出 → 记录 exc_info 后按 2s 间隔重启；
  仅 ``stop_event`` 为唯一退出通道。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from quant.alerts.manager import AlertManager
from quant.cli.dashboard import DashboardRenderer, DashboardState
from quant.cli.keyboard import KeyboardController
from quant.config import Settings
from quant.feeds.base import FeedAdapter, FeedStatus
from quant.feeds.binance_rest import BinanceRestClient, new_rest_client
from quant.feeds.binance_ws import BinanceWSAdapter
from quant.indicators.engine import IndicatorEngine
from quant.models import Alert, Candle, Tick
from quant.scanner.base import RuleContext
from quant.scanner.engine import RuleEngine
from quant.scanner.rules import build_default_rules
from quant.storage.sqlite_store import Storage

logger = logging.getLogger("quant.app")

#: 任务异常后的重启间隔（秒）
RESTART_DELAY_SEC = 2.0

#: 预热拉取的 1min K 线数量
WARMUP_LIMIT = 300


class QuantApp:
    """量化程序编排器。"""

    def __init__(
        self,
        settings: Settings,
        adapter: Optional[FeedAdapter] = None,
        rest_client: Optional[BinanceRestClient] = None,
        storage: Optional[Storage] = None,
        renderer: Optional[DashboardRenderer] = None,
        keyboard: Optional[KeyboardController] = None,
        alert_manager: Optional[AlertManager] = None,
        indicator_engine: Optional[IndicatorEngine] = None,
        rule_engine: Optional[RuleEngine] = None,
    ) -> None:
        """初始化编排器（各依赖可注入，便于集成测试）。

        参数:
            settings: 全局配置。
            adapter: 行情适配器（默认构造币安 WS 适配器）。
            rest_client: REST 客户端（默认按配置构造）。
            storage: SQLite 存储（默认按配置构造）。
            renderer: 看板渲染器（默认 rich Live）。
            keyboard: 键盘控制器（默认跨平台实现）。
            alert_manager: 告警管理器（默认按配置构造）。
            indicator_engine: 指标引擎（默认按配置构造）。
            rule_engine: 规则引擎（默认按配置构造）。
        """
        self.settings = settings
        self.storage = storage if storage is not None else Storage(settings.storage.db_path)
        self.rest = rest_client if rest_client is not None else new_rest_client(settings)
        self.indicators = (
            indicator_engine if indicator_engine is not None else IndicatorEngine(settings.indicators)
        )
        self.rules = rule_engine if rule_engine is not None else RuleEngine(build_default_rules(settings))
        self.alert_manager = (
            alert_manager
            if alert_manager is not None
            else AlertManager(
                cooldown_sec=settings.alerts.cooldown_sec,
                log_path=settings.alerts.log_path,
                store=self.storage,
            )
        )
        self.state = DashboardState(symbols=settings.market.symbols)
        self.state.rules_enabled = {rule.name: rule.enabled for rule in self.rules.rules}
        self.adapter = (
            adapter
            if adapter is not None
            else BinanceWSAdapter(
                symbols=settings.market.symbols,
                ws_url=settings.network.ws_url,
                proxy=settings.network.proxy,
                idle_timeout_sec=settings.network.ws_idle_timeout_sec,
                rest_client=self.rest,
            )
        )
        self.renderer = renderer if renderer is not None else DashboardRenderer(
            refresh_interval_sec=settings.cli.refresh_interval_sec
        )
        self.keyboard = keyboard if keyboard is not None else KeyboardController()

        self.command_queue: asyncio.Queue[str] = asyncio.Queue()
        self.stop_event: asyncio.Event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """启动：建表 → 预热 → spawn 三任务 → 等待停止事件 → 收敛。"""
        self.storage.init_schema()
        await self._warmup()

        await self.adapter.connect()
        self._tasks = [
            asyncio.create_task(
                self._supervise("feed_consumer", self._feed_consumer),
                name="quant-feed-consumer",
            ),
            asyncio.create_task(
                self._supervise("command_consumer", self._command_consumer),
                name="quant-command-consumer",
            ),
            asyncio.create_task(
                self._supervise("render_loop", self._render_loop),
                name="quant-render-loop",
            ),
        ]
        self.keyboard.run_in_thread(asyncio.get_running_loop(), self.command_queue)
        logger.info("量化程序启动完成：币种=%s", ",".join(self.settings.market.symbols))

        await self.stop_event.wait()
        await self.stop()

    async def stop(self) -> None:
        """优雅停止：置位停止事件 → 取消任务 → 关闭各组件。"""
        if self.stop_event.is_set() and not self._tasks:
            return  # 已收敛
        self.stop_event.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        self.keyboard.stop()
        try:
            await self.adapter.close()
        except Exception as exc:
            logger.warning("关闭适配器异常：%s", exc)
        try:
            await self.rest.aclose()
        except Exception as exc:
            logger.warning("关闭 REST 客户端异常：%s", exc)
        self.storage.close()
        logger.info("量化程序已停止")

    # ------------------------------------------------------------------
    # 任务
    # ------------------------------------------------------------------

    async def _feed_consumer(self) -> None:
        """消费行情事件：分发 State → 指标 → 规则 → 告警（全同步）。"""
        async for event in self.adapter.events():
            if isinstance(event, Tick):
                self.state.update_tick(event)
                self.state.set_feed_status(self.adapter.status())
                continue
            if not isinstance(event, Candle):
                continue
            self.state.update_candle(event)
            self.state.set_feed_status(self.adapter.status())
            if not event.is_closed:
                continue  # 未收盘 K 线仅更新展示，不触发规则

            previous = self.indicators.latest(event.symbol)
            snapshot = self.indicators.update(event.symbol, event)
            if snapshot is None:
                continue  # 窗口不足，规则暂不评估
            if self.state.paused:
                continue  # 暂停仅停止扫描，不停止数据与指标更新

            context = RuleContext(
                symbol=event.symbol,
                candle=event,
                window=self.indicators.windows[event.symbol].to_list(),
                indicators=snapshot,
                prev_indicators=previous,
            )
            for alert in self.rules.scan(context):
                if self.alert_manager.publish(alert):
                    self.state.push_alert(alert)

    async def _command_consumer(self) -> None:
        """消费键盘命令：p 暂停 / r 规则面板 / 1-4 规则开关 / q 退出。"""
        while not self.stop_event.is_set():
            command = await self.command_queue.get()
            try:
                self._handle_command(command)
            except Exception as exc:
                logger.warning("命令处理异常（已忽略）：%s；命令=%s", exc, command)

    async def _render_loop(self) -> None:
        """渲染循环：0.5s 周期刷新看板。"""
        await self.renderer.render_loop(self.state, self.stop_event)

    def _handle_command(self, command: str) -> None:
        """处理单条键盘命令。"""
        if command == "pause":
            self.state.paused = not self.state.paused
            logger.info("扫描%s", "已暂停" if self.state.paused else "已恢复")
        elif command == "rule_panel":
            self.state.rule_panel_open = not self.state.rule_panel_open
        elif command.startswith("toggle_rule:"):
            self._toggle_rule(command.split(":", 1)[1])
        elif command == "quit":
            logger.info("收到退出命令")
            self.stop_event.set()
        else:
            logger.debug("未知命令：%s", command)

    def _toggle_rule(self, index_text: str) -> None:
        """按序号切换规则开关（1 起）。"""
        try:
            index = int(index_text)
        except ValueError:
            logger.debug("非法规则序号：%s", index_text)
            return
        names = self.rules.rule_names()
        if not 1 <= index <= len(names):
            logger.debug("规则序号越界：%s", index_text)
            return
        name = names[index - 1]
        new_state = not self.state.rules_enabled.get(name, True)
        self.rules.set_enabled(name, new_state)
        self.state.rules_enabled[name] = new_state

    def feed_status(self) -> FeedStatus:
        """同步适配器状态到看板（供外部轮询/测试断言）。"""
        status = self.adapter.status()
        self.state.set_feed_status(status)
        return status

    # ------------------------------------------------------------------
    # 预热与监督
    # ------------------------------------------------------------------

    async def _warmup(self) -> None:
        """启动预热：4 币种并发拉 300 根 1min K 线灌指标窗口（并落库）。"""
        symbols = self.settings.market.symbols
        if not symbols:
            return

        async def _one(symbol: str) -> None:
            try:
                candles = await self.rest.klines(
                    symbol, interval="1m", limit=WARMUP_LIMIT
                )
                self.indicators.warmup(symbol, candles)
                self.storage.save_candles(symbol, candles)
            except Exception as exc:
                logger.warning("预热失败（%s）：%s", symbol, exc)

        await asyncio.gather(*(_one(symbol) for symbol in symbols))
        logger.info("预热完成：%d 个币种", len(symbols))

    async def _supervise(self, name: str, factory) -> None:
        """任务监督：异常退出按 2s 间隔重启，直到 ``stop_event`` 置位。"""
        while not self.stop_event.is_set():
            try:
                await factory()
                return  # 正常结束（如 render_loop 收到停止）
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "任务 %s 异常退出，%.0fs 后重启", name, RESTART_DELAY_SEC, exc_info=True
                )
                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(), timeout=RESTART_DELAY_SEC
                    )
                except asyncio.TimeoutError:
                    continue
                return

    def feed_status(self) -> FeedStatus:
        """同步适配器状态到看板（供外部轮询/测试断言）。"""
        status = self.adapter.status()
        self.state.set_feed_status(status)
        return status
