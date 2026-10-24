"""QuantApp 编排器：初始化 → 预热 → 多适配器并行编排 → 监督重启 → 优雅停止。

任务模型（对应 M1 设计 1.3 与 M2 设计 1.3）：
- ``_adapter_loop(adapter)``：每适配器一个监督协程，驱动 ``adapter.events()``，
  经 ``_dispatch`` 分发 Tick→State、Candle→State；已收盘 Candle →
  IndicatorEngine.update → RuleEngine.scan → AlertManager.publish →
  （paper 启用时）PaperEngine.on_signal；
- ``_command_consumer``：键盘命令（p 暂停 / r 规则面板 / 1-9 规则开关 /
  y 确认 / n 忽略 / m 切模式 / q 退出）；
- ``_pending_watchdog``：manual 模式过期待确认扫描；
- ``render_loop``：0.5s 周期读 DashboardState.snapshot() 渲染；
- 监督粒度 = 适配器级（某适配器异常仅该协程按退避重启）。
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
from quant.feeds.eastmoney_poll import EastmoneyPollAdapter
from quant.feeds.tencent_poll import TencentPollAdapter
from quant.indicators.engine import IndicatorEngine
from quant.models import Alert, Candle, SignalRecord, Tick, market_from_symbol
from quant.paper.engine import PaperEngine
from quant.scanner.base import RuleContext
from quant.scanner.engine import RuleEngine
from quant.scanner.rules import build_default_rules
from quant.storage.sqlite_store import Storage

logger = logging.getLogger("quant.app")

#: 任务异常后的重启间隔（秒）
RESTART_DELAY_SEC = 2.0

#: 预热拉取的 1min K 线数量
WARMUP_LIMIT = 300

#: 待确认看门狗扫描周期（秒）
PENDING_WATCHDOG_SEC = 1.0


class QuantApp:
    """量化程序编排器（多适配器 + 模拟盘）。"""

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
        adapters: Optional[list[FeedAdapter]] = None,
    ) -> None:
        """初始化编排器（各依赖可注入，便于集成测试）。

        参数:
            settings: 全局配置。
            adapter: 单行情适配器（向后兼容；等价于 ``adapters=[adapter]``）。
            rest_client: REST 客户端（默认按配置构造）。
            storage: SQLite 存储（默认按配置构造）。
            renderer: 看板渲染器（默认 rich Live）。
            keyboard: 键盘控制器（默认跨平台实现）。
            alert_manager: 告警管理器（默认按配置构造）。
            indicator_engine: 指标引擎（默认按配置构造）。
            rule_engine: 规则引擎（默认按配置构造）。
            adapters: 多适配器列表（优先于 ``adapter``；默认按配置构造三市场）。
        """
        self.settings = settings
        self.storage = storage if storage is not None else Storage(settings.storage.db_path)
        self.rest = rest_client if rest_client is not None else new_rest_client(settings)
        self.indicators = (
            indicator_engine if indicator_engine is not None else IndicatorEngine(settings.indicators)
        )
        self.rules = rule_engine if rule_engine is not None else RuleEngine(build_default_rules(settings))

        # 自定义规则插件（配置登记，加载失败仅告警不阻断启动）
        custom_entries = list(getattr(settings.rules, "custom", []) or [])
        if custom_entries:
            try:
                self.rules.load_custom(custom_entries)
            except ValueError as exc:
                logger.error("自定义规则加载失败（已跳过）：%s", exc)

        self.alert_manager = (
            alert_manager
            if alert_manager is not None
            else AlertManager(
                cooldown_sec=settings.alerts.cooldown_sec,
                log_path=settings.alerts.log_path,
                store=self.storage,
            )
        )
        self.state = DashboardState(symbols=self._seed_symbols(settings))
        self.state.rules_enabled = {rule.name: rule.enabled for rule in self.rules.rules}
        self.state.paper_mode = settings.paper.mode if settings.paper.enabled else ""

        if adapters is not None:
            self.adapters: list[FeedAdapter] = list(adapters)
        elif adapter is not None:
            self.adapters = [adapter]
        else:
            self.adapters = self._build_default_adapters(settings)
        self.adapter: Optional[FeedAdapter] = self.adapters[0] if self.adapters else None

        self.renderer = renderer if renderer is not None else DashboardRenderer(
            refresh_interval_sec=settings.cli.refresh_interval_sec
        )
        self.keyboard = keyboard if keyboard is not None else KeyboardController()
        #: 模拟盘引擎（start 时按配置启用）
        self.paper: Optional[PaperEngine] = None

        self.command_queue: asyncio.Queue[str] = asyncio.Queue()
        self.stop_event: asyncio.Event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """启动：建表 → 模拟盘 → 预热 → spawn 多适配器/命令/渲染/看门狗任务。"""
        self.storage.init_schema()
        if self.settings.paper.enabled:
            self.paper = PaperEngine(self.settings.paper, self.storage)
            self.state.paper_mode = self.paper.mode
            self._sync_paper_state()
        await self._warmup()

        for adapter in self.adapters:
            try:
                await adapter.connect()
            except Exception as exc:
                logger.warning("适配器 %s 连接失败（继续启动）：%s", adapter.name, exc)

        self._tasks = []
        for adapter in self.adapters:
            self._tasks.append(
                asyncio.create_task(
                    self._supervise(
                        f"adapter:{adapter.name}",
                        lambda a=adapter: self._adapter_loop(a),
                    ),
                    name=f"quant-adapter-{adapter.name}",
                )
            )
        self._tasks.append(
            asyncio.create_task(
                self._supervise("command_consumer", self._command_consumer),
                name="quant-command-consumer",
            )
        )
        self._tasks.append(
            asyncio.create_task(
                self._supervise("render_loop", self._render_loop),
                name="quant-render-loop",
            )
        )
        self._tasks.append(
            asyncio.create_task(
                self._supervise("pending_watchdog", self._pending_watchdog),
                name="quant-pending-watchdog",
            )
        )
        self.keyboard.run_in_thread(asyncio.get_running_loop(), self.command_queue)
        logger.info(
            "量化程序启动完成：适配器=%s",
            ",".join(adapter.name for adapter in self.adapters) or "无",
        )

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
        for adapter in self.adapters:
            try:
                await adapter.close()
            except Exception as exc:
                logger.warning("关闭适配器 %s 异常：%s", adapter.name, exc)
        try:
            await self.rest.aclose()
        except Exception as exc:
            logger.warning("关闭 REST 客户端异常：%s", exc)
        self.storage.close()
        logger.info("量化程序已停止")

    # ------------------------------------------------------------------
    # 适配器消费与分发
    # ------------------------------------------------------------------

    async def _adapter_loop(self, adapter: FeedAdapter) -> None:
        """消费单个适配器的事件流并经统一分发链路处理。"""
        async for event in adapter.events():
            self._dispatch(event, adapter)

    async def _feed_consumer(self) -> None:
        """向后兼容：单适配器消费（等价于 ``_adapter_loop(self.adapter)``）。"""
        if self.adapter is not None:
            await self._adapter_loop(self.adapter)

    def _dispatch(self, event, adapter: FeedAdapter) -> None:
        """统一分发：Tick→State；Candle→State/指标/规则/告警/模拟盘。"""
        if isinstance(event, Tick):
            self.state.update_tick(event)
            self.state.set_feed_status(adapter.status())
            self.state.set_adapter_status(adapter.name, adapter.status())
            if self.paper is not None:
                self.paper.update_price(event.symbol, event.price)
            return
        if not isinstance(event, Candle):
            return

        self.state.update_candle(event)
        self.state.set_feed_status(adapter.status())
        self.state.set_adapter_status(adapter.name, adapter.status())
        latency_tag = getattr(adapter, "latency_tag", "")
        if latency_tag:
            self.state.mark_latency(event.symbol, latency_tag)
        if self.paper is not None:
            self.paper.update_price(event.symbol, event.close)
        if not event.is_closed:
            return  # 未收盘 K 线仅更新展示，不触发规则

        previous = self.indicators.latest(event.symbol)
        snapshot = self.indicators.update(event.symbol, event)
        if snapshot is None:
            return  # 窗口不足，规则暂不评估
        if self.state.paused:
            return  # 暂停仅停止扫描，不停止数据与指标更新

        context = RuleContext(
            symbol=event.symbol,
            candle=event,
            window=self.indicators.windows[event.symbol].to_list(),
            indicators=snapshot,
            prev_indicators=previous,
        )
        for alert in self.rules.scan(context):
            if latency_tag and not alert.message.startswith("[延迟"):
                alert.message = f"[延迟约15min] {alert.message}"
            published = self.alert_manager.publish(alert)
            if not published:
                continue
            self.state.push_alert(alert)
            if self.paper is not None and alert.direction:
                signal = SignalRecord.from_alert(
                    alert,
                    ref_price=float(event.close),
                    market=market_from_symbol(event.symbol),
                    mode=self.paper.mode,
                )
                self.paper.on_signal(signal)
        self._sync_paper_state()

    async def _command_consumer(self) -> None:
        """消费键盘命令：p/r/1-9/y/n/m/q。"""
        while not self.stop_event.is_set():
            command = await self.command_queue.get()
            try:
                self._handle_command(command)
            except Exception as exc:
                logger.warning("命令处理异常（已忽略）：%s；命令=%s", exc, command)

    async def _render_loop(self) -> None:
        """渲染循环：0.5s 周期刷新看板。"""
        await self.renderer.render_loop(self.state, self.stop_event)

    async def _pending_watchdog(self) -> None:
        """看门狗：扫描 manual 模式过期待确认信号（默认 300s）。"""
        while not self.stop_event.is_set():
            try:
                if self.paper is not None:
                    self.paper.expire_pending()
                    self._sync_paper_state()
            except Exception as exc:
                logger.warning("待确认看门狗异常（已忽略）：%s", exc)
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=PENDING_WATCHDOG_SEC)
            except asyncio.TimeoutError:
                pass

    def _handle_command(self, command: str) -> None:
        """处理单条键盘命令。"""
        if command == "pause":
            self.state.paused = not self.state.paused
            logger.info("扫描%s", "已暂停" if self.state.paused else "已恢复")
        elif command == "rule_panel":
            self.state.rule_panel_open = not self.state.rule_panel_open
        elif command.startswith("toggle_rule:"):
            self._toggle_rule(command.split(":", 1)[1])
        elif command == "confirm_signal":
            if self.paper is not None:
                self.paper.confirm_head()
                self._sync_paper_state()
        elif command == "ignore_signal":
            if self.paper is not None:
                self.paper.ignore_head()
                self._sync_paper_state()
        elif command == "toggle_mode":
            if self.paper is not None:
                self.paper.toggle_mode()
                self.state.paper_mode = self.paper.mode
        elif command == "quit":
            logger.info("收到退出命令")
            self.stop_event.set()
        else:
            logger.debug("未知命令：%s", command)

    def _toggle_rule(self, index_text: str) -> None:
        """按序号切换规则开关（1 起，泛化到 1-9）。"""
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

    # ------------------------------------------------------------------
    # 预热与监督
    # ------------------------------------------------------------------

    async def _warmup(self) -> None:
        """启动预热：币安 crypto（REST）+ A股（东财 K 线），失败不阻断启动。"""
        symbols = list(self.settings.market.symbols)
        if symbols:
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

        # A股轮询适配器预热（可选，失败降级冷启动）
        for adapter in self.adapters:
            if not isinstance(adapter, EastmoneyPollAdapter):
                continue
            try:
                result = await adapter.preheat(WARMUP_LIMIT)
            except Exception as exc:
                logger.warning("A股预热异常（降级冷启动）：%s", exc)
                continue
            for symbol, candles in result.items():
                self.indicators.warmup(symbol, candles)
                self.storage.save_candles(symbol, candles)
        logger.info("预热完成：crypto=%d 个", len(symbols))

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
        """同步首个适配器状态到看板（供外部轮询/测试断言）。"""
        if self.adapter is None:
            return self.state.feed_status
        status = self.adapter.status()
        self.state.set_feed_status(status)
        return status

    # ------------------------------------------------------------------
    # 构造辅助
    # ------------------------------------------------------------------

    @staticmethod
    def _seed_symbols(settings: Settings) -> list[str]:
        """看板预置标的：crypto + A股 + 美港股自选（行情到达时自动补建）。"""
        symbols = list(settings.market.symbols)
        if settings.market.a_share.enabled:
            symbols += list(settings.market.a_share.watchlist)
        if settings.market.hk_us.enabled:
            symbols += list(settings.market.hk_us.watchlist_us)
            symbols += list(settings.market.hk_us.watchlist_hk)
        # 去重保序
        seen: set[str] = set()
        ordered: list[str] = []
        for symbol in symbols:
            if symbol not in seen:
                seen.add(symbol)
                ordered.append(symbol)
        return ordered

    def _build_default_adapters(self, settings: Settings) -> list[FeedAdapter]:
        """按配置构造三市场适配器（币安 WS / A股轮询 / 美港股轮询）。"""
        adapters: list[FeedAdapter] = []
        if settings.market.symbols:
            adapters.append(
                BinanceWSAdapter(
                    symbols=list(settings.market.symbols),
                    ws_url=settings.network.ws_url,
                    proxy=settings.network.proxy,
                    idle_timeout_sec=settings.network.ws_idle_timeout_sec,
                    rest_client=self.rest,
                )
            )
        if settings.market.a_share.enabled and settings.market.a_share.watchlist:
            adapters.append(
                EastmoneyPollAdapter(
                    watchlist=list(settings.market.a_share.watchlist),
                    poll_interval_sec=settings.market.a_share.poll_interval_sec,
                    proxy=settings.network.proxy,
                )
            )
        if settings.market.hk_us.enabled and (
            settings.market.hk_us.watchlist_us or settings.market.hk_us.watchlist_hk
        ):
            adapters.append(
                TencentPollAdapter(
                    watchlist_us=list(settings.market.hk_us.watchlist_us),
                    watchlist_hk=list(settings.market.hk_us.watchlist_hk),
                    poll_interval_sec=settings.market.hk_us.poll_interval_sec,
                    proxy=settings.network.proxy,
                )
            )
        if not adapters:  # 兜底：至少一个币安适配器
            adapters.append(
                BinanceWSAdapter(
                    symbols=["BTCUSDT"],
                    ws_url=settings.network.ws_url,
                    proxy=settings.network.proxy,
                    idle_timeout_sec=settings.network.ws_idle_timeout_sec,
                    rest_client=self.rest,
                )
            )
        return adapters

    def _sync_paper_state(self) -> None:
        """把模拟盘账户/待确认/模式同步到看板状态（渲染任务只读快照）。"""
        if self.paper is None:
            return
        self.state.paper_accounts = self.paper.accounts_view()
        self.state.pending = self.paper.pending_view()
        self.state.paper_mode = self.paper.mode
