"""集成测试：伪造 adapter 回放录制帧 + MockTransport 预热 → 全链路 → 告警落盘。

零真实网络；验证：
- 启动预热灌指标窗口；
- 录制帧回放产出行情并更新看板状态；
- 价格异动规则触发 → alerts.jsonl 落盘 + DashboardState 快照含行情与告警；
- ``p`` 命令暂停后不再产出告警，恢复后继续产出；
- ``q`` 命令优雅收敛。
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from pathlib import Path
from typing import Optional

import httpx
import pytest

from quant.alerts.manager import AlertManager
from quant.app import QuantApp
from quant.cli.keyboard import KeyboardController
from quant.config import load_settings
from quant.feeds.base import FeedAdapter, FeedEvent, FeedStatus
from quant.feeds.binance_rest import BinanceRestClient
from quant.feeds.binance_ws import BinanceWSAdapter
from quant.models import Candle
from quant.storage import Storage
from tests.conftest import write_config, load_ws_raw_frames

REST_BASE = "https://data-api.binance.vision"
KLINES_PATH = "/api/v3/klines"

#: 预热基准开盘时间（2025-10-24T06:20:00Z）
WARMUP_BASE_OPEN_MS = 1_761_287_200_000
MINUTE_MS = 60_000


class FakeFeedAdapter(FeedAdapter):
    """伪造行情适配器：先回放事件，之后等待 emit 注入或关闭。"""

    name = "fake_feed"

    def __init__(self, events: list[FeedEvent]) -> None:
        super().__init__()
        self._pending: deque[FeedEvent] = deque(events)
        self._injected: asyncio.Queue[FeedEvent] = asyncio.Queue()
        self._closed = False
        self._status = FeedStatus(connected=True, reconnect_count=1, detail="伪造连接")

    async def connect(self) -> None:
        self._status.connected = True

    async def events(self):
        while not self._closed:
            if self._pending:
                yield self._pending.popleft()
                continue
            try:
                event = await asyncio.wait_for(self._injected.get(), timeout=0.05)
            except asyncio.TimeoutError:
                continue
            yield event

    async def close(self) -> None:
        self._closed = True
        self._status.connected = False

    def status(self) -> FeedStatus:
        return self._status

    def emit(self, event: FeedEvent) -> None:
        """注入一个新事件（测试驱动用）。"""
        self._injected.put_nowait(event)


class StubRenderer:
    """桩渲染器：只记录渲染次数，不占用终端。"""

    def __init__(self) -> None:
        self.renders = 0

    async def render_loop(self, state, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            self.renders += 1
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.05)
            except asyncio.TimeoutError:
                pass


def kline_row(open_ms: int, close: float) -> list:
    """构造币安 K 线接口一行（数值为字符串）。"""
    return [
        open_ms,
        f"{close - 10:.2f}",
        f"{close + 10:.2f}",
        f"{close - 20:.2f}",
        f"{close:.2f}",
        "10.0",
        open_ms + MINUTE_MS - 1,
        f"{close * 10:.2f}",
        5,
        "0",
        "0",
        "0",
    ]


def build_settings(tmp_path: Path):
    """集成测试配置：仅启用价格异动规则、冷却 0、两币种。"""
    config_path = write_config(
        tmp_path,
        '[network]\nproxy = "http://127.0.0.1:63571"\n\n'
        '[market]\nsymbols = ["BTCUSDT", "ETHUSDT"]\n\n'
        "[indicators]\ncandle_window = 300\nrsi_length = 14\nma_short = 5\nma_long = 25\nvol_ma_length = 20\n\n"
        "[rules.price_move]\nenabled = true\nwindow_min = 5\nthreshold_pct = 5.0\n\n"
        "[rules.volume_spike]\nenabled = false\nmultiplier = 3.0\n\n"
        "[rules.rsi]\nenabled = false\noverbought = 70.0\noversold = 30.0\n\n"
        "[rules.ma_cross]\nenabled = false\n\n"
        "[alerts]\ncooldown_sec = 0.0\nlog_path = \"data/alerts.jsonl\"\n\n"
        '[storage]\ndb_path = "data/quant.db"\n\n'
        "[cli]\nrefresh_interval_sec = 0.5\n",
    )
    return load_settings(config_path)


def read_jsonl(path: str | Path) -> list[dict]:
    """读取 JSONL 全部行。"""
    target = Path(path)
    if not target.is_file():
        return []
    return [
        json.loads(line)
        for line in target.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def jump_candle(symbol: str, index: int, close: float) -> Candle:
    """构造一根已收盘的跳变 K 线（用于触发/暂停验证）。"""
    open_ms = WARMUP_BASE_OPEN_MS + index * MINUTE_MS
    return Candle(
        symbol=symbol,
        open_time_ms=open_ms,
        close_time_ms=open_ms + MINUTE_MS - 1,
        open=close - 50.0,
        high=close + 50.0,
        low=close - 100.0,
        close=close,
        volume=10.0,
        quote_volume=close * 10.0,
        trades=5,
        is_closed=True,
    )


def test_full_pipeline_with_recordings(tmp_path: Path) -> None:
    """全链路集成：预热 → 回放 → 告警落盘 → 暂停/恢复 → 退出。"""
    settings = build_settings(tmp_path)

    # REST 预热：MockTransport 返回两币种各 300 根 1min K 线
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == KLINES_PATH
        symbol = request.url.params["symbol"]
        base_close = 68000.0 if symbol == "BTCUSDT" else 2456.0
        rows = [
            kline_row(WARMUP_BASE_OPEN_MS + i * MINUTE_MS, base_close + (i % 5) * 0.5)
            for i in range(300)
        ]
        return httpx.Response(200, json=rows)

    rest = BinanceRestClient(
        base_url=REST_BASE,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url=REST_BASE
        ),
    )

    # 伪造适配器：回放录制帧（复用真实 WS 解析器转换）
    parser = BinanceWSAdapter(["BTCUSDT", "ETHUSDT"])
    events = [
        item for raw in load_ws_raw_frames() for item in parser._parse_raw(raw)
    ]
    adapter = FakeFeedAdapter(events)

    storage = Storage(settings.storage.db_path)
    storage.init_schema()
    alert_manager = AlertManager(
        cooldown_sec=settings.alerts.cooldown_sec,
        log_path=settings.alerts.log_path,
        store=storage,
    )
    renderer = StubRenderer()
    keyboard = KeyboardController(reader=lambda: None, poll_interval_sec=0.01)

    app = QuantApp(
        settings,
        adapter=adapter,
        rest_client=rest,
        storage=storage,
        renderer=renderer,
        keyboard=keyboard,
        alert_manager=alert_manager,
    )

    async def _run() -> None:
        task = asyncio.create_task(app.start())
        # 1) 预热 + 回放：看板快照含行情（回放帧涨跌幅不足阈值，暂无告警）
        await asyncio.sleep(0.6)
        snapshot = app.state.snapshot()
        assert snapshot.symbols["BTCUSDT"].last_price > 0
        assert snapshot.symbols["ETHUSDT"].last_price > 0
        assert read_jsonl(settings.alerts.log_path) == []
        assert snapshot.feed_status.connected is True
        assert snapshot.rules_enabled == {
            "price_move": True,
            "volume_spike": False,
            "rsi": False,
            "ma_cross": False,
        }
        assert renderer.renders > 0

        # 2) 注入跳变 K 线 → 触发价格异动 → 告警落盘 + 快照含告警
        adapter.emit(jump_candle("BTCUSDT", 400, 68000.0 * 1.06))
        await asyncio.sleep(0.3)
        alerts = read_jsonl(settings.alerts.log_path)
        assert len(alerts) == 1
        assert alerts[0]["rule"] == "price_move"
        assert alerts[0]["symbol"] == "BTCUSDT"
        snapshot = app.state.snapshot()
        assert len(snapshot.alerts) >= 1
        assert snapshot.symbols["BTCUSDT"].last_signal == "price_move"

        # 3) p 暂停后注入跳变 K 线 → 不再产出
        app.command_queue.put_nowait("pause")
        await asyncio.sleep(0.15)
        assert app.state.paused is True
        adapter.emit(jump_candle("BTCUSDT", 401, 68000.0 * 1.12))
        await asyncio.sleep(0.3)
        assert len(read_jsonl(settings.alerts.log_path)) == 1

        # 4) 恢复后再注入 → 继续产出
        app.command_queue.put_nowait("pause")
        await asyncio.sleep(0.15)
        assert app.state.paused is False
        adapter.emit(jump_candle("BTCUSDT", 402, 68000.0 * 1.2))
        await asyncio.sleep(0.3)
        assert len(read_jsonl(settings.alerts.log_path)) == 2

        # 5) q 优雅退出
        app.command_queue.put_nowait("quit")
        await asyncio.wait_for(task, timeout=5.0)

    asyncio.run(_run())

    # 6) 退出后：SQLite 中可查询到告警（落库验证）
    reopened = Storage(settings.storage.db_path)
    stored = reopened.query_alerts(symbol="BTCUSDT")
    reopened.close()
    assert len(stored) >= 1
    assert stored[0].rule == "price_move"


def test_pipeline_warmup_failure_does_not_block_startup(tmp_path: Path) -> None:
    """预热失败（REST 5xx）不阻断启动：看板仍可运行并能退出。"""
    settings = build_settings(tmp_path)

    async def _instant_sleep(seconds: float) -> None:
        return None

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"msg": "boom"})

    rest = BinanceRestClient(
        base_url=REST_BASE,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url=REST_BASE
        ),
        sleep_func=_instant_sleep,
    )
    adapter = FakeFeedAdapter([])
    storage = Storage(settings.storage.db_path)
    storage.init_schema()

    app = QuantApp(
        settings,
        adapter=adapter,
        rest_client=rest,
        storage=storage,
        renderer=StubRenderer(),
        keyboard=KeyboardController(reader=lambda: None, poll_interval_sec=0.01),
    )

    async def _run() -> None:
        task = asyncio.create_task(app.start())
        await asyncio.sleep(0.3)  # 预热重试 4 次（瞬时睡眠）后继续启动
        assert app.indicators.latest("BTCUSDT") is None  # 预热失败无快照
        app.command_queue.put_nowait("quit")
        await asyncio.wait_for(task, timeout=5.0)

    asyncio.run(_run())
    storage.close()


def test_pipeline_command_toggles_rule(tmp_path: Path) -> None:
    """1-4 命令经队列协程切换规则开关（不阻塞数据流）。"""
    settings = build_settings(tmp_path)

    def handler(request: httpx.Request) -> httpx.Response:
        rows = [
            kline_row(WARMUP_BASE_OPEN_MS + i * MINUTE_MS, 68000.0 + (i % 5) * 0.5)
            for i in range(300)
        ]
        return httpx.Response(200, json=rows)

    rest = BinanceRestClient(
        base_url=REST_BASE,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url=REST_BASE
        ),
    )
    adapter = FakeFeedAdapter([])
    storage = Storage(settings.storage.db_path)
    storage.init_schema()

    app = QuantApp(
        settings,
        adapter=adapter,
        rest_client=rest,
        storage=storage,
        renderer=StubRenderer(),
        keyboard=KeyboardController(reader=lambda: None, poll_interval_sec=0.01),
    )

    async def _run() -> None:
        task = asyncio.create_task(app.start())
        await asyncio.sleep(0.3)
        # 命令 1：关闭 price_move；命令 9：越界忽略；命令 r：开面板
        app.command_queue.put_nowait("toggle_rule:1")
        app.command_queue.put_nowait("toggle_rule:9")
        app.command_queue.put_nowait("rule_panel")
        await asyncio.sleep(0.2)
        snapshot = app.state.snapshot()
        assert snapshot.rules_enabled["price_move"] is False
        assert snapshot.rule_panel_open is True
        assert app.rules.rule_names()[0] == "price_move"
        # 禁用后注入跳变 K 线不再告警
        adapter.emit(jump_candle("BTCUSDT", 500, 68000.0 * 1.2))
        await asyncio.sleep(0.3)
        assert read_jsonl(settings.alerts.log_path) == []
        app.command_queue.put_nowait("quit")
        await asyncio.wait_for(task, timeout=5.0)

    asyncio.run(_run())
    storage.close()


# ---------------------------------------------------------------------------
# 任务监督与停止路径
# ---------------------------------------------------------------------------


def build_app(settings, tmp_path: Path, **overrides) -> QuantApp:
    """用桩适配器 + MockTransport 构造 QuantApp（零网络）。"""

    def handler(request: httpx.Request) -> httpx.Response:
        rows = [
            kline_row(WARMUP_BASE_OPEN_MS + i * MINUTE_MS, 68000.0 + (i % 5) * 0.5)
            for i in range(300)
        ]
        return httpx.Response(200, json=rows)

    rest = BinanceRestClient(
        base_url=REST_BASE,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url=REST_BASE
        ),
    )
    defaults = dict(
        adapter=FakeFeedAdapter([]),
        rest_client=rest,
        storage=Storage(settings.storage.db_path),
        renderer=StubRenderer(),
        keyboard=KeyboardController(reader=lambda: None, poll_interval_sec=0.01),
    )
    defaults.update(overrides)
    return QuantApp(settings, **defaults)


def test_supervise_restarts_after_exception(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """任务异常退出后按退避重启（退避间隔打桩加速）。"""
    monkeypatch.setattr("quant.app.RESTART_DELAY_SEC", 0.05)
    settings = build_settings(tmp_path)
    app = build_app(settings, tmp_path)
    calls = {"n": 0}

    async def flaky() -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("模拟任务崩溃")

    async def _run() -> int:
        await app._supervise("flaky_task", flaky)
        return calls["n"]

    assert asyncio.run(_run()) == 2  # 崩溃一次后重启并正常结束


def test_supervise_stops_on_stop_event(tmp_path: Path) -> None:
    """stop_event 置位后监督循环退出，不再重启任务。"""
    settings = build_settings(tmp_path)
    app = build_app(settings, tmp_path)
    app.stop_event.set()
    calls = {"n": 0}

    async def factory() -> None:
        calls["n"] += 1

    async def _run() -> int:
        await app._supervise("stopped_task", factory)
        return calls["n"]

    assert asyncio.run(_run()) == 0


def test_stop_degrades_on_component_errors(tmp_path: Path) -> None:
    """组件关闭异常被吞掉，stop() 仍能收敛。"""

    class BadAdapter(FakeFeedAdapter):
        async def close(self) -> None:
            raise RuntimeError("模拟关闭故障")

    class BadRest(BinanceRestClient):
        async def aclose(self) -> None:
            raise RuntimeError("模拟关闭故障")

    settings = build_settings(tmp_path)
    storage = Storage(settings.storage.db_path)
    storage.init_schema()
    app = build_app(
        settings,
        tmp_path,
        adapter=BadAdapter([]),
        rest_client=BadRest(
            base_url=REST_BASE,
            client=httpx.AsyncClient(
                transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[])),
                base_url=REST_BASE,
            ),
        ),
        storage=storage,
    )

    async def _run() -> None:
        task = asyncio.create_task(app.start())
        await asyncio.sleep(0.2)
        app.command_queue.put_nowait("quit")
        await asyncio.wait_for(task, timeout=5.0)
        # 停止后再次调用 stop() 应为无操作（幂等）
        await app.stop()

    asyncio.run(_run())


def test_stop_closes_storage(tmp_path: Path) -> None:
    """stop() 关闭存储连接（连接不可再使用）。"""
    settings = build_settings(tmp_path)
    app = build_app(settings, tmp_path)

    async def _run() -> None:
        task = asyncio.create_task(app.start())
        await asyncio.sleep(0.2)
        app.command_queue.put_nowait("quit")
        await asyncio.wait_for(task, timeout=5.0)
        with pytest.raises(Exception):
            app.storage.conn.execute("SELECT 1")

    asyncio.run(_run())
