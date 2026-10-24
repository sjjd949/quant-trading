"""M2 集成测试：3 适配器并行（含异常监督）+ 模拟交易全链路 + 复盘回放。

零真实网络；验证：
- 每适配器一个监督协程，单一适配器异常不影响其他适配器；
- 跨市场行情（A股 Tick）与加密收盘 K 线并行进入统一分发链路；
- 一笔完整交易（信号→订单→成交→持仓→平仓→已实现盈亏）可串联；
- ``review`` 子命令可回放该交易生命周期。
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from quant.app import QuantApp
from quant.cli.keyboard import KeyboardController
from quant.cli.review import build_review_parser, run_review
from quant.config import Settings
from quant.feeds.base import FeedStatus
from quant.models import Candle, Tick
from quant.paper.engine import PaperEngine
from quant.storage.sqlite_store import Storage

BASE_MS = 1_700_000_000_000
MINUTE_MS = 60_000


class ScriptedAdapter:
    """脚本化适配器：先 yield 预设事件，之后空转至关闭。"""

    def __init__(self, name: str, events: list, latency_tag: str = "") -> None:
        self.name = name
        self.latency_tag = latency_tag
        self._events = list(events)
        self._closed = False
        self._status = FeedStatus(connected=True, reconnect_count=1, detail=name)

    async def connect(self) -> None:
        self._closed = False

    async def events(self):
        for event in self._events:
            yield event
        while not self._closed:
            await asyncio.sleep(0.02)

    async def close(self) -> None:
        self._closed = True

    def status(self) -> FeedStatus:
        return self._status


class FlakyAdapter(ScriptedAdapter):
    """首次 events() 抛异常，之后空转（验证适配器级监督重启）。"""

    def __init__(self) -> None:
        super().__init__("flaky", [])
        self.calls = 0

    async def events(self):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("模拟适配器崩溃")
        while not self._closed:
            await asyncio.sleep(0.02)
        if False:  # pragma: no cover - 保持 async generator 语义
            yield None


class StubRest:
    async def klines(self, symbol, interval="1m", limit=300, start_ms=None, end_ms=None):
        return []

    async def aclose(self) -> None:
        pass


class StubRenderer:
    async def render_loop(self, state, stop_event) -> None:
        while not stop_event.is_set():
            await asyncio.sleep(0.05)


def _settings(tmp_path: Path) -> Settings:
    settings = Settings()
    settings.network.proxy = "http://127.0.0.1:1"
    settings.market.symbols = ["BTCUSDT"]
    settings.alerts.cooldown_sec = 0.0
    settings.alerts.log_path = str(tmp_path / "alerts.jsonl")
    settings.storage.db_path = str(tmp_path / "q.db")
    settings.paper.enabled = True
    settings.paper.mode = "auto"
    settings.rules.price_move.enabled = True
    settings.rules.volume_spike.enabled = False
    settings.rules.rsi.enabled = False
    settings.rules.ma_cross.enabled = False
    return settings


def _candle(index: int, close: float, volume: float = 10.0, symbol: str = "BTCUSDT") -> Candle:
    open_ms = BASE_MS + index * MINUTE_MS
    return Candle(
        symbol=symbol,
        open_time_ms=open_ms,
        close_time_ms=open_ms + MINUTE_MS - 1,
        open=close - 1.0,
        high=close + 1.0,
        low=close - 2.0,
        close=close,
        volume=volume,
        quote_volume=close * volume,
        trades=1,
        is_closed=True,
    )


def _read_jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_multi_adapter_pipeline_with_paper_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
):
    monkeypatch.setattr("quant.app.RESTART_DELAY_SEC", 0.05)
    settings = _settings(tmp_path)

    # 适配器 A：加密收盘 K 线（先拉升开仓，再下跌平仓）
    adapter_a = ScriptedAdapter(
        "scripted_a",
        [
            Tick("BTCUSDT", 112.0, 1.0, BASE_MS + 40 * MINUTE_MS, False, "scripted_a"),
            _candle(40, 112.0, volume=50.0),  # +12% → price_move long
            _candle(41, 90.0, volume=10.0),   # -10% → price_move close
        ],
    )
    # 适配器 B：A股行情（验证跨市场并行）
    adapter_b = ScriptedAdapter(
        "scripted_b",
        [Tick("600519.SH", 1680.0, 100.0, BASE_MS, False, "scripted_b")],
    )
    # 适配器 C：启动即崩溃（验证适配器级监督）
    adapter_c = FlakyAdapter()

    storage = Storage(settings.storage.db_path)
    app = QuantApp(
        settings,
        adapters=[adapter_a, adapter_b, adapter_c],
        rest_client=StubRest(),
        storage=storage,
        renderer=StubRenderer(),
        keyboard=KeyboardController(reader=lambda: None),
    )
    # 预热指标窗口（40 根平稳 K 线）
    app.indicators.warmup("BTCUSDT", [_candle(i, 100.0) for i in range(40)])

    async def _run() -> None:
        task = asyncio.create_task(app.start())
        await asyncio.sleep(0.6)
        app.command_queue.put_nowait("quit")
        await asyncio.wait_for(task, timeout=5.0)

    asyncio.run(_run())

    # 告警：拉升 + 下跌两次 price_move
    rows = _read_jsonl(tmp_path / "alerts.jsonl")
    assert [row["rule"] for row in rows] == ["price_move", "price_move"]
    assert all(row["symbol"] == "BTCUSDT" for row in rows)

    # 跨市场并行：A股 Tick 已进入看板
    assert app.state.symbols["600519.SH"].last_price == 1680.0

    # 完整交易生命周期（app.stop() 已关闭注入的 storage，另开连接校验）
    verify = Storage(settings.storage.db_path)
    verify.init_schema()
    life = verify.query_trade_lifecycle("BTCUSDT")
    assert [o.side for o in life["orders"]] == ["buy", "sell"]
    assert all(o.status == "filled" for o in life["orders"])
    assert [p.event for p in life["positions"]] == ["open", "close"]
    assert app.paper.accounts["crypto"].realized_pnl != 0.0
    assert "BTCUSDT" not in app.paper.positions

    # 适配器级监督：C 崩溃后被重启（calls>=2），且未影响 A/B
    assert adapter_c.calls >= 2
    assert "scripted_a" in app.state.adapter_statuses
    assert "scripted_b" in app.state.adapter_statuses

    verify.close()

    # 复盘回放：可查询到买入+卖出两笔生命周期
    args = build_review_parser().parse_args(["--format", "json", "--symbol", "BTCUSDT"])
    assert run_review(settings, args) == 0
    reviewed = json.loads(capsys.readouterr().out)
    assert [row["side"] for row in reviewed] == ["buy", "sell"]
    assert reviewed[1]["realized_pnl"] != 0.0
