"""M2 应用编排单元测试：模拟盘接线 / 延迟前缀 / y-n-m 命令（禁网）。"""

from __future__ import annotations

import asyncio
import json

from quant.app import QuantApp
from quant.cli.keyboard import KeyboardController
from quant.config import Settings
from quant.feeds.base import FeedStatus
from quant.models import Candle
from quant.paper.engine import PaperEngine
from quant.storage.sqlite_store import Storage

BASE_MS = 1_700_000_000_000
MINUTE_MS = 60_000


class FakeAdapter:
    """最小伪造适配器（供 _dispatch 直接驱动，无事件流）。"""

    name = "fake"

    def __init__(self, latency_tag: str = "") -> None:
        self.latency_tag = latency_tag
        self._status = FeedStatus(connected=True, reconnect_count=1, detail="fake")

    async def connect(self) -> None:  # pragma: no cover - 直接 dispatch 不调用
        pass

    async def events(self):
        if False:  # pragma: no cover
            yield None

    async def close(self) -> None:  # pragma: no cover
        pass

    def status(self) -> FeedStatus:
        return self._status


class StubRest:
    async def klines(self, symbol, interval="1m", limit=300, start_ms=None, end_ms=None):
        return []

    async def aclose(self) -> None:
        pass


class StubRenderer:
    async def render_loop(self, state, stop_event) -> None:
        while not stop_event.is_set():
            await asyncio.sleep(0.05)


def _settings(tmp_path, mode: str = "auto") -> Settings:
    settings = Settings()
    settings.network.proxy = "http://127.0.0.1:1"
    settings.market.symbols = ["BTCUSDT"]
    settings.alerts.cooldown_sec = 0.0
    settings.alerts.log_path = str(tmp_path / "alerts.jsonl")
    settings.storage.db_path = str(tmp_path / "q.db")
    settings.paper.enabled = True
    settings.paper.mode = mode
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


def _build_app(tmp_path, mode: str = "auto", latency: str = ""):
    settings = _settings(tmp_path, mode)
    storage = Storage(settings.storage.db_path)
    storage.init_schema()
    adapter = FakeAdapter(latency_tag=latency)
    app = QuantApp(
        settings,
        adapter=adapter,
        rest_client=StubRest(),
        storage=storage,
        renderer=StubRenderer(),
        keyboard=KeyboardController(reader=lambda: None),
    )
    app.indicators.warmup("BTCUSDT", [_candle(i, 100.0) for i in range(40)])
    app.paper = PaperEngine(settings.paper, storage)
    app.state.paper_mode = app.paper.mode
    return app, storage, adapter, settings


def test_dispatch_auto_places_order(tmp_path):
    app, storage, adapter, _ = _build_app(tmp_path, mode="auto")
    app._dispatch(_candle(40, 112.0, volume=50.0), adapter)  # +12% → price_move long

    rows = (tmp_path / "alerts.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(rows) == 1
    assert json.loads(rows[0])["rule"] == "price_move"

    signals = storage.query_signals(symbol="BTCUSDT")
    assert len(signals) == 1
    assert signals[0].direction == "long"
    assert signals[0].status == "ordered"

    life = storage.query_trade_lifecycle("BTCUSDT")
    assert life["orders"][0].status == "filled"
    assert "BTCUSDT" in app.paper.positions


def test_dispatch_manual_pending_then_confirm(tmp_path):
    app, storage, adapter, _ = _build_app(tmp_path, mode="manual")
    app._dispatch(_candle(40, 112.0, volume=50.0), adapter)
    assert len(app.paper.pending) == 1
    assert storage.query_trade_lifecycle("BTCUSDT")["orders"] == []

    app._handle_command("confirm_signal")
    assert len(app.paper.pending) == 0
    assert storage.query_trade_lifecycle("BTCUSDT")["orders"][0].status == "filled"


def test_ignore_signal_command(tmp_path):
    app, storage, adapter, _ = _build_app(tmp_path, mode="manual")
    app._dispatch(_candle(40, 112.0, volume=50.0), adapter)
    app._handle_command("ignore_signal")
    assert storage.query_signals(symbol="BTCUSDT")[0].status == "ignored"


def test_toggle_mode_command(tmp_path):
    app, _, _, _ = _build_app(tmp_path, mode="manual")
    assert app.state.paper_mode == "manual"
    app._handle_command("toggle_mode")
    assert app.state.paper_mode == "auto"
    assert app.paper.mode == "auto"


def test_latency_prefix_on_alert(tmp_path):
    app, _, adapter, _ = _build_app(tmp_path, mode="auto", latency="~15min")
    app._dispatch(_candle(40, 112.0, volume=50.0), adapter)
    row = json.loads((tmp_path / "alerts.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["message"].startswith("[延迟约15min]")
