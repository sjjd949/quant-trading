"""币安 WS 适配器测试：录制回放/解析/退避序列/看门狗/gap 修复（零真实网络）。

伪造 WS 通过 ``BinanceWSAdapter(connector=...)`` 注入；退避睡眠经
``sleep_func`` 注入记录器，不产生真实等待。
"""

from __future__ import annotations

import asyncio
import json
from collections import deque

import pytest

from quant.feeds import BinanceWSAdapter
from quant.feeds.binance_ws import GAP_REPAIR_LIMIT
from quant.models import Candle, Tick
from quant.utils.timeutils import MS_PER_MINUTE
from tests.conftest import load_ws_frames, load_ws_raw_frames

WS_URL = "wss://data-stream.binance.vision/stream"


class StopLoop(Exception):
    """测试用：达到预期睡眠次数后中断 events() 循环。"""


class FakeWS:
    """伪造 WS 连接：按序回放帧，耗尽后按模式阻塞或抛断连异常。"""

    def __init__(self, frames: list[str], on_exhausted: str = "block") -> None:
        self._frames = deque(frames)
        self._on_exhausted = on_exhausted
        self.closed = False
        self._closed_event = asyncio.Event()
        self.recv_calls = 0

    async def recv(self) -> str:
        self.recv_calls += 1
        if self._frames:
            return self._frames.popleft()
        if self._on_exhausted == "raise":
            raise ConnectionError("模拟连接中断")
        await self._closed_event.wait()
        raise ConnectionError("连接已关闭")

    async def close(self) -> None:
        self.closed = True
        self._closed_event.set()


class FakeConnector:
    """伪造连接工厂：按调用次序返回/抛出，记录调用次数。"""

    def __init__(self, behaviors: list) -> None:
        self._behaviors = list(behaviors)
        self.calls = 0

    async def __call__(self, url: str):
        self.calls += 1
        if not self._behaviors:
            raise AssertionError("连接工厂被超额调用")
        behavior = self._behaviors.pop(0)
        if isinstance(behavior, Exception):
            raise behavior
        return behavior


def make_sleep_recorder(limit: int) -> tuple[list[float], callable]:
    """构造退避记录器：达到 limit 次后抛 StopLoop 中断循环。"""
    recorded: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)
        if len(recorded) >= limit:
            raise StopLoop()

    return recorded, fake_sleep


async def drain(agen, max_items: int, timeout: float = 2.0) -> list:
    """从异步生成器收集最多 max_items 个事件（超时即返回已收集部分）。

    返回前显式 ``aclose`` 生成器，确保 ``events()`` 的 finally 收敛
    （状态回落、底层连接关闭）在断言前完成。
    """

    async def _collect() -> list:
        items = []
        async for item in agen:
            items.append(item)
            if len(items) >= max_items:
                break
        return items

    try:
        return await asyncio.wait_for(_collect(), timeout=timeout)
    finally:
        await agen.aclose()


#: 录制帧可产出的事件总数（12 trade + 5 kline；非法帧不产出）
RECORDING_EVENT_COUNT = 17


# ---------------------------------------------------------------------------
# 订阅 URL
# ---------------------------------------------------------------------------


def test_stream_url_multiplexed() -> None:
    """组合流 URL：每币种 @trade + @kline_1m，小写，斜杠连接。"""
    adapter = BinanceWSAdapter(["BTCUSDT", "ETHUSDT"], ws_url=WS_URL)
    url = adapter.stream_url()
    assert url.startswith(WS_URL + "?streams=")
    streams = url.split("?streams=")[1].split("/")
    assert streams == [
        "btcusdt@trade",
        "btcusdt@kline_1m",
        "ethusdt@trade",
        "ethusdt@kline_1m",
    ]


# ---------------------------------------------------------------------------
# 录制回放解析
# ---------------------------------------------------------------------------


def test_recordings_fixture_shape() -> None:
    """录制帧资产：≥10 条 trade + ≥3 条 kline（含 1 根完整收盘）+ 1 条非法帧。"""
    frames = load_ws_frames()
    trades = [f for f in frames if isinstance(f, dict) and f.get("data", {}).get("e") == "trade"]
    klines = [f for f in frames if isinstance(f, dict) and f.get("data", {}).get("e") == "kline"]
    closed = [f for f in klines if f["data"]["k"].get("x") is True]
    invalid = [f for f in frames if isinstance(f, str)]
    assert len(trades) >= 10
    assert len(klines) >= 3
    assert len(closed) >= 1
    assert len(invalid) == 1


def test_parse_recorded_frames_fields() -> None:
    """录制帧解析：Tick/Candle 字段与币安文档一致。"""
    adapter = BinanceWSAdapter(["BTCUSDT", "ETHUSDT"], ws_url=WS_URL)
    items = []
    for raw in load_ws_raw_frames():
        items.extend(adapter._parse_raw(raw))

    ticks = [i for i in items if isinstance(i, Tick)]
    candles = [i for i in items if isinstance(i, Candle)]
    assert len(ticks) == 12
    assert len(candles) == 5

    first = ticks[0]
    assert first.symbol == "BTCUSDT"
    assert first.price == pytest.approx(68000.10)
    assert first.quantity == pytest.approx(0.012)
    assert first.trade_time_ms == 1761287524999
    assert first.is_buyer_maker is True
    assert first.source == "binance_ws"

    eth_tick = [t for t in ticks if t.symbol == "ETHUSDT"][0]
    assert eth_tick.price == pytest.approx(2455.80)
    assert eth_tick.is_buyer_maker is False

    closed = [c for c in candles if c.is_closed]
    assert len(closed) == 2  # BTC 一根 + ETH 一根
    btc_closed = [c for c in closed if c.symbol == "BTCUSDT"][0]
    assert btc_closed.open_time_ms == 1761287520000
    assert btc_closed.close_time_ms == 1761287579999
    assert btc_closed.open == pytest.approx(67980.00)
    assert btc_closed.high == pytest.approx(68020.00)
    assert btc_closed.low == pytest.approx(67975.00)
    assert btc_closed.close == pytest.approx(68015.60)
    assert btc_closed.volume == pytest.approx(25.8)
    assert btc_closed.quote_volume == pytest.approx(1756000.00)
    assert btc_closed.trades == 22

    unclosed = [c for c in candles if not c.is_closed]
    assert len(unclosed) == 3


def test_parse_invalid_frames_skipped() -> None:
    """非法帧与缺字段帧被跳过且不抛异常。"""
    adapter = BinanceWSAdapter(["BTCUSDT"], ws_url=WS_URL)
    assert adapter._parse_raw("not-json") == []
    assert adapter._parse_raw('{"stream":"btcusdt@trade"}') == []
    assert adapter._parse_raw('{"stream":"btcusdt@trade","data":{}}') == []
    assert adapter._parse_raw('{"stream":"x","data":{"e":"unknown"}}') == []
    assert adapter._parse_raw("[1,2,3]") == []
    # 字段类型非法的帧同样被吞掉
    assert adapter._parse_raw('{"data":{"e":"trade","s":"BTCUSDT","p":"abc","q":"1","T":1}}') == []


# ---------------------------------------------------------------------------
# events() 回放与状态
# ---------------------------------------------------------------------------


def test_events_replays_recordings() -> None:
    """events() 回放录制帧：产出全部 Tick/Candle，状态为已连接。"""

    async def _run() -> tuple:
        frames = load_ws_raw_frames()
        ws = FakeWS(frames, on_exhausted="raise")
        adapter = BinanceWSAdapter(
            ["BTCUSDT", "ETHUSDT"], ws_url=WS_URL, connector=FakeConnector([ws])
        )
        items = await drain(adapter.events(), max_items=RECORDING_EVENT_COUNT)
        status = adapter.status()
        return items, status

    items, status = asyncio.run(_run())
    assert len(items) == RECORDING_EVENT_COUNT
    assert any(isinstance(i, Tick) for i in items)
    assert any(isinstance(i, Candle) and i.is_closed for i in items)
    assert status.connected is False  # 生成器关闭后状态回落
    assert status.reconnect_count == 1
    assert status.last_msg_monotonic > 0


def test_events_never_raises_on_disconnect() -> None:
    """连接中断后 events() 收敛为重连而不向上抛异常。"""

    async def _run() -> tuple:
        first = FakeWS(load_ws_raw_frames(), on_exhausted="raise")
        second = FakeWS(load_ws_raw_frames(), on_exhausted="raise")
        recorded, fake_sleep = make_sleep_recorder(limit=1)
        adapter = BinanceWSAdapter(
            ["BTCUSDT", "ETHUSDT"],
            ws_url=WS_URL,
            connector=FakeConnector([first, second]),
            sleep_func=fake_sleep,
        )
        items = []
        try:
            async for item in adapter.events():
                items.append(item)
        except StopLoop:
            pass
        return items, recorded

    items, recorded = asyncio.run(_run())
    assert recorded == [1.0]  # 断线后首次退避 1s
    assert len(items) == RECORDING_EVENT_COUNT  # 第一次连接的帧全部产出


def test_close_stops_generator() -> None:
    """close() 后 events() 结束且底层连接已关闭。"""

    async def _run() -> None:
        ws = FakeWS([], on_exhausted="block")
        adapter = BinanceWSAdapter(
            ["BTCUSDT"], ws_url=WS_URL, connector=FakeConnector([ws])
        )

        async def _consume() -> None:
            async for _ in adapter.events():
                pass

        task = asyncio.create_task(_consume())
        await asyncio.sleep(0.05)  # 进入 recv 阻塞
        await adapter.close()
        await asyncio.wait_for(task, timeout=2.0)

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# 退避序列
# ---------------------------------------------------------------------------


def test_backoff_sequence_capped_at_30() -> None:
    """连接持续失败：退避序列 1→2→4→8→16→30→30（封顶）。"""

    async def _run() -> list[float]:
        recorded, fake_sleep = make_sleep_recorder(limit=7)
        connector = FakeConnector([ConnectionError("模拟连接失败")] * 10)
        adapter = BinanceWSAdapter(
            ["BTCUSDT"], ws_url=WS_URL, connector=connector, sleep_func=fake_sleep
        )
        try:
            async for _ in adapter.events():
                pass
        except StopLoop:
            pass
        return recorded

    recorded = asyncio.run(_run())
    assert recorded == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


def test_backoff_resets_after_success() -> None:
    """连接成功后退避重置；再次断线又从 1s 开始。"""

    async def _run() -> tuple:
        first = FakeWS([], on_exhausted="block")  # 连接成功后空闲超时
        second = FakeWS([], on_exhausted="block")
        recorded, fake_sleep = make_sleep_recorder(limit=2)
        connector = FakeConnector([ConnectionError("第一次失败"), first, second])
        adapter = BinanceWSAdapter(
            ["BTCUSDT"],
            ws_url=WS_URL,
            connector=connector,
            idle_timeout_sec=0.05,
            sleep_func=fake_sleep,
        )
        try:
            async for _ in adapter.events():
                pass
        except StopLoop:
            pass
        return recorded, adapter.status()

    recorded, status = asyncio.run(_run())
    assert recorded == [1.0, 1.0]  # 失败退避 1s；成功后重置，空闲超时又退避 1s
    assert status.reconnect_count == 1


def test_idle_timeout_triggers_reconnect() -> None:
    """空闲看门狗：超过 idle_timeout 未收消息则关闭并重连。"""

    async def _run() -> tuple:
        first = FakeWS([], on_exhausted="block")
        second = FakeWS([], on_exhausted="block")
        recorded, fake_sleep = make_sleep_recorder(limit=2)
        connector = FakeConnector([first, second])
        adapter = BinanceWSAdapter(
            ["BTCUSDT"],
            ws_url=WS_URL,
            connector=connector,
            idle_timeout_sec=0.05,
            sleep_func=fake_sleep,
        )
        try:
            async for _ in adapter.events():
                pass
        except StopLoop:
            pass
        return recorded, connector.calls, first.closed, adapter.status()

    recorded, calls, first_closed, status = asyncio.run(_run())
    assert recorded == [1.0, 1.0]
    assert calls == 2  # 已触发一次重连
    assert first_closed is True  # 旧连接被关闭
    assert "空闲超时" in status.detail


# ---------------------------------------------------------------------------
# gap 修复
# ---------------------------------------------------------------------------


class FakeRest:
    """伪造 REST 客户端：记录调用参数并返回预置 K 线。"""

    def __init__(self, results: dict[str, list[Candle]]) -> None:
        self._results = results
        self.calls: list[dict] = []

    async def klines(self, symbol, interval="1m", limit=300, start_ms=None, end_ms=None):
        self.calls.append(
            {
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
                "start_ms": start_ms,
                "end_ms": end_ms,
            }
        )
        return list(self._results.get(symbol, []))


def _rest_candle(symbol: str, open_time_ms: int, close: float = 100.0) -> Candle:
    return Candle(
        symbol=symbol,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 59_999,
        open=close - 1.0,
        high=close + 1.0,
        low=close - 2.0,
        close=close,
        volume=10.0,
        quote_volume=close * 10.0,
        trades=5,
        is_closed=True,
    )


def test_gap_repair_yields_before_live_and_uses_start_time() -> None:
    """重连后先补洞再恢复实时流；startTime=最后收盘开盘时间+1m。"""

    async def _run() -> tuple:
        last_open = 1761287520000  # 06:32:00
        live_open = last_open + 5 * MS_PER_MINUTE  # 06:37:00
        rest = FakeRest(
            {
                "BTCUSDT": [
                    _rest_candle("BTCUSDT", last_open + 1 * MS_PER_MINUTE),
                    _rest_candle("BTCUSDT", last_open + 2 * MS_PER_MINUTE),
                ]
            }
        )
        first = FakeWS(load_ws_raw_frames(), on_exhausted="raise")  # 含一根收盘 K 线
        second = FakeWS(
            [
                json.dumps(
                    {
                        "stream": "btcusdt@trade",
                        "data": {
                            "e": "trade",
                            "s": "BTCUSDT",
                            "p": "68100.00",
                            "q": "0.01",
                            "T": live_open + 1_000,
                            "m": False,
                        },
                    }
                )
            ],
            on_exhausted="raise",
        )
        recorded, fake_sleep = make_sleep_recorder(limit=2)
        adapter = BinanceWSAdapter(
            ["BTCUSDT"],
            ws_url=WS_URL,
            rest_client=rest,
            connector=FakeConnector([first, second]),
            sleep_func=fake_sleep,
        )
        items = []
        try:
            async for item in adapter.events():
                items.append(item)
        except StopLoop:
            pass
        return items, rest.calls

    items, calls = asyncio.run(_run())
    # 第一段：录制帧全部产出；断线退避后重连 → 补洞 2 根 → 再产出实时 trade
    repaired = [i for i in items if isinstance(i, Candle) and i.volume == 10.0]
    assert len(repaired) == 2
    assert calls[0]["start_ms"] == 1761287520000 + MS_PER_MINUTE
    assert calls[0]["limit"] == GAP_REPAIR_LIMIT
    assert calls[0]["interval"] == "1m"
    # 顺序断言：补洞 K 线出现在实时 trade 之前
    live_tick_index = next(
        i for i, item in enumerate(items) if isinstance(item, Tick) and item.price == 68100.00
    )
    repaired_indexes = [i for i, item in enumerate(items) if item in repaired]
    assert max(repaired_indexes) < live_tick_index


def test_gap_repair_skips_already_seen() -> None:
    """已 yield 过的收盘 K 线不会被补洞重复产出。"""
    last_open = 1761287520000

    async def _run() -> list:
        rest = FakeRest(
            {
                "BTCUSDT": [
                    _rest_candle("BTCUSDT", last_open),  # 已 yield 过（同一开盘时间）
                    _rest_candle("BTCUSDT", last_open + MS_PER_MINUTE),  # 缺失
                ]
            }
        )
        adapter = BinanceWSAdapter(
            ["BTCUSDT"], ws_url=WS_URL, rest_client=rest, now_func=lambda: last_open + 10 * MS_PER_MINUTE
        )
        adapter._last_kline_open_ms["BTCUSDT"] = last_open
        seen = _rest_candle("BTCUSDT", last_open)
        adapter._mark_kline_seen(seen)
        return await adapter._gap_repair()

    repaired = asyncio.run(_run())
    assert len(repaired) == 1
    assert repaired[0].open_time_ms == last_open + MS_PER_MINUTE


def test_gap_repair_limit_warning(caplog) -> None:
    """缺口超过 5 根时仅补 5 根并记警告。"""

    async def _run() -> list:
        last_open = 1761287520000
        now = last_open + 20 * MS_PER_MINUTE  # 缺口约 19 根
        rest = FakeRest(
            {
                "BTCUSDT": [
                    _rest_candle("BTCUSDT", last_open + i * MS_PER_MINUTE)
                    for i in range(1, GAP_REPAIR_LIMIT + 1)
                ]
            }
        )
        adapter = BinanceWSAdapter(
            ["BTCUSDT"], ws_url=WS_URL, rest_client=rest, now_func=lambda: now
        )
        adapter._last_kline_open_ms["BTCUSDT"] = last_open
        with caplog.at_level("WARNING", logger="quant.feeds.binance_ws"):
            return await adapter._gap_repair()

    repaired = asyncio.run(_run())
    assert len(repaired) == GAP_REPAIR_LIMIT
    assert any("缺口超过" in record.getMessage() for record in caplog.records)


def test_gap_repair_skipped_without_rest_client() -> None:
    """无 REST 客户端时不补洞。"""

    async def _run() -> list:
        adapter = BinanceWSAdapter(["BTCUSDT"], ws_url=WS_URL, rest_client=None)
        adapter._last_kline_open_ms["BTCUSDT"] = 1761287520000
        return await adapter._gap_repair()

    assert asyncio.run(_run()) == []


def test_gap_repair_skipped_without_history() -> None:
    """无历史收盘 K 线（首次连接）时不补洞。"""

    async def _run() -> list:
        rest = FakeRest({})
        adapter = BinanceWSAdapter(["BTCUSDT"], ws_url=WS_URL, rest_client=rest)
        return await adapter._gap_repair()

    assert asyncio.run(_run()) == []


def test_gap_repair_rest_failure_isolated() -> None:
    """单币种补洞失败不影响其他币种。"""

    class FlakyRest(FakeRest):
        async def klines(self, symbol, **kwargs):
            self.calls.append({"symbol": symbol})
            if symbol == "BTCUSDT":
                raise RuntimeError("模拟 REST 故障")
            return [_rest_candle("ETHUSDT", 1761287580000)]

    async def _run() -> list:
        rest = FlakyRest({})
        adapter = BinanceWSAdapter(
            ["BTCUSDT", "ETHUSDT"],
            ws_url=WS_URL,
            rest_client=rest,
            now_func=lambda: 1761287640000,
        )
        adapter._last_kline_open_ms = {
            "BTCUSDT": 1761287520000,
            "ETHUSDT": 1761287520000,
        }
        return await adapter._gap_repair()

    repaired = asyncio.run(_run())
    assert len(repaired) == 1
    assert repaired[0].symbol == "ETHUSDT"


# ---------------------------------------------------------------------------
# 其他
# ---------------------------------------------------------------------------


def test_connect_resets_closed_flag() -> None:
    """connect() 复位停止标志。"""

    async def _run() -> None:
        adapter = BinanceWSAdapter(["BTCUSDT"], ws_url=WS_URL)
        await adapter.close()
        assert adapter._closed is True
        await adapter.connect()
        assert adapter._closed is False

    asyncio.run(_run())


def test_status_returns_copy() -> None:
    """status() 返回浅拷贝，外部修改不影响内部状态。"""

    async def _run() -> None:
        adapter = BinanceWSAdapter(["BTCUSDT"], ws_url=WS_URL)
        status = adapter.status()
        status.connected = True
        assert adapter.status().connected is False

    asyncio.run(_run())
