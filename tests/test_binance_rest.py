"""币安 REST 客户端测试：请求构造/重试/超时/5xx/429/4xx（MockTransport，零网络）。"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from quant.feeds import BinanceRestClient
from quant.feeds.binance_rest import DEFAULT_BASE_URL

BASE_URL = "https://data-api.binance.vision"
KLINES_PATH = "/api/v3/klines"

#: 固定 K 线开盘时间（2025-10-24T06:32:00Z）
OPEN_MS = 1_761_287_520_000


def kline_row(open_ms: int, close: float = 100.0) -> list:
    """构造币安 K 线接口的一行返回（数值为字符串）。"""
    return [
        open_ms,
        "99.0",
        "101.0",
        "98.0",
        f"{close}",
        "10.0",
        open_ms + 59_999,
        f"{close * 10}",
        5,
        "0",
        "0",
        "0",
    ]


def make_sleep_recorder() -> tuple[list[float], callable]:
    """构造睡眠记录器（异步测试注入用）。"""
    recorded: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)

    return recorded, fake_sleep


def make_client(handler, sleep_recorder: list[float]) -> BinanceRestClient:
    """用 MockTransport + 睡眠记录器构造客户端。"""

    async def _sleep(seconds: float) -> None:
        sleep_recorder.append(seconds)

    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport, base_url=BASE_URL)
    return BinanceRestClient(base_url=BASE_URL, client=http_client, sleep_func=_sleep)


# ---------------------------------------------------------------------------
# 请求构造与解析
# ---------------------------------------------------------------------------


def test_defaults() -> None:
    """默认值：.vision 域名、超时 10s、3 次重试。"""
    client = BinanceRestClient()
    assert client.base_url == DEFAULT_BASE_URL
    assert "data-api.binance.vision" in client.base_url
    assert client.timeout_sec == 10.0
    assert client.max_retries == 3


def test_klines_request_and_parse() -> None:
    """请求 path/params 正确；200 响应解析为 Candle 列表。"""
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json=[
                kline_row(OPEN_MS, 100.0),
                kline_row(OPEN_MS + 60_000, 101.5),
            ],
        )

    sleeps: list[float] = []
    client = make_client(handler, sleeps)

    async def _run() -> list:
        candles = await client.klines("btcusdt", interval="1m", limit=300)
        await client.aclose()
        return candles

    candles = asyncio.run(_run())

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "GET"
    assert request.url.path == KLINES_PATH
    params = dict(request.url.params)
    assert params["symbol"] == "BTCUSDT"  # 内部转大写
    assert params["interval"] == "1m"
    assert params["limit"] == "300"
    assert "startTime" not in params

    assert sleeps == []  # 首次成功无重试
    assert len(candles) == 2
    first = candles[0]
    assert first.symbol == "BTCUSDT"
    assert first.open_time_ms == OPEN_MS
    assert first.close_time_ms == OPEN_MS + 59_999
    assert first.open == pytest.approx(99.0)
    assert first.high == pytest.approx(101.0)
    assert first.low == pytest.approx(98.0)
    assert first.close == pytest.approx(100.0)
    assert first.volume == pytest.approx(10.0)
    assert first.quote_volume == pytest.approx(1000.0)
    assert first.trades == 5
    assert first.is_closed is True


def test_klines_time_params() -> None:
    """start_ms/end_ms 映射为 startTime/endTime 查询参数。"""
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[])

    client = make_client(handler, [])

    async def _run() -> None:
        await client.klines(
            "BTCUSDT", start_ms=OPEN_MS, end_ms=OPEN_MS + 300_000, limit=5
        )
        await client.aclose()

    asyncio.run(_run())
    params = dict(requests[0].url.params)
    assert params["startTime"] == str(OPEN_MS)
    assert params["endTime"] == str(OPEN_MS + 300_000)
    assert params["limit"] == "5"


def test_empty_klines_response() -> None:
    """空数组响应返回空列表。"""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    client = make_client(handler, [])

    async def _run() -> list:
        candles = await client.klines("BTCUSDT")
        await client.aclose()
        return candles

    assert asyncio.run(_run()) == []


def test_invalid_row_skipped(caplog) -> None:
    """非法行被跳过并记警告，不影响其他行。"""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[kline_row(OPEN_MS), ["坏行"]])

    client = make_client(handler, [])

    async def _run() -> list:
        candles = await client.klines("BTCUSDT")
        await client.aclose()
        return candles

    candles = asyncio.run(_run())
    assert len(candles) == 1
    assert any("格式非法" in r.message or "解析失败" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 重试策略
# ---------------------------------------------------------------------------


def test_timeout_retry_then_success() -> None:
    """超时后重试：退避 0.5s 后成功，共 2 次请求。"""
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectTimeout("模拟连接超时")
        return httpx.Response(200, json=[kline_row(OPEN_MS)])

    sleeps: list[float] = []
    client = make_client(handler, sleeps)

    async def _run() -> list:
        candles = await client.klines("BTCUSDT")
        await client.aclose()
        return candles

    candles = asyncio.run(_run())
    assert calls["n"] == 2
    assert sleeps == [0.5]
    assert len(candles) == 1


def test_5xx_retry_exhausted_raises() -> None:
    """持续 5xx：1+3 次尝试，退避 0.5/1/2s，最终抛出。"""
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500, json={"msg": "server error"})

    sleeps: list[float] = []
    client = make_client(handler, sleeps)

    async def _run() -> None:
        try:
            await client.klines("BTCUSDT")
        finally:
            await client.aclose()

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        asyncio.run(_run())
    assert exc_info.value.response.status_code == 500
    assert calls["n"] == 4  # 1 次原始 + 3 次重试
    assert sleeps == [0.5, 1.0, 2.0]


def test_4xx_raises_immediately_without_retry() -> None:
    """4xx 直接抛出，不重试。"""
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, json={"msg": "bad request"})

    sleeps: list[float] = []
    client = make_client(handler, sleeps)

    async def _run() -> None:
        try:
            await client.klines("BTCUSDT")
        finally:
            await client.aclose()

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        asyncio.run(_run())
    assert exc_info.value.response.status_code == 400
    assert calls["n"] == 1
    assert sleeps == []


def test_429_respects_retry_after() -> None:
    """429 尊重 Retry-After 头（优先于指数退避）。"""
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "1.5"})
        return httpx.Response(200, json=[kline_row(OPEN_MS)])

    sleeps: list[float] = []
    client = make_client(handler, sleeps)

    async def _run() -> list:
        candles = await client.klines("BTCUSDT")
        await client.aclose()
        return candles

    candles = asyncio.run(_run())
    assert calls["n"] == 2
    assert sleeps == [1.5]
    assert len(candles) == 1


def test_429_without_retry_after_uses_backoff() -> None:
    """429 无 Retry-After 头时用指数退避 0.5s。"""
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429)
        return httpx.Response(200, json=[])

    sleeps: list[float] = []
    client = make_client(handler, sleeps)

    async def _run() -> None:
        await client.klines("BTCUSDT")
        await client.aclose()

    asyncio.run(_run())
    assert sleeps == [0.5]


def test_429_exhausted_raises() -> None:
    """持续 429：重试用尽后抛出。"""
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(429, headers={"Retry-After": "2"})

    sleeps: list[float] = []
    client = make_client(handler, sleeps)

    async def _run() -> None:
        try:
            await client.klines("BTCUSDT")
        finally:
            await client.aclose()

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        asyncio.run(_run())
    assert exc_info.value.response.status_code == 429
    assert calls["n"] == 4
    assert sleeps == [2.0, 2.0, 2.0]


def test_transport_error_exhausted_raises() -> None:
    """持续网络错误：1+3 次尝试后退避序列 0.5/1/2s，原样抛出。"""
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("模拟连接被拒")

    sleeps: list[float] = []
    client = make_client(handler, sleeps)

    async def _run() -> None:
        try:
            await client.klines("BTCUSDT")
        finally:
            await client.aclose()

    with pytest.raises(httpx.ConnectError):
        asyncio.run(_run())
    assert calls["n"] == 4
    assert sleeps == [0.5, 1.0, 2.0]


def test_aclose_closes_client() -> None:
    """aclose 关闭底层客户端。"""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport, base_url=BASE_URL)
    client = BinanceRestClient(client=http_client)

    async def _run() -> None:
        assert client.client.is_closed is False
        await client.aclose()
        assert client.client.is_closed is True

    asyncio.run(_run())
