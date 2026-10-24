"""美港股轮询适配器测试：解析 / 延迟标注 / 退避（全程禁网）。"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from quant.feeds.tencent_poll import TencentPollAdapter, _parse_gt_time
from quant.models import Tick

_CN = timezone(timedelta(hours=8))


def _gt_line(code: str, price: float, prev: float, open_: float, vol: float,
             time_str: str, high: float, low: float) -> str:
    fields = ["0"] * 35
    fields[0] = "200"
    fields[1] = "name"
    fields[2] = code
    fields[3] = str(price)
    fields[4] = str(prev)
    fields[5] = str(open_)
    fields[6] = str(vol)
    fields[30] = time_str
    fields[33] = str(high)
    fields[34] = str(low)
    return f'v_{code}="' + "~".join(fields) + '";'


QUOTE_TEXT = "\n".join(
    [
        _gt_line("usAAPL", 185.50, 184.00, 184.50, 123456, "20240102150000", 186.0, 183.0),
        _gt_line("hk00700", 320.20, 318.00, 319.00, 65432, "20240102160000", 322.0, 317.0),
    ]
)


def _make_adapter(**kwargs) -> TencentPollAdapter:
    defaults = dict(
        watchlist_us=["AAPL.US"],
        watchlist_hk=["0700.HK"],
        poll_interval_sec=2.5,
        http_get=lambda url: _async_return(""),
        sleep_func=lambda sec: _async_return(None),
    )
    defaults.update(kwargs)
    return TencentPollAdapter(**defaults)


async def _async_return(value):
    return value


def test_latency_tag():
    assert TencentPollAdapter.latency_tag == "~15min"


def test_gt_code_mapping():
    assert TencentPollAdapter._gt_code("AAPL.US") == "usAAPL"
    assert TencentPollAdapter._gt_code("0700.HK") == "hk00700"
    assert TencentPollAdapter._gt_code("9988.HK") == "hk09988"


def test_parse_quotes():
    adapter = _make_adapter()
    quotes = adapter._parse(QUOTE_TEXT)
    assert [q.symbol for q in quotes] == ["AAPL.US", "0700.HK"]
    assert quotes[0].price == 185.50
    assert quotes[0].high == 186.0 and quotes[0].low == 183.0
    expected = int(datetime(2024, 1, 2, 15, 0, 0, tzinfo=_CN).timestamp() * 1000)
    assert quotes[0].ts_ms == expected


def test_parse_gt_time():
    assert _parse_gt_time("20240102150000") == int(
        datetime(2024, 1, 2, 15, 0, 0, tzinfo=_CN).timestamp() * 1000
    )
    assert _parse_gt_time("") is None


def test_events_yields_ticks():
    async def http_get(url: str) -> str:
        return QUOTE_TEXT

    async def fake_sleep(sec: float) -> None:
        if sec == 2.5:
            adapter._closed = True

    adapter = _make_adapter(http_get=http_get, sleep_func=fake_sleep)

    async def _run():
        return [e async for e in adapter.events()]

    events = asyncio.run(_run())
    ticks = [e for e in events if isinstance(e, Tick)]
    assert {t.symbol for t in ticks} == {"AAPL.US", "0700.HK"}
    assert all(t.source == "tencent" for t in ticks)


def test_backoff_grows_on_failure():
    slept: list[float] = []

    async def http_get(url: str) -> str:
        raise RuntimeError("网络不可用")

    async def fake_sleep(sec: float) -> None:
        slept.append(sec)
        if len(slept) >= 3:
            adapter._closed = True

    adapter = _make_adapter(http_get=http_get, sleep_func=fake_sleep)

    async def _run():
        return [e async for e in adapter.events()]

    assert asyncio.run(_run()) == []
    assert slept[:3] == [1.0, 2.0, 4.0]
