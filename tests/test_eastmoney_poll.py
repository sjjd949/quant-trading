"""A股轮询适配器测试：解析 / 聚合 / 主备切换 / 退避 / 预热（全程禁网）。"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

from quant.feeds.eastmoney_poll import EastmoneyPollAdapter
from quant.feeds.polling_base import CandleAggregator, Quote
from quant.models import Candle, Tick

_CN = timezone(timedelta(hours=8))

#: 东财主源示例响应（fltt=2，价格已是两位小数）
PRIMARY_TEXT = json.dumps(
    {
        "rc": 0,
        "data": {
            "total": 2,
            "f124": 1704092400,
            "diff": [
                {"f12": "600519", "f2": 1680.5, "f5": 12345, "f15": 1690.0, "f16": 1670.0, "f17": 1675.0, "f18": 1672.0},
                {"f12": "000001", "f2": 10.53, "f5": 99999, "f15": 10.6, "f16": 10.4, "f17": 10.45, "f18": 10.5},
            ],
        },
    }
)

#: 新浪备源示例响应（33 个字段）
BACKUP_TEXT = (
    'var hq_str_sh600519="贵州茅台,1675.00,1672.00,1680.50,1690.00,1670.00,1680.00,'
    "1680.60,12345,2000000,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2024-01-02,15:00:00,00\";\n"
    'var hq_str_sz000001="平安银行,10.45,10.50,10.53,10.60,10.40,10.52,10.54,'
    "99999,900000,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2024-01-02,15:00:00,00\";"
)

KLINE_TEXT = json.dumps(
    {
        "data": {
            "klines": [
                "2024-01-02 09:31,1670.00,1675.00,1678.00,1668.00,1200,2000000.0",
                "2024-01-02 09:32,1675.00,1680.00,1682.00,1674.00,900,1500000.0",
            ]
        }
    }
)


def _make_adapter(**kwargs) -> EastmoneyPollAdapter:
    defaults = dict(
        watchlist=["600519.SH", "000001.SZ"],
        poll_interval_sec=2.5,
        http_get=lambda url: _async_return(""),
        sleep_func=lambda sec: _async_return(None),
    )
    defaults.update(kwargs)
    return EastmoneyPollAdapter(**defaults)


async def _async_return(value):
    return value


def test_secid_and_sina_code_mapping():
    assert EastmoneyPollAdapter._secid("600519.SH") == "1.600519"
    assert EastmoneyPollAdapter._secid("000001.SZ") == "0.000001"
    assert EastmoneyPollAdapter._sina_code("600519.SH") == "sh600519"
    assert EastmoneyPollAdapter._sina_code("000001.SZ") == "sz000001"


def test_parse_primary():
    adapter = _make_adapter()
    quotes = adapter._parse_primary(PRIMARY_TEXT)
    assert [q.symbol for q in quotes] == ["600519.SH", "000001.SZ"]
    assert quotes[0].price == 1680.5
    assert quotes[0].ts_ms == 1704092400000
    assert quotes[1].cum_volume == 99999


def test_parse_backup():
    adapter = _make_adapter()
    quotes = adapter._parse_backup(BACKUP_TEXT)
    assert [q.symbol for q in quotes] == ["600519.SH", "000001.SZ"]
    assert quotes[0].price == 1680.5
    expected = int(datetime(2024, 1, 2, 15, 0, 0, tzinfo=_CN).timestamp() * 1000)
    assert quotes[0].ts_ms == expected


def test_parse_kline_and_preheat_success():
    async def http_get(url: str) -> str:
        assert "push2his" in url
        return KLINE_TEXT

    adapter = _make_adapter(http_get=http_get)

    async def _run():
        return await adapter.preheat(limit=300)

    result = asyncio.run(_run())
    assert "600519.SH" in result and "000001.SZ" in result
    candles = result["600519.SH"]
    assert len(candles) == 2
    assert all(c.is_closed for c in candles)
    assert candles[0].open == 1670.0 and candles[0].close == 1675.0


def test_preheat_failure_degrades_to_empty():
    async def http_get(url: str) -> str:
        raise RuntimeError("网络不可用")

    adapter = _make_adapter(http_get=http_get)

    async def _run():
        return await adapter.preheat()

    assert asyncio.run(_run()) == {}


def test_aggregator_closes_on_minute_rollover():
    agg = CandleAggregator("eastmoney")
    base = 1_700_000_000_000  # 任意 epoch ms
    minute1 = (base // 60_000) * 60_000

    closed1, tick1 = agg.add(Quote("600519.SH", 100.0, 1000, minute1))
    assert closed1 == [] and isinstance(tick1, Tick)
    closed2, tick2 = agg.add(Quote("600519.SH", 105.0, 1500, minute1 + 1000))
    assert closed2 == []  # 同一分钟无收盘
    closed3, _ = agg.add(Quote("600519.SH", 102.0, 2000, minute1 + 60_000))
    assert len(closed3) == 1  # 分钟翻滚，上一根收盘
    candle = closed3[0]
    assert isinstance(candle, Candle) and candle.is_closed
    assert candle.open == 100.0
    assert candle.high == 105.0 and candle.low == 100.0
    assert candle.volume == 500.0  # 分钟量 = 1000 基线起，累计量差分
    assert candle.open_time_ms == minute1


def test_events_failover_to_backup_and_backoff_sequence():
    slept: list[float] = []

    async def http_get(url: str) -> str:
        if "push2" in url:
            raise RuntimeError("主源不可用")
        return BACKUP_TEXT

    async def fake_sleep(sec: float) -> None:
        slept.append(sec)
        if sec == 2.5:  # 成功后的正常轮询间隔 → 停止
            adapter._closed = True

    adapter = _make_adapter(
        http_get=http_get, sleep_func=fake_sleep, max_failover=3
    )

    async def _run():
        return [e async for e in adapter.events()]

    events = asyncio.run(_run())
    # 主源失败 3 次退避 1/2/4，随后切备源成功
    assert slept[:3] == [1.0, 2.0, 4.0]
    assert any(isinstance(e, Tick) and e.source == "eastmoney" for e in events)
    assert adapter._source_index == 0  # 备源成功后切回主源
