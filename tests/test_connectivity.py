"""数据源连通性测试：验证三个市场的行情接口在当前网络环境下可用。

⚠️ 本模块需要真实外部网络，默认被 pyproject 的 ``-m 'not network'`` 排除；
仅手动运行：``pytest tests/test_connectivity.py -m network -v``。
"""

import asyncio
import json
import os

import httpx
import pytest
import websockets

# 整个模块标记为 network：默认不运行，保证本地/交付 pytest 全绿且不触网
pytestmark = pytest.mark.network

BINANCE_WS_URL = "wss://data-stream.binance.vision/ws/btcusdt@trade"
EASTMONEY_URL = (
    "https://push2.eastmoney.com/api/qt/clist/get"
    "?pn=1&pz=3&fs=m:0+t:6&fields=f12,f14,f2"
)
TENCENT_URL = "https://qt.gtimg.cn/q=usAAPL"

WS_TIMEOUT = 20
HTTP_TIMEOUT = 15


def _ws_proxy() -> str | None:
    return os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")


def test_binance_ws_realtime_trade():
    """币安公共 WebSocket 应能收到 BTC 实时成交流"""

    async def _run():
        async with websockets.connect(
            BINANCE_WS_URL, proxy=_ws_proxy(), open_timeout=15
        ) as ws:
            raw = await asyncio.wait_for(ws.recv(), timeout=WS_TIMEOUT)
            return json.loads(raw)

    trade = asyncio.run(_run())
    assert float(trade["p"]) > 0
    assert float(trade["q"]) > 0


def test_eastmoney_ashare_quotes():
    """东方财富 A 股行情接口应返回深市主板股票快照（fs=m:0+t:6）"""
    resp = httpx.get(EASTMONEY_URL, timeout=HTTP_TIMEOUT)
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"]["total"] > 1500
    assert len(body["data"]["diff"]) == 3


def test_tencent_us_stock_quote():
    """腾讯行情应返回美股 AAPL 实时报价"""
    resp = httpx.get(TENCENT_URL, timeout=HTTP_TIMEOUT)
    assert resp.status_code == 200
    assert "v_usAAPL" in resp.text
    assert "Apple" in resp.text
