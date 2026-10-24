"""币安 REST 客户端（``data-api.binance.vision``，httpx 实现）。

- 重试策略：初始 1 次 + 最多 ``max_retries``（默认 3）次重试，
  退避 0.5s/1s/2s；仅对网络错误/超时、5xx、429 重试（429 尊重
  ``Retry-After``），4xx 直接抛出；
- 可注入 ``httpx.AsyncClient``（测试用 ``httpx.MockTransport``），
  也可注入异步 ``sleep_func`` 加速重试测试；
- 只访问 .vision 镜像域名（配置层已强校验）。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional

import httpx

from quant.models import Candle
from quant.utils.timeutils import now_ms

logger = logging.getLogger("quant.feeds.binance_rest")

#: 币安 K 线接口路径
KLINES_PATH = "/api/v3/klines"

#: 默认 REST 基地址（.vision 镜像）
DEFAULT_BASE_URL = "https://data-api.binance.vision"

#: 重试退避序列（秒）：0.5 / 1 / 2
_BACKOFF_STEPS = (0.5, 1.0, 2.0)

#: 异步睡眠函数类型（便于测试注入记录器）
SleepFunc = Callable[[float], Awaitable[None]]


class BinanceRestClient:
    """币安 REST 客户端：K 线拉取 + 指数退避重试。"""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        proxy: Optional[str] = None,
        timeout_sec: float = 10.0,
        max_retries: int = 3,
        client: Optional[httpx.AsyncClient] = None,
        sleep_func: Optional[SleepFunc] = None,
    ) -> None:
        """初始化客户端。

        参数:
            base_url: REST 基地址（默认 .vision 域名）。
            proxy: 本地代理地址（注入 ``client`` 时忽略）。
            timeout_sec: 单次请求超时（秒）。
            max_retries: 最大重试次数（不含首次请求）。
            client: 可注入的 ``httpx.AsyncClient``（测试 MockTransport）。
            sleep_func: 可注入的异步睡眠函数（测试记录退避序列）。
        """
        self.base_url = base_url
        self.proxy = proxy
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.client: httpx.AsyncClient = (
            client
            if client is not None
            else httpx.AsyncClient(base_url=base_url, proxy=proxy, timeout=timeout_sec)
        )
        self._sleep: SleepFunc = sleep_func if sleep_func is not None else asyncio.sleep

    async def klines(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 300,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> list[Candle]:
        """拉取 K 线并解析为 :class:`~quant.models.Candle` 列表（均为已收盘）。

        参数:
            symbol: 币种（如 ``"BTCUSDT"``，内部转大写）。
            interval: K 线周期（默认 ``"1m"``）。
            limit: 数量上限（默认 300）。
            start_ms: 起始开盘时间（epoch ms，映射为 ``startTime``）。
            end_ms: 结束开盘时间（epoch ms，映射为 ``endTime``）。
        """
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": int(limit),
        }
        if start_ms is not None:
            params["startTime"] = int(start_ms)
        if end_ms is not None:
            params["endTime"] = int(end_ms)

        raw = await self._request_with_retry(KLINES_PATH, params)
        candles: list[Candle] = []
        for row in raw:
            candle = _parse_kline_row(symbol.upper(), row)
            if candle is not None:
                candles.append(candle)
        return candles

    async def _request_with_retry(self, path: str, params: dict[str, Any]) -> Any:
        """带重试的 GET 请求，返回解析后的 JSON。

        重试条件：网络错误/超时、5xx、429；其余 4xx 直接
        ``raise_for_status()`` 抛出。429 尊重 ``Retry-After`` 头。
        """
        attempt = 0
        while True:
            try:
                response = await self.client.get(path, params=params)
            except httpx.TransportError as exc:  # 含超时与连接类错误
                if attempt >= self.max_retries:
                    raise
                delay = self._backoff_sec(attempt)
                logger.warning(
                    "REST 网络错误（第 %d/%d 次尝试）：%s；%.1fs 后重试",
                    attempt + 1,
                    self.max_retries + 1,
                    exc,
                    delay,
                )
                await self._sleep(delay)
                attempt += 1
                continue

            if response.status_code >= 400:
                retryable = response.status_code == 429 or response.status_code >= 500
                if retryable and attempt < self.max_retries:
                    delay = self._retry_delay(response, attempt)
                    logger.warning(
                        "REST HTTP %d（第 %d/%d 次尝试）；%.1fs 后重试",
                        response.status_code,
                        attempt + 1,
                        self.max_retries + 1,
                        delay,
                    )
                    await self._sleep(delay)
                    attempt += 1
                    continue
                # 4xx 或重试用尽：直接抛出（含 429/5xx 耗尽）
                response.raise_for_status()

            return response.json()

    def _backoff_sec(self, attempt: int) -> float:
        """第 ``attempt`` 次重试（从 0 计）的退避秒数：0.5/1/2。"""
        return _BACKOFF_STEPS[min(attempt, len(_BACKOFF_STEPS) - 1)]

    def _retry_delay(self, response: httpx.Response, attempt: int) -> float:
        """429 时尊重 Retry-After，否则用指数退避。"""
        if response.status_code == 429:
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            if retry_after is not None:
                return retry_after
        return self._backoff_sec(attempt)

    async def aclose(self) -> None:
        """关闭底层 HTTP 客户端。"""
        await self.client.aclose()


def _parse_kline_row(symbol: str, row: Any) -> Optional[Candle]:
    """解析 K 线接口返回的一行（12 元素数组，数值为字符串）。"""
    if not isinstance(row, (list, tuple)) or len(row) < 9:
        logger.warning("REST K 线行格式非法（已跳过）：%.120s", row)
        return None
    try:
        return Candle(
            symbol=symbol,
            open_time_ms=int(row[0]),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            close_time_ms=int(row[6]),
            quote_volume=float(row[7]),
            trades=int(row[8]),
            is_closed=True,  # REST 返回的均为已收盘 K 线
        )
    except (TypeError, ValueError) as exc:
        logger.warning("REST K 线行数值解析失败（已跳过）：%s", exc)
        return None


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    """解析 ``Retry-After`` 头（秒）；HTTP-date 形式返回 None。"""
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return max(seconds, 0.0)


def new_rest_client(settings: Any) -> BinanceRestClient:
    """按配置构造 REST 客户端（代理/超时/重试来自 :class:`~quant.config.Settings`）。"""
    return BinanceRestClient(
        base_url=settings.network.rest_url,
        proxy=settings.network.proxy,
        timeout_sec=settings.network.rest_timeout_sec,
    )


def rest_now_ms() -> int:
    """当前 epoch ms（供 gap 修复判断已收盘，测试可注入替代）。"""
    return now_ms()
