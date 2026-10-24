"""轮询适配器共享基座：快照聚合 1min K 线 + 退避重试 + 主备切换。

设计要点（对应 M2-D2/D5）：
- 每标的维护「当前未收盘分钟 Candle」，以数据源时间戳判定自然分钟边界；
- 分钟翻滚时关闭上一根（``is_closed=True``）并开新根；
- 成交量按累计量（day cumulative）差分得到分钟量；
- 每次轮询同时产出一条 Tick（更新最新价展示）；
- 拉取失败按指数退避（1→2→4→…→上限秒封顶），成功即重置；
- 多源时主源连续失败达阈值切备源，备源成功一次后切回主源。
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable, Optional

from quant.feeds.base import FeedAdapter, FeedEvent, FeedStatus
from quant.models import Candle, Tick
from quant.utils.timeutils import MS_PER_MINUTE, align_minute_ms, now_ms

logger = logging.getLogger("quant.feeds.polling")

#: HTTP GET 函数类型：``url -> 响应文本``
HttpGet = Callable[[str], Awaitable[str]]

#: 异步睡眠函数类型（测试注入退避记录器）
SleepFunc = Callable[[float], Awaitable[None]]

#: 时间函数类型（测试注入固定 now）
NowFunc = Callable[[], int]


@dataclass(slots=True)
class Quote:
    """一次轮询得到的单标的快照（价格/累计量/数据源时间）。"""

    symbol: str
    price: float
    cum_volume: float
    ts_ms: int
    high: float = 0.0
    low: float = 0.0
    open: float = 0.0
    prev_close: float = 0.0


class CandleAggregator:
    """把离散轮询快照按自然分钟聚合成 1min Candle。"""

    def __init__(self, source: str) -> None:
        """初始化聚合器。

        参数:
            source: 数据源标识（写入 Tick.source，如 ``"eastmoney"``）。
        """
        self.source = source
        self._current: dict[str, Candle] = {}
        self._last_cum: dict[str, float] = {}

    def add(self, quote: Quote) -> tuple[list[Candle], Tick]:
        """吸收一条快照，返回 ``(新收盘的 K 线列表, 最新 Tick)``。

        分钟翻滚时会把上一根未收盘 Candle 关闭并放入返回列表（可能为空）。
        """
        ts = int(quote.ts_ms)
        open_ms = align_minute_ms(ts)
        closed: list[Candle] = []
        cur = self._current.get(quote.symbol)
        prev_cum = self._last_cum.get(quote.symbol)

        if cur is None or cur.open_time_ms != open_ms:
            if cur is not None:
                cur.is_closed = True
                closed.append(cur)
            minute_vol = 0.0
            cur = Candle(
                symbol=quote.symbol,
                open_time_ms=open_ms,
                close_time_ms=open_ms + MS_PER_MINUTE - 1,
                open=quote.open or quote.price,
                high=quote.price,
                low=quote.price,
                close=quote.price,
                volume=0.0,
                quote_volume=0.0,
                trades=0,
                is_closed=False,
            )
        else:
            minute_vol = cur.volume

        cur.high = max(cur.high, quote.price)
        cur.low = min(cur.low, quote.price)
        cur.close = quote.price

        # 分钟量差分（累计量下降视为新交易日重置，delta 记 0）
        delta = 0.0
        if prev_cum is not None and quote.cum_volume >= prev_cum:
            delta = quote.cum_volume - prev_cum
        cur.volume = minute_vol + delta
        self._last_cum[quote.symbol] = quote.cum_volume
        self._current[quote.symbol] = cur

        tick = Tick(
            symbol=quote.symbol,
            price=quote.price,
            quantity=delta,
            trade_time_ms=ts,
            is_buyer_maker=False,
            source=self.source,
        )
        return closed, tick

    def flush(self) -> list[Candle]:
        """关闭全部当前未收盘 Candle（停止时调用，返回已收盘列表）。"""
        closed: list[Candle] = []
        for cur in self._current.values():
            cur.is_closed = True
            closed.append(cur)
        self._current.clear()
        return closed


class PollingFeedAdapter(FeedAdapter):
    """轮询适配器抽象基类：拉取 → 聚合 → 退避/主备切换。"""

    name = "polling"
    latency_tag = ""

    def __init__(
        self,
        watchlist: list[str],
        poll_interval_sec: float = 3.0,
        proxy: Optional[str] = None,
        http_get: Optional[HttpGet] = None,
        sleep_func: Optional[SleepFunc] = None,
        now_func: Optional[NowFunc] = None,
        initial_backoff_sec: float = 1.0,
        max_backoff_sec: float = 30.0,
        max_failover: int = 3,
        sources: tuple[str, ...] = ("primary",),
    ) -> None:
        """初始化轮询适配器。

        参数:
            watchlist: 自选标的清单（按 D3 代码规范）。
            poll_interval_sec: 正常轮询间隔（秒）。
            proxy: 本地代理（默认 HTTP GET 使用）。
            http_get: HTTP GET 函数（测试注入伪造响应）。
            sleep_func: 异步睡眠函数（测试记录退避/轮询序列）。
            now_func: 当前 epoch ms 函数（测试固定时间）。
            initial_backoff_sec: 失败退避初始秒数。
            max_backoff_sec: 失败退避上限（秒）。
            max_failover: 主源连续失败达到该次数切换备源。
            sources: 源优先级（``("primary", "backup")`` 表示有备源）。
        """
        self.watchlist = list(watchlist)
        self.poll_interval_sec = float(poll_interval_sec)
        self.proxy = proxy
        self._http_get: HttpGet = http_get if http_get is not None else self._default_http_get()
        self._sleep: SleepFunc = sleep_func if sleep_func is not None else asyncio.sleep
        self._now: NowFunc = now_func if now_func is not None else now_ms
        self._initial_backoff_sec = float(initial_backoff_sec)
        self._backoff_sec = float(initial_backoff_sec)
        self.max_backoff_sec = float(max_backoff_sec)
        self.max_failover = int(max_failover)
        self._sources = list(sources) or ["primary"]
        self._source_index = 0
        self._fail_count = 0
        self._closed = False
        self._status = FeedStatus(detail="未连接")
        self._aggregator = CandleAggregator(self.name)

    # ------------------------------------------------------------------
    # 子类实现
    # ------------------------------------------------------------------

    async def _fetch(self, source: str) -> list[Quote]:
        """拉取一批快照（子类按 source 实现主/备源解析）。"""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # FeedAdapter 接口
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """复位停止标志（轮询无长连接）。"""
        self._closed = False
        logger.info("%s 轮询适配器就绪：%d 个标的", self.name, len(self.watchlist))

    async def close(self) -> None:
        """停止轮询并关闭未收盘 Candle（幂等）。"""
        self._closed = True
        self._status.connected = False
        self._status.detail = "已关闭"

    def status(self) -> FeedStatus:
        """返回连接状态浅拷贝。"""
        return FeedStatus(
            connected=self._status.connected,
            reconnect_count=self._status.reconnect_count,
            last_msg_monotonic=self._status.last_msg_monotonic,
            detail=self._status.detail,
        )

    async def events(self) -> AsyncIterator[FeedEvent]:
        """持续轮询并产出行情事件；内部处理退避与主备切换。"""
        primary = self._sources[0]
        while not self._closed:
            source = self._sources[self._source_index]
            try:
                quotes = await self._fetch(source)
            except Exception as exc:  # 拉取失败：退避 + 可能的源切换
                self._fail_count += 1
                self._status.connected = False
                self._status.detail = f"{source} 失败: {exc}"
                logger.warning(
                    "%s 轮询失败（源=%s，第 %d 次）：%s",
                    self.name,
                    source,
                    self._fail_count,
                    exc,
                )
                if len(self._sources) > 1 and self._fail_count >= self.max_failover:
                    self._source_index = (self._source_index + 1) % len(self._sources)
                    self._fail_count = 0
                    switched = self._sources[self._source_index]
                    logger.warning("%s 连续失败，切换数据源 → %s", self.name, switched)
                if self._closed:
                    break
                await self._sleep_and_grow()
                continue

            # 成功：重置失败计数与退避；备源成功后切回主源
            self._fail_count = 0
            self._backoff_sec = self._initial_backoff_sec
            self._status.connected = True
            self._status.reconnect_count += 1
            self._status.last_msg_monotonic = time.monotonic()
            self._status.detail = f"{source} 已连接"

            for quote in quotes:
                closed, tick = self._aggregator.add(quote)
                for candle in closed:
                    yield candle
                yield tick

            if self._source_index != 0:
                self._source_index = 0
                logger.info("%s 备源恢复成功，切回主源 %s", self.name, primary)

            if self._closed:
                break
            await self._sleep(self.poll_interval_sec)

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    async def _sleep_and_grow(self) -> None:
        """按当前退避秒数睡眠，随后翻倍并封顶。"""
        await self._sleep(self._backoff_sec)
        self._backoff_sec = min(self._backoff_sec * 2, self.max_backoff_sec)

    def _default_http_get(self) -> HttpGet:
        """默认 HTTP GET：httpx 异步客户端 + 显式代理 + 新浪 Referer。"""

        async def _get(url: str) -> str:
            import httpx  # 延迟导入：测试环境无需真实网络

            async with httpx.AsyncClient(
                proxy=self.proxy,
                timeout=10.0,
                headers={"Referer": "https://finance.sina.com.cn"},
            ) as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.text

        return _get


def to_float(value: object) -> Optional[float]:
    """宽松地把字段转换为 float；无法转换或空值返回 ``None``。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text in ("", "-", "--", "None", "null"):
        return None
    try:
        return float(text)
    except ValueError:
        return None
