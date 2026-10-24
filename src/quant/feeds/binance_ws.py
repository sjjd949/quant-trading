"""币安 WebSocket 适配器（``data-stream.binance.vision`` 组合流）。

能力：
- multiplexed 组合流订阅（每币种 ``@trade`` + ``@kline_1m``，一条连接）；
- 协议级心跳（``ping_interval=20/ping_timeout=10``）+ 业务级空闲看门狗
  （``wait_for(recv, idle_timeout_sec)``）；
- 断线指数退避重连（1→2→4→…→30s 封顶，连接成功即重置）；
- 重连后先做 gap 修复（REST 补齐缺失的已收盘 K 线，上限 5 根），
  再恢复实时流；
- ``connector`` 可注入：测试传入伪造连接回放录制帧，零真实网络；
- ``events()`` 永不向上抛异常（断线收敛为循环内重连）。
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from collections import deque
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

from quant.feeds.base import FeedAdapter, FeedEvent, FeedStatus
from quant.feeds.binance_rest import BinanceRestClient
from quant.models import Candle, Tick
from quant.utils.timeutils import MS_PER_MINUTE, now_ms

logger = logging.getLogger("quant.feeds.binance_ws")

#: 断线 gap 修复的最大补齐根数（超出记警告）
GAP_REPAIR_LIMIT = 5

#: 每币种已 yield 收盘 K 线开盘时间去重窗口（防补洞与实时流重复）
_SEEN_KLINES_MAXLEN = 16

#: 连接工厂类型：``connector(url) -> awaitable ws``
Connector = Callable[[str], Awaitable[Any]]

#: 异步睡眠函数类型（测试注入退避记录器）
SleepFunc = Callable[[float], Awaitable[None]]

#: 时间函数类型（测试注入固定 now）
NowFunc = Callable[[], int]


class BinanceWSAdapter(FeedAdapter):
    """币安组合流 WS 适配器。"""

    name = "binance_ws"

    def __init__(
        self,
        symbols: list[str],
        ws_url: str = "wss://data-stream.binance.vision/stream",
        proxy: Optional[str] = None,
        idle_timeout_sec: float = 60.0,
        rest_client: Optional[BinanceRestClient] = None,
        connector: Optional[Connector] = None,
        initial_backoff_sec: float = 1.0,
        max_backoff_sec: float = 30.0,
        sleep_func: Optional[SleepFunc] = None,
        now_func: Optional[NowFunc] = None,
    ) -> None:
        """初始化适配器。

        参数:
            symbols: 监控币种列表（内部转大写）。
            ws_url: 组合流端点（.vision 域名）。
            proxy: 本地代理（注入 ``connector`` 时忽略）。
            idle_timeout_sec: 业务级空闲看门狗阈值（秒）。
            rest_client: 断线补洞用的 REST 客户端（可为 None，则跳过补洞）。
            connector: 连接工厂（测试注入伪造 WS）。
            initial_backoff_sec: 重连退避初始秒数。
            max_backoff_sec: 重连退避上限（秒）。
            sleep_func: 异步睡眠函数（测试记录退避序列）。
            now_func: 当前 epoch ms 函数（测试固定时间）。
        """
        self.symbols = [s.upper() for s in symbols]
        self.ws_url = ws_url
        self.proxy = proxy
        self.idle_timeout_sec = idle_timeout_sec
        self.rest_client = rest_client
        self._connector: Connector = connector if connector is not None else self._default_connector()
        self._initial_backoff_sec = initial_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self._backoff_sec = initial_backoff_sec
        self._sleep: SleepFunc = sleep_func if sleep_func is not None else asyncio.sleep
        self._now: NowFunc = now_func if now_func is not None else now_ms
        self._closed = False
        self._ws: Any = None
        self._status = FeedStatus()
        #: 每币种最后一根已收盘 K 线的开盘时间（gap 修复起点）
        self._last_kline_open_ms: dict[str, int] = {}
        #: 每币种已 yield 的收盘 K 线开盘时间窗口（去重）
        self._seen_kline_opens: dict[str, deque[int]] = {}

    # ------------------------------------------------------------------
    # FeedAdapter 接口
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """复位停止标志；实际连接由 :meth:`events` 内部按需建立。"""
        self._closed = False
        logger.info("WS 适配器就绪：%s", self.stream_url())

    async def close(self) -> None:
        """停止迭代并关闭底层连接（幂等）。"""
        self._closed = True
        ws = self._ws
        self._ws = None
        if ws is not None:
            await self._close_ws(ws)
        self._status.connected = False
        self._status.detail = "已关闭"

    def status(self) -> FeedStatus:
        """返回连接状态浅拷贝（渲染任务只读）。"""
        return FeedStatus(
            connected=self._status.connected,
            reconnect_count=self._status.reconnect_count,
            last_msg_monotonic=self._status.last_msg_monotonic,
            detail=self._status.detail,
        )

    async def events(self) -> AsyncIterator[FeedEvent]:
        """持续产出行情事件；内部处理连接/重连/看门狗/补洞。"""
        while not self._closed:
            try:
                ws = await self._connector(self.stream_url())
            except Exception as exc:  # 连接失败：退避后重试
                self._status.connected = False
                self._status.detail = f"连接失败: {exc}"
                logger.warning(
                    "WS 连接失败：%s；%.1fs 后重连（第 %d 次重连）",
                    exc,
                    self._backoff_sec,
                    self._status.reconnect_count + 1,
                )
                if self._closed:
                    break
                await self._sleep_and_grow()
                continue

            self._ws = ws
            self._status.connected = True
            self._status.detail = "已连接"
            self._backoff_sec = self._initial_backoff_sec  # 连接成功即重置
            self._status.reconnect_count += 1
            logger.info(
                "WS 已连接（第 %d 次）：%s", self._status.reconnect_count, self.stream_url()
            )

            try:
                # 重连场景：先补洞（缺失的已收盘 K 线），再恢复实时流
                for candle in await self._gap_repair():
                    yield candle
                while not self._closed:
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=self.idle_timeout_sec
                    )
                    self._status.last_msg_monotonic = time.monotonic()
                    for item in self._parse_raw(raw):
                        if isinstance(item, Candle) and item.is_closed:
                            if self._is_kline_seen(item):
                                logger.debug(
                                    "WS 跳过重复收盘 K 线：%s %d",
                                    item.symbol,
                                    item.open_time_ms,
                                )
                                continue
                            self._mark_kline_seen(item)
                            self._last_kline_open_ms[item.symbol] = item.open_time_ms
                        yield item
            except asyncio.TimeoutError:
                logger.warning(
                    "WS 空闲超过 %.0fs 无消息，准备重连", self.idle_timeout_sec
                )
                self._status.detail = "空闲超时重连"
            except Exception as exc:  # 连接中断/解析器异常：收敛为重连
                logger.warning("WS 连接中断：%s", exc)
                self._status.detail = f"连接中断: {exc}"
            finally:
                self._status.connected = False
                if self._ws is ws:
                    self._ws = None
                await self._close_ws(ws)

            if self._closed:
                break
            await self._sleep_and_grow()

    # ------------------------------------------------------------------
    # 订阅与解析
    # ------------------------------------------------------------------

    def stream_url(self) -> str:
        """构造 multiplexed 组合流 URL（小写币种，``@trade`` + ``@kline_1m``）。"""
        streams: list[str] = []
        for symbol in self.symbols:
            lowered = symbol.lower()
            streams.append(f"{lowered}@trade")
            streams.append(f"{lowered}@kline_1m")
        base = self.ws_url.rstrip("/")
        return f"{base}?streams={'/'.join(streams)}"

    def _parse_raw(self, raw: Any) -> list[FeedEvent]:
        """解析一条原始 WS 帧，产出 0~N 个事件；非法帧跳过并记日志。"""
        try:
            frame = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning("WS 收到非法帧（已跳过）：%s；%.120s", exc, raw)
            return []
        try:
            return self._parse_frame(frame)
        except Exception as exc:  # 防御：单帧字段异常不得中断事件流
            logger.warning("WS 帧解析异常（已跳过）：%s；%.120s", exc, raw)
            return []

    def _parse_frame(self, frame: Any) -> list[FeedEvent]:
        """解析组合流信封（``{"stream": ..., "data": ...}``）。"""
        if not isinstance(frame, dict):
            logger.warning("WS 帧非 JSON 对象（已跳过）")
            return []
        data = frame.get("data")
        if not isinstance(data, dict):
            logger.debug("WS 帧缺少 data 字段（已跳过）：stream=%s", frame.get("stream"))
            return []
        event = data.get("e")
        if event == "trade":
            return [self._parse_trade(data)]
        if event == "kline":
            candle = self._parse_kline(data)
            return [candle] if candle is not None else []
        logger.debug("WS 未知事件类型（已跳过）：e=%s", event)
        return []

    def _parse_trade(self, data: dict[str, Any]) -> Tick:
        """解析 ``@trade`` 帧为 :class:`~quant.models.Tick`。"""
        return Tick(
            symbol=str(data["s"]).upper(),
            price=float(data["p"]),
            quantity=float(data["q"]),
            trade_time_ms=int(data["T"]),
            is_buyer_maker=bool(data.get("m", False)),
            source=self.name,
        )

    def _parse_kline(self, data: dict[str, Any]) -> Optional[Candle]:
        """解析 ``@kline_1m`` 帧为 :class:`~quant.models.Candle`。"""
        kline = data.get("k")
        if not isinstance(kline, dict):
            logger.debug("WS kline 帧缺少 k 字段（已跳过）")
            return None
        symbol = str(kline.get("s") or data.get("s", "")).upper()
        return Candle(
            symbol=symbol,
            open_time_ms=int(kline["t"]),
            close_time_ms=int(kline["T"]),
            open=float(kline["o"]),
            high=float(kline["h"]),
            low=float(kline["l"]),
            close=float(kline["c"]),
            volume=float(kline["v"]),
            quote_volume=float(kline["q"]),
            trades=int(kline["n"]),
            is_closed=bool(kline.get("x", False)),
        )

    # ------------------------------------------------------------------
    # gap 修复与去重
    # ------------------------------------------------------------------

    async def _gap_repair(self) -> list[Candle]:
        """重连后按 ``startTime=最后一根已收盘 K 线开盘时间 + 1m`` 补洞。

        仅补齐已收盘且未 yield 过的 K 线，上限 :data:`GAP_REPAIR_LIMIT`
        根；超出上限记警告日志。无 REST 客户端或无历史时不补洞。
        """
        if self.rest_client is None or not self._last_kline_open_ms:
            return []
        now_ms_value = self._now()
        repaired: list[Candle] = []
        for symbol, last_open in sorted(self._last_kline_open_ms.items()):
            start_ms = last_open + MS_PER_MINUTE
            try:
                candles = await self.rest_client.klines(
                    symbol=symbol,
                    interval="1m",
                    start_ms=start_ms,
                    limit=GAP_REPAIR_LIMIT,
                )
            except Exception as exc:
                logger.warning("gap 修复失败（%s）：%s", symbol, exc)
                continue
            fresh = [
                candle
                for candle in candles
                if candle.is_closed
                and candle.close_time_ms <= now_ms_value
                and not self._is_kline_seen(candle)
            ]
            if len(candles) >= GAP_REPAIR_LIMIT and (
                candles[-1].close_time_ms + MS_PER_MINUTE <= now_ms_value
            ):
                logger.warning(
                    "%s 断线缺口超过 %d 根，仅按上限补齐（更早缺口不再回补）",
                    symbol,
                    GAP_REPAIR_LIMIT,
                )
            for candle in fresh:
                self._mark_kline_seen(candle)
                self._last_kline_open_ms[symbol] = max(
                    self._last_kline_open_ms.get(symbol, 0), candle.open_time_ms
                )
                repaired.append(candle)
            if fresh:
                logger.info(
                    "gap 修复：%s 补齐 %d 根已收盘 K 线（startTime=%d）",
                    symbol,
                    len(fresh),
                    start_ms,
                )
        return repaired

    def _is_kline_seen(self, candle: Candle) -> bool:
        """该收盘 K 线是否已 yield 过（防补洞与实时流重复轰炸）。"""
        return candle.open_time_ms in self._seen_kline_opens.get(candle.symbol, ())

    def _mark_kline_seen(self, candle: Candle) -> None:
        """记录已 yield 的收盘 K 线开盘时间（窗口容量有限）。"""
        window = self._seen_kline_opens.setdefault(
            candle.symbol, deque(maxlen=_SEEN_KLINES_MAXLEN)
        )
        window.append(candle.open_time_ms)

    # ------------------------------------------------------------------
    # 连接管理
    # ------------------------------------------------------------------

    def _default_connector(self) -> Connector:
        """默认连接工厂：``websockets.connect`` 显式传代理与协议级心跳。"""

        async def _connect(url: str) -> Any:
            import websockets  # 延迟导入：测试/无网环境不承担导入成本

            return await websockets.connect(
                url,
                proxy=self.proxy,
                ping_interval=20,
                ping_timeout=10,
                open_timeout=15,
            )

        return _connect

    async def _sleep_and_grow(self) -> None:
        """按当前退避秒数睡眠，随后翻倍并封顶（1→2→4→…→30→30）。"""
        await self._sleep(self._backoff_sec)
        self._backoff_sec = min(self._backoff_sec * 2, self.max_backoff_sec)

    async def _close_ws(self, ws: Any) -> None:
        """关闭底层连接（兼容同步/异步 ``close``，异常仅记日志）。"""
        try:
            close = getattr(ws, "close", None)
            if close is None:
                return
            result = close()
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            logger.debug("关闭 WS 连接异常（忽略）：%s", exc)
