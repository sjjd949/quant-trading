"""A股轮询适配器：东方财富 push2 主源 + 新浪 hq 备源（约 3s 轮询）。

- 统一 ``Candle`` 模型：轮询快照按自然分钟聚合，分钟收盘 ``is_closed=True``；
- 时间戳取数据源时间（东财 ``f124`` / 新浪日期时间），非本地接收时间；
- 主源连续失败达阈值切备源，备源成功一次后切回主源；
- 指数退避 1→30s 封顶；
- 可选预热：拉最近 N 根 1min K 线（失败降级冷启动）。
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from quant.feeds.polling_base import (
    HttpGet,
    PollingFeedAdapter,
    Quote,
    SleepFunc,
    NowFunc,
    to_float,
)
from quant.models import Candle

logger = logging.getLogger("quant.feeds.eastmoney_poll")

#: 东财批量行情端点（fltt=2 返回两位小数，无需手工缩放）
PRIMARY_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"
#: 东财 1min K 线历史端点（预热）
KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
#: 新浪行情端点（备源，需 Referer）
BACKUP_URL = "https://hq.sinajs.cn/list="
#: A股交易所时区（UTC+8）
_CN_TZ = timezone(timedelta(hours=8))


class EastmoneyPollAdapter(PollingFeedAdapter):
    """A股轮询适配器（东财主 / 新浪备）。"""

    name = "eastmoney"
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
    ) -> None:
        """初始化 A股轮询适配器（主备双源）。"""
        super().__init__(
            watchlist,
            poll_interval_sec=poll_interval_sec,
            proxy=proxy,
            http_get=http_get,
            sleep_func=sleep_func,
            now_func=now_func,
            initial_backoff_sec=initial_backoff_sec,
            max_backoff_sec=max_backoff_sec,
            max_failover=max_failover,
            sources=("primary", "backup"),
        )

    # ------------------------------------------------------------------
    # 拉取
    # ------------------------------------------------------------------

    async def _fetch(self, source: str) -> list[Quote]:
        """按源拉取并解析快照。"""
        if source == "primary":
            return await self._fetch_primary()
        return await self._fetch_backup()

    async def _fetch_primary(self) -> list[Quote]:
        """东财 push2 批量行情。"""
        secids = ",".join(self._secid(symbol) for symbol in self.watchlist)
        url = (
            f"{PRIMARY_URL}?fltt=2&invt=2"
            "&fields=f2,f3,f5,f12,f13,f15,f16,f17,f18,f124"
            f"&secids={secids}"
        )
        text = await self._http_get(url)
        return self._parse_primary(text)

    async def _fetch_backup(self) -> list[Quote]:
        """新浪 hq 行情（备源）。"""
        codes = ",".join(self._sina_code(symbol) for symbol in self.watchlist)
        text = await self._http_get(f"{BACKUP_URL}{codes}")
        return self._parse_backup(text)

    # ------------------------------------------------------------------
    # 解析
    # ------------------------------------------------------------------

    def _parse_primary(self, text: str) -> list[Quote]:
        """解析东财 ``ulist.np`` 响应。"""
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("东财响应非 JSON 对象")
        if data.get("rc") not in (0, None):
            raise ValueError(f"东财返回异常 rc={data.get('rc')}")
        payload = data.get("data")
        if not isinstance(payload, dict):
            raise ValueError("东财响应缺少 data 字段")
        ts_ms = int(to_float(payload.get("f124")) or 0) * 1000 or self._now()
        code_to_symbol = {symbol.split(".")[0]: symbol for symbol in self.watchlist}

        quotes: list[Quote] = []
        for item in payload.get("diff") or []:
            if not isinstance(item, dict):
                continue
            symbol = code_to_symbol.get(str(item.get("f12", "")))
            if symbol is None:
                continue
            price = to_float(item.get("f2"))
            if price is None or price <= 0:  # 停牌/无报价
                continue
            quotes.append(
                Quote(
                    symbol=symbol,
                    price=price,
                    cum_volume=to_float(item.get("f5")) or 0.0,
                    ts_ms=ts_ms,
                    high=to_float(item.get("f15")) or price,
                    low=to_float(item.get("f16")) or price,
                    open=to_float(item.get("f17")) or price,
                    prev_close=to_float(item.get("f18")) or price,
                )
            )
        return quotes

    def _parse_backup(self, text: str) -> list[Quote]:
        """解析新浪 ``hq_str_<code>`` 响应。"""
        code_to_symbol = {self._sina_code(symbol): symbol for symbol in self.watchlist}
        quotes: list[Quote] = []
        for line in text.splitlines():
            match = re.match(r'var hq_str_(\w+)="([^"]*)"', line.strip())
            if not match:
                continue
            symbol = code_to_symbol.get(match.group(1))
            payload = match.group(2)
            if symbol is None or not payload:
                continue
            parts = payload.split(",")
            # 新浪 A股字段：0 名称 1 今开 2 昨收 3 现价 4 最高 5 最低 ... 8 成交量 30 日期 31 时间
            if len(parts) < 32:
                continue
            price = to_float(parts[3])
            if price is None or price <= 0:
                continue
            ts_ms = _cn_dt_to_ms(parts[30], parts[31]) or self._now()
            quotes.append(
                Quote(
                    symbol=symbol,
                    price=price,
                    cum_volume=to_float(parts[8]) or 0.0,
                    ts_ms=ts_ms,
                    high=to_float(parts[4]) or price,
                    low=to_float(parts[5]) or price,
                    open=to_float(parts[1]) or price,
                    prev_close=to_float(parts[2]) or price,
                )
            )
        return quotes

    # ------------------------------------------------------------------
    # 代码映射
    # ------------------------------------------------------------------

    @staticmethod
    def _secid(symbol: str) -> str:
        """``600519.SH`` → ``1.600519``；``000001.SZ`` → ``0.000001``。"""
        upper = symbol.upper()
        code = upper.split(".")[0]
        market = "1" if upper.endswith(".SH") else "0"
        return f"{market}.{code}"

    @staticmethod
    def _sina_code(symbol: str) -> str:
        """``600519.SH`` → ``sh600519``；``000001.SZ`` → ``sz000001``。"""
        upper = symbol.upper()
        code = upper.split(".")[0]
        prefix = "sh" if upper.endswith(".SH") else "sz"
        return f"{prefix}{code}"

    # ------------------------------------------------------------------
    # 预热
    # ------------------------------------------------------------------

    async def preheat(self, limit: int = 300) -> dict[str, list[Candle]]:
        """拉取最近 ``limit`` 根 1min K 线（失败降级为空，不抛异常）。

        返回:
            ``{symbol: [Candle, ...]}``；失败标的不在结果中（调用方冷启动）。
        """
        result: dict[str, list[Candle]] = {}
        for symbol in self.watchlist:
            try:
                text = await self._http_get(self._kline_url(symbol, limit))
                candles = self.parse_kline(text, symbol)
                if candles:
                    result[symbol] = candles
            except Exception as exc:
                logger.warning("A股预热失败（%s）：%s", symbol, exc)
        return result

    def _kline_url(self, symbol: str, limit: int) -> str:
        """构造东财 1min K 线历史 URL。"""
        return (
            f"{KLINE_URL}?secid={self._secid(symbol)}&klt=1&fqt=1&lmt={int(limit)}"
            "&fields1=f1,f2,f3,f4,f5,f6"
            "&fields2=f51,f52,f53,f54,f55,f56,f57"
        )

    @staticmethod
    def parse_kline(text: str, symbol: str) -> list[Candle]:
        """解析东财 K 线响应为已收盘 Candle 列表。

        每行格式：``时间,开,收,高,低,量,额``（``f51..f57``）。
        """
        data = json.loads(text)
        payload = data.get("data") if isinstance(data, dict) else None
        if not isinstance(payload, dict):
            return []
        candles: list[Candle] = []
        for row in payload.get("klines") or []:
            parts = str(row).split(",")
            if len(parts) < 7:
                continue
            ts_ms = _cn_dt_to_ms(parts[0][:10], parts[0][11:19])
            if ts_ms is None:
                continue
            open_ = to_float(parts[1])
            close = to_float(parts[2])
            high = to_float(parts[3])
            low = to_float(parts[4])
            volume = to_float(parts[5]) or 0.0
            if None in (open_, close, high, low):
                continue
            candles.append(
                Candle(
                    symbol=symbol,
                    open_time_ms=ts_ms,
                    close_time_ms=ts_ms + 59_999,
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                    quote_volume=to_float(parts[6]) or 0.0,
                    trades=0,
                    is_closed=True,
                )
            )
        return candles


def _cn_dt_to_ms(date_text: str, time_text: str) -> Optional[int]:
    """A股本地时间（UTC+8）``YYYY-MM-DD`` + ``HH:MM[:SS]`` → epoch ms。"""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(f"{date_text} {time_text}", fmt)
        except ValueError:
            continue
        aware = dt.replace(tzinfo=_CN_TZ)
        return int(aware.timestamp() * 1000)
    return None
