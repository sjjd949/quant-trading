"""美股/港股轮询适配器：腾讯 qt.gtimg.cn（单源，延迟约 15 分钟）。

- 延迟数据：UI 与告警中必须明确标注「数据延迟约 15 分钟」（三处：状态栏 /
  行情表 / 告警前缀），本适配器提供 ``latency_tag="~15min"`` 供上层渲染；
- 触发时间戳取数据源时间（腾讯报价时间），不承诺实时性；
- 冷启动无预热（延迟源预热价值低），前 25 分钟仅积累不触发；
- 退避重试机制复用 :class:`~quant.feeds.polling_base.PollingFeedAdapter`。
"""

from __future__ import annotations

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

logger = logging.getLogger("quant.feeds.tencent_poll")

#: 腾讯批量行情端点
PRIMARY_URL = "https://qt.gtimg.cn/q="
#: 港股/美股交易时区（UTC+8，腾讯报价时间为北京时间）
_CN_TZ = timezone(timedelta(hours=8))


class TencentPollAdapter(PollingFeedAdapter):
    """美股/港股轮询适配器（腾讯单源，延迟约 15min）。"""

    name = "tencent"
    latency_tag = "~15min"

    def __init__(
        self,
        watchlist_us: list[str],
        watchlist_hk: list[str],
        poll_interval_sec: float = 3.0,
        proxy: Optional[str] = None,
        http_get: Optional[HttpGet] = None,
        sleep_func: Optional[SleepFunc] = None,
        now_func: Optional[NowFunc] = None,
        initial_backoff_sec: float = 1.0,
        max_backoff_sec: float = 30.0,
    ) -> None:
        """初始化美港股轮询适配器（单源）。"""
        watchlist = list(watchlist_us) + list(watchlist_hk)
        super().__init__(
            watchlist,
            poll_interval_sec=poll_interval_sec,
            proxy=proxy,
            http_get=http_get,
            sleep_func=sleep_func,
            now_func=now_func,
            initial_backoff_sec=initial_backoff_sec,
            max_backoff_sec=max_backoff_sec,
            sources=("primary",),
        )

    async def _fetch(self, source: str) -> list[Quote]:
        """腾讯单源拉取并解析。"""
        codes = ",".join(self._gt_code(symbol) for symbol in self.watchlist)
        text = await self._http_get(f"{PRIMARY_URL}{codes}")
        return self._parse(text)

    def _parse(self, text: str) -> list[Quote]:
        """解析腾讯 ``v_<code>="..."`` 响应（``~`` 分隔字段）。"""
        code_to_symbol = {self._gt_code(symbol): symbol for symbol in self.watchlist}
        quotes: list[Quote] = []
        for line in text.splitlines():
            match = re.match(r'v_(\w+)="([^"]*)"', line.strip())
            if not match:
                continue
            symbol = code_to_symbol.get(match.group(1))
            payload = match.group(2)
            if symbol is None or not payload:
                continue
            fields = payload.split("~")
            # 腾讯字段：3 现价 4 昨收 5 今开 6 成交量 30 时间 33 最高 34 最低
            if len(fields) < 35:
                continue
            price = to_float(fields[3])
            if price is None or price <= 0:
                continue
            ts_ms = _parse_gt_time(fields[30]) or self._now()
            quotes.append(
                Quote(
                    symbol=symbol,
                    price=price,
                    cum_volume=to_float(fields[6]) or 0.0,
                    ts_ms=ts_ms,
                    high=to_float(fields[33]) or price,
                    low=to_float(fields[34]) or price,
                    open=to_float(fields[5]) or price,
                    prev_close=to_float(fields[4]) or price,
                )
            )
        return quotes

    @staticmethod
    def _gt_code(symbol: str) -> str:
        """``AAPL.US`` → ``usAAPL``；``0700.HK`` → ``hk00700``。"""
        upper = symbol.upper()
        if upper.endswith(".US"):
            return "us" + upper[:-3]
        if upper.endswith(".HK"):
            return "hk" + upper[:-3].zfill(5)
        return upper


def _parse_gt_time(text: str) -> Optional[int]:
    """解析腾讯报价时间：``YYYYMMDDHHMMSS`` 或 ``YYYY-MM-DD HH:MM:SS``（北京时间）。"""
    digits = re.sub(r"\D", "", text or "")
    if len(digits) >= 14:
        try:
            dt = datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
        except ValueError:
            return None
        return int(dt.replace(tzinfo=_CN_TZ).timestamp() * 1000)
    return None
