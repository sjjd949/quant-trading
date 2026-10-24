"""行情适配器抽象基座。

P0 实现币安（.vision 域名）；P1 的 A股/美股适配器与模拟盘引擎
通过继承 :class:`FeedAdapter` 并复用 :mod:`quant.models` 统一模型挂接，
P0 代码结构上保证可插拔。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import AsyncIterator, Union

from quant.models import Candle, Tick

#: 适配器产出的事件类型（用 isinstance 判别）
FeedEvent = Union[Tick, Candle]


@dataclass(slots=True)
class FeedStatus:
    """适配器连接状态快照（供看板状态栏展示）。"""

    connected: bool = False
    reconnect_count: int = 0
    last_msg_monotonic: float = 0.0
    detail: str = ""


class FeedAdapter(ABC):
    """行情适配器抽象基类。

    约定：
    - ``events()`` 为异步生成器，yield :class:`~quant.models.Tick` 或
      :class:`~quant.models.Candle`；内部保证「先补洞数据、后实时数据」；
    - 仅当 ``Candle.is_closed=True`` 时该 K 线应进入指标/规则链路；
    - ``events()`` 永不向上抛异常（断线收敛为循环内重连）。
    """

    #: 适配器名称（子类覆盖，如 ``"binance_ws"``）
    name: str = ""

    #: 延迟数据标注（延迟源如腾讯 ``"~15min"``；实时源为空串）
    latency_tag: str = ""

    @abstractmethod
    async def connect(self) -> None:
        """建立连接（具体生命周期由 ``events()`` 内部管理的实现可仅复位状态）。"""

    @abstractmethod
    def events(self) -> AsyncIterator[FeedEvent]:
        """异步生成器：持续产出 Tick/Candle 事件。"""

    @abstractmethod
    async def close(self) -> None:
        """关闭连接并停止 ``events()`` 迭代。"""

    @abstractmethod
    def status(self) -> FeedStatus:
        """返回当前连接状态。"""
