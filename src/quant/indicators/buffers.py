"""指标缓冲区：泛型环形缓冲（``collections.deque(maxlen)`` 封装）。"""

from __future__ import annotations

from collections import deque
from typing import Generic, TypeVar

T = TypeVar("T")


class RingBuffer(Generic[T]):
    """定容环形缓冲：超出容量时自动淘汰最旧元素。

    用途：每币种已收盘 1min K 线窗口（``maxlen=300``）与 Tick 环
    （``maxlen=512``，P0 仅更新最新价展示）。
    """

    def __init__(self, maxlen: int) -> None:
        """初始化环形缓冲。

        参数:
            maxlen: 容量上限（须为正整数）。

        异常:
            ValueError: ``maxlen`` 非正。
        """
        if maxlen <= 0:
            raise ValueError(f"maxlen 必须为正整数，当前为 {maxlen}")
        self.maxlen = maxlen
        self._items: deque[T] = deque(maxlen=maxlen)

    def append(self, item: T) -> None:
        """追加元素；超出容量时淘汰最旧元素。"""
        self._items.append(item)

    def to_list(self) -> list[T]:
        """按从旧到新顺序返回元素列表（浅拷贝）。"""
        return list(self._items)

    def __len__(self) -> int:
        """当前元素个数。"""
        return len(self._items)

    def clear(self) -> None:
        """清空全部元素。"""
        self._items.clear()
