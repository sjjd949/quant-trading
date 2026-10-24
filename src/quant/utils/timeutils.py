"""时间工具：内部统一 epoch 毫秒（UTC int），展示层 ISO 8601。

约定：
- 内部存储与计算一律 ``int`` epoch 毫秒（UTC）；
- 展示层经 :func:`to_iso` 输出 ``2025-10-24T06:32:05.123Z`` 形式；
- 测试注入固定 epoch ms，禁止依赖 ``datetime.now()``。
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

#: 每分钟毫秒数
MS_PER_MINUTE = 60_000

#: Unix 纪元（UTC），用于精确的整数毫秒换算
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def now_ms() -> int:
    """当前 UTC 时间的 epoch 毫秒整数。"""
    return int(time.time() * 1000)


def ms_to_datetime(ms: int) -> datetime:
    """epoch 毫秒 -> 带 UTC 时区的 ``datetime``（精确到毫秒）。"""
    return _EPOCH + timedelta(milliseconds=int(ms))


def datetime_to_ms(dt: datetime) -> int:
    """``datetime`` -> epoch 毫秒；naive 时间按 UTC 解释。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = dt - _EPOCH
    return (
        delta.days * 86_400_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )


def to_iso(ms: int) -> str:
    """epoch 毫秒 -> ISO 8601 UTC 字符串（毫秒精度，``Z`` 后缀）。"""
    dt = ms_to_datetime(ms)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def from_iso(text: str) -> int:
    """ISO 8601 字符串 -> epoch 毫秒（兼容 ``Z`` 与 ``+00:00`` 后缀）。"""
    normalized = text.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"非法 ISO 8601 时间字符串: {text!r}") from exc
    return datetime_to_ms(dt)


def align_minute_ms(ms: int) -> int:
    """将 epoch 毫秒向下对齐到分钟边界（用于 K 线开盘时间归一化）。"""
    return (int(ms) // MS_PER_MINUTE) * MS_PER_MINUTE
