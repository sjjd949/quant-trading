"""时间工具测试：固定 epoch ms 的转换与往返一致性。"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant.utils.timeutils import (
    MS_PER_MINUTE,
    align_minute_ms,
    datetime_to_ms,
    from_iso,
    ms_to_datetime,
    now_ms,
    to_iso,
)

#: 固定 epoch ms（2025-10-24T06:32:05.123Z），禁止依赖 now()
FIXED_MS = 1_761_287_525_123


def test_ms_to_datetime_utc() -> None:
    """epoch ms -> UTC datetime 精确到毫秒。"""
    dt = ms_to_datetime(FIXED_MS)
    assert dt == datetime(2025, 10, 24, 6, 32, 5, 123000, tzinfo=timezone.utc)
    assert dt.tzinfo is not None


def test_datetime_to_ms_roundtrip() -> None:
    """datetime -> epoch ms 与固定值一致，且往返一致。"""
    dt = datetime(2025, 10, 24, 6, 32, 5, 123000, tzinfo=timezone.utc)
    assert datetime_to_ms(dt) == FIXED_MS
    assert datetime_to_ms(ms_to_datetime(FIXED_MS)) == FIXED_MS


def test_naive_datetime_treated_as_utc() -> None:
    """naive datetime 按 UTC 解释。"""
    dt = datetime(2025, 10, 24, 6, 32, 5, 123000)
    assert datetime_to_ms(dt) == FIXED_MS


def test_to_iso_format() -> None:
    """to_iso 输出毫秒精度 ISO 8601 UTC（Z 后缀）。"""
    assert to_iso(FIXED_MS) == "2025-10-24T06:32:05.123Z"
    assert to_iso(0) == "1970-01-01T00:00:00.000Z"


def test_from_iso_roundtrip() -> None:
    """from_iso 兼容 Z 与 +00:00 后缀，且与 to_iso 往返一致。"""
    assert from_iso("2025-10-24T06:32:05.123Z") == FIXED_MS
    assert from_iso("2025-10-24T06:32:05.123+00:00") == FIXED_MS
    assert from_iso(to_iso(FIXED_MS)) == FIXED_MS


def test_from_iso_invalid_raises() -> None:
    """非法 ISO 字符串抛出 ValueError。"""
    with pytest.raises(ValueError, match="非法 ISO 8601"):
        from_iso("not-a-time")


def test_align_minute_ms() -> None:
    """向下对齐到分钟边界。"""
    assert align_minute_ms(FIXED_MS) == FIXED_MS - 5_123
    # 同一分钟内的任意毫秒都对齐到该分钟起点（未跨入下一分钟）
    assert align_minute_ms(FIXED_MS + 54_000) == FIXED_MS - 5_123
    assert align_minute_ms(0) == 0
    assert align_minute_ms(MS_PER_MINUTE - 1) == 0
    assert align_minute_ms(MS_PER_MINUTE) == MS_PER_MINUTE


def test_now_ms_is_int() -> None:
    """now_ms 返回 int 量级合理（不用于断言具体值）。"""
    value = now_ms()
    assert isinstance(value, int)
    assert value > 1_600_000_000_000
