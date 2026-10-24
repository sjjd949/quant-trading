"""SQLite 存储层测试：schema/CRUD/upsert 幂等/查询过滤/时间边界。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from quant.models import Alert, Candle
from quant.storage import Storage
from quant.utils.timeutils import datetime_to_ms, to_iso

#: 固定基准时间（UTC），禁止依赖 now()
BASE_MS = 1_761_287_525_123  # 2025-10-24T06:32:05.123Z


def _base_dt() -> datetime:
    return datetime(2025, 10, 24, 6, 32, 5, 123000, tzinfo=timezone.utc)


def _candle(open_time_ms: int, close: float = 100.0, is_closed: bool = True) -> Candle:
    """构造测试用 1min K 线（固定时间、显式字段）。"""
    return Candle(
        symbol="BTCUSDT",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 59_999,
        open=close - 1.0,
        high=close + 1.0,
        low=close - 2.0,
        close=close,
        volume=10.0,
        quote_volume=close * 10.0,
        trades=5,
        is_closed=is_closed,
    )


def _alert(
    rule: str = "price_move",
    symbol: str = "BTCUSDT",
    offset_ms: int = 0,
    level: str = "info",
) -> Alert:
    """构造测试用告警（固定 UTC 时间）。"""
    return Alert(
        ts=datetime(2025, 10, 24, 6, 32, 5, 123000, tzinfo=timezone.utc)
        + timedelta(milliseconds=offset_ms),
        symbol=symbol,
        rule=rule,
        message=f"{symbol} 触发 {rule}",
        trigger_value=5.2,
        threshold=5.0,
        level=level,
    )


@pytest.fixture()
def store(tmp_path) -> Storage:
    """已建表的临时数据库。"""
    storage = Storage(tmp_path / "data" / "quant.db")
    storage.init_schema()
    yield storage
    storage.close()


# ---------------------------------------------------------------------------
# schema 与连接
# ---------------------------------------------------------------------------


def test_init_schema_creates_two_tables(store: Storage) -> None:
    """建表后存在 alerts 与 candles 两表及索引。"""
    tables = {
        row[0]
        for row in store.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert {"alerts", "candles"} <= tables


def test_init_schema_idempotent(store: Storage) -> None:
    """init_schema 可重复调用。"""
    store.init_schema()
    store.init_schema()


def test_context_manager_creates_db(tmp_path) -> None:
    """上下文管理器：进入即建表，退出即关闭。"""
    db_path = tmp_path / "ctx" / "quant.db"
    with Storage(db_path) as storage:
        tables = {
            row[0]
            for row in storage.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {"alerts", "candles"} <= tables
    with pytest.raises(Exception):
        storage.conn.execute("SELECT 1")


# ---------------------------------------------------------------------------
# 告警 CRUD 与过滤
# ---------------------------------------------------------------------------


def test_save_and_query_all_alerts(store: Storage) -> None:
    """告警写入后可按时间升序查出，字段无损往返。"""
    store.save_alert(_alert(rule="price_move", offset_ms=0))
    store.save_alert(_alert(rule="rsi", offset_ms=1_000))
    alerts = store.query_alerts()
    assert len(alerts) == 2
    assert [a.rule for a in alerts] == ["price_move", "rsi"]
    first = alerts[0]
    assert first.symbol == "BTCUSDT"
    assert first.trigger_value == pytest.approx(5.2)
    assert first.threshold == pytest.approx(5.0)
    assert first.message == "BTCUSDT 触发 price_move"
    assert datetime_to_ms(first.ts) == BASE_MS


def test_query_alerts_filter_by_symbol(store: Storage) -> None:
    """按币种过滤。"""
    store.save_alert(_alert(symbol="BTCUSDT"))
    store.save_alert(_alert(symbol="ETHUSDT"))
    alerts = store.query_alerts(symbol="ETHUSDT")
    assert len(alerts) == 1
    assert alerts[0].symbol == "ETHUSDT"


def test_query_alerts_filter_by_rule(store: Storage) -> None:
    """按规则名过滤。"""
    store.save_alert(_alert(rule="price_move"))
    store.save_alert(_alert(rule="volume_spike"))
    store.save_alert(_alert(rule="volume_spike"))
    alerts = store.query_alerts(rule="volume_spike")
    assert len(alerts) == 2


def test_query_alerts_time_range_inclusive(store: Storage) -> None:
    """时间范围为闭区间（含端点）。"""
    base = _base_dt()
    store.save_alert(_alert(offset_ms=-60_000))  # 06:31:05.123
    store.save_alert(_alert(offset_ms=0))        # 06:32:05.123
    store.save_alert(_alert(offset_ms=60_000))   # 06:33:05.123
    alerts = store.query_alerts(start=base, end=base + timedelta(seconds=30))
    assert len(alerts) == 1
    assert datetime_to_ms(alerts[0].ts) == BASE_MS

    # 仅 start：返回之后所有
    alerts = store.query_alerts(start=base + timedelta(seconds=30))
    assert len(alerts) == 1

    # 仅 end：返回之前所有
    alerts = store.query_alerts(end=base - timedelta(seconds=30))
    assert len(alerts) == 1


def test_query_alerts_naive_datetime_as_utc(store: Storage) -> None:
    """naive datetime 过滤按 UTC 解释。"""
    store.save_alert(_alert())
    alerts = store.query_alerts(
        start=datetime(2025, 10, 24, 0, 0, 0), end=datetime(2025, 10, 25, 0, 0, 0)
    )
    assert len(alerts) == 1


def test_query_alerts_empty_db(store: Storage) -> None:
    """空库查询返回空列表。"""
    assert store.query_alerts() == []


def test_query_alerts_no_match(store: Storage) -> None:
    """无匹配过滤条件返回空列表。"""
    store.save_alert(_alert(symbol="BTCUSDT"))
    assert store.query_alerts(symbol="ETHUSDT") == []


# ---------------------------------------------------------------------------
# K 线 upsert 与加载
# ---------------------------------------------------------------------------


def test_save_candles_upsert_idempotent(store: Storage) -> None:
    """重复 save_candles 同一 (symbol, open_time_ms) 不产生重复行。"""
    minute = 60_000
    base_open = 1_761_287_200_000  # 2025-10-24T06:20:00.000Z
    candles = [_candle(base_open + i * minute) for i in range(3)]
    store.save_candles("BTCUSDT", candles)
    store.save_candles("BTCUSDT", candles)
    count = store.conn.execute(
        "SELECT COUNT(*) FROM candles WHERE symbol = 'BTCUSDT'"
    ).fetchone()[0]
    assert count == 3

    # upsert 更新：收盘价与收盘标记被覆盖
    updated = [_candle(base_open, close=999.0, is_closed=True)]
    store.save_candles("BTCUSDT", updated)
    loaded = store.load_candles("BTCUSDT", limit=10)
    assert len(loaded) == 3
    assert loaded[0].close == pytest.approx(999.0)


def test_load_candles_order_and_limit(store: Storage) -> None:
    """加载最近 limit 根且按开盘时间升序。"""
    minute = 60_000
    base_open = 1_761_287_200_000
    candles = [_candle(base_open + i * minute, close=100.0 + i) for i in range(10)]
    store.save_candles("BTCUSDT", candles)
    loaded = store.load_candles("BTCUSDT", limit=4)
    assert len(loaded) == 4
    assert [c.close for c in loaded] == [106.0, 107.0, 108.0, 109.0]
    opens = [c.open_time_ms for c in loaded]
    assert opens == sorted(opens)


def test_load_candles_only_closed_default(store: Storage) -> None:
    """默认仅加载已收盘 K 线；only_closed=False 时包含未收盘。"""
    minute = 60_000
    base_open = 1_761_287_200_000
    store.save_candles(
        "BTCUSDT",
        [
            _candle(base_open, close=1.0, is_closed=True),
            _candle(base_open + minute, close=2.0, is_closed=False),
        ],
    )
    closed_only = store.load_candles("BTCUSDT", limit=10)
    assert len(closed_only) == 1
    assert closed_only[0].close == pytest.approx(1.0)

    all_candles = store.load_candles("BTCUSDT", limit=10, only_closed=False)
    assert len(all_candles) == 2
    assert all_candles[1].is_closed is False


def test_load_candles_symbol_isolation(store: Storage) -> None:
    """多币种数据互不干扰。"""
    minute = 60_000
    base_open = 1_761_287_200_000
    store.save_candles("BTCUSDT", [_candle(base_open)])
    eth = _candle(base_open)
    eth.symbol = "ETHUSDT"
    store.save_candles("ETHUSDT", [eth])
    assert len(store.load_candles("BTCUSDT", limit=10)) == 1
    assert len(store.load_candles("ETHUSDT", limit=10)) == 1
    assert store.load_candles("SOLUSDT", limit=10) == []


def test_save_candles_empty_list_noop(store: Storage) -> None:
    """空列表为无操作。"""
    store.save_candles("BTCUSDT", [])
    assert store.load_candles("BTCUSDT", limit=10) == []


def test_load_candles_invalid_limit(store: Storage) -> None:
    """非法 limit 抛 ValueError。"""
    with pytest.raises(ValueError, match="limit 必须为正整数"):
        store.load_candles("BTCUSDT", limit=0)


def test_save_candles_uses_symbol_param(store: Storage) -> None:
    """save_candles 的 symbol 参数覆盖 Candle.symbol（防御性归一）。"""
    candle = _candle(1_761_287_200_000)
    assert candle.symbol == "BTCUSDT"
    store.save_candles("ETHUSDT", [candle])
    loaded = store.load_candles("ETHUSDT", limit=10)
    assert len(loaded) == 1
    assert loaded[0].symbol == "ETHUSDT"


def test_alert_ts_iso_roundtrip(store: Storage) -> None:
    """告警时间经 to_iso 展示层格式无损。"""
    store.save_alert(_alert())
    alerts = store.query_alerts()
    assert to_iso(datetime_to_ms(alerts[0].ts)) == "2025-10-24T06:32:05.123Z"
