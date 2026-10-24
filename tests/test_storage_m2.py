"""M2 存储层测试：signals/orders/fills/positions/accounts/positions_current 六表 CRUD + 查询 + 重启恢复。"""

from __future__ import annotations

from pathlib import Path

from quant.models import Account, Fill, Order, PositionEvent, SignalRecord
from quant.storage.sqlite_store import Storage


def _make_signal(
    symbol: str = "BTCUSDT", status: str = "ordered", rule: str = "price_move"
) -> SignalRecord:
    return SignalRecord(
        id=None,
        ts_ms=1_700_000_000_000,
        market="crypto",
        symbol=symbol,
        rule=rule,
        direction="long",
        mode="auto",
        status=status,
        trigger_value=68000.0,
    )


def test_m2_schema_created(tmp_path: Path) -> None:
    """init_schema 创建全部 M2 新表。"""
    storage = Storage(tmp_path / "m2.db")
    storage.init_schema()
    tables = {
        row[0]
        for row in storage.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    for name in (
        "signals",
        "orders",
        "fills",
        "positions",
        "accounts",
        "positions_current",
    ):
        assert name in tables


def test_save_signal_sets_id_and_order_link(tmp_path: Path) -> None:
    """save_signal 回填 id，link_signal_order 更新 order_id。"""
    storage = Storage(tmp_path / "m2.db")
    storage.init_schema()
    sig = _make_signal()
    sid = storage.save_signal(sig)
    assert sig.id == sid
    order = Order(
        id=None, ts_ms=1, signal_id=sig.id, symbol="BTCUSDT", side="buy",
        qty=0.1, ref_price=68000.0, status="filled",
    )
    oid = storage.save_order(order)
    storage.link_signal_order(sig.id, oid)
    reloaded = storage.query_signals(symbol="BTCUSDT")[0]
    assert reloaded.id == sig.id
    assert reloaded.order_id == oid


def test_query_signals_filters(tmp_path: Path) -> None:
    """按 标的/规则/时间范围/limit 过滤。"""
    storage = Storage(tmp_path / "m2.db")
    storage.init_schema()
    for i, sym in enumerate(["BTCUSDT", "ETHUSDT", "BTCUSDT"]):
        s = _make_signal(symbol=sym, status="ordered")
        s.ts_ms = 1_700_000_000_000 + i * 60_000
        storage.save_signal(s)
        if i == 1:
            s2 = _make_signal(symbol=sym, rule="rsi", status="ordered")
            s2.ts_ms = s.ts_ms
            storage.save_signal(s2)

    # 按标的过滤
    assert len(storage.query_signals(symbol="BTCUSDT")) == 2
    # 按规则过滤
    assert len(storage.query_signals(rule="rsi")) == 1
    assert len(storage.query_signals(symbol="ETHUSDT", rule="rsi")) == 1
    # limit
    assert len(storage.query_signals(limit=1)) == 1
    # 时间范围
    start = 1_700_000_000_000 + 60_000
    assert len(storage.query_signals(start_ms=start)) == 3


def test_account_and_position_current_upsert(tmp_path: Path) -> None:
    """账户与当前持仓 upsert，平仓（qty<=0）删除行。"""
    storage = Storage(tmp_path / "m2.db")
    storage.init_schema()
    storage.save_account(Account(market="crypto", cash=90000.0, realized_pnl=10.0))
    storage.save_account(Account(market="crypto", cash=80000.0, realized_pnl=20.0))
    acc = storage.get_account("crypto")
    assert acc is not None and acc.cash == 80000.0 and acc.realized_pnl == 20.0
    assert storage.get_account("a_share") is None

    storage.update_position_current("BTCUSDT", "crypto", 0.5, 68000.0, 1_700_000_000_000)
    storage.update_position_current("ETHUSDT", "crypto", 2.0, 2400.0, 1_700_000_000_000)
    positions = storage.get_positions_current()
    assert len(positions) == 2
    # 平仓 BTCUSDT：qty<=0 删除
    storage.update_position_current("BTCUSDT", "crypto", 0.0, 0.0, 0)
    assert len(storage.get_positions_current()) == 1


def test_query_trade_lifecycle(tmp_path: Path) -> None:
    """一笔交易全生命周期可串联回放。"""
    storage = Storage(tmp_path / "m2.db")
    storage.init_schema()
    sig = _make_signal(symbol="BTCUSDT")
    storage.save_signal(sig)
    order = Order(
        id=None, ts_ms=1, signal_id=sig.id, symbol="BTCUSDT", side="buy",
        qty=0.1, ref_price=68000.0, status="filled",
    )
    storage.save_order(order)
    storage.link_signal_order(sig.id, order.id)
    fill = Fill(
        id=None, order_id=order.id, ts_ms=1, symbol="BTCUSDT", qty=0.1,
        fill_price=68034.0, fee=6.8, amount=6803.4,
    )
    storage.save_fill(fill)
    storage.save_position(
        PositionEvent(id=None, ts_ms=1, symbol="BTCUSDT", event="open",
                     qty=0.1, price=68034.0)
    )

    lifecycle = storage.query_trade_lifecycle("BTCUSDT")
    assert len(lifecycle["signals"]) == 1
    assert len(lifecycle["orders"]) == 1
    assert len(lifecycle["fills"]) == 1
    assert len(lifecycle["positions"]) == 1
    assert lifecycle["orders"][0].signal_id == sig.id
    assert lifecycle["fills"][0].order_id == order.id


def test_account_summary(tmp_path: Path) -> None:
    """账户汇总：现金/已实现盈亏/未平仓数/持仓数量。"""
    storage = Storage(tmp_path / "m2.db")
    storage.init_schema()
    storage.save_account(Account(market="crypto", cash=80000.0, realized_pnl=123.0))
    storage.update_position_current("BTCUSDT", "crypto", 0.5, 68000.0, 1)
    storage.update_position_current("ETHUSDT", "crypto", 2.0, 2400.0, 1)

    summary = storage.account_summary("crypto")
    assert summary["cash"] == 80000.0
    assert summary["realized_pnl"] == 123.0
    assert summary["open_positions"] == 2
    assert summary["position_qty"] == 2.5


def test_restart_recovery(tmp_path: Path) -> None:
    """进程重启后账户与持仓当前态可恢复。"""
    db_path = tmp_path / "m2.db"
    storage = Storage(db_path)
    storage.init_schema()
    storage.save_account(Account(market="crypto", cash=77777.0, realized_pnl=99.0))
    storage.update_position_current("BTCUSDT", "crypto", 0.3, 65000.0, 1_700_000_000_000)
    storage.close()

    reopened = Storage(db_path)
    reopened.init_schema()  # 幂等
    acc = reopened.get_account("crypto")
    assert acc is not None and acc.cash == 77777.0 and acc.realized_pnl == 99.0
    positions = reopened.get_positions_current()
    assert len(positions) == 1
    assert positions[0]["symbol"] == "BTCUSDT"
    assert positions[0]["qty"] == 0.3
    reopened.close()
