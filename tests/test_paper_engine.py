"""模拟盘引擎测试：滑点/费率对照手算、双模式、边界拒绝、账户视图（禁网）。"""

from __future__ import annotations

import pytest

from quant.config import PaperSettings
from quant.models import SignalRecord
from quant.paper.engine import PaperEngine
from quant.storage.sqlite_store import Storage

FIXED_MS = 1_700_000_000_000
REL = 1e-6


def _engine(
    tmp_path,
    *,
    mode: str = "auto",
    initial: float = 100000.0,
    slippage: float = 5.0,
    position_pct: float = 10.0,
    one_position: bool = True,
    clock=None,
    market_initial: dict | None = None,
) -> tuple[PaperEngine, Storage]:
    settings = PaperSettings()
    settings.mode = mode
    settings.slippage_bps = slippage
    settings.position_pct = position_pct
    settings.one_position_per_symbol = one_position
    settings.initial_cash.crypto = initial
    settings.initial_cash.a_share = initial
    settings.initial_cash.us = initial
    settings.initial_cash.hk = initial
    for market, value in (market_initial or {}).items():
        setattr(settings.initial_cash, market, value)
    store = Storage(tmp_path / "paper.db")
    store.init_schema()
    engine = PaperEngine(settings, store, clock_ms=clock or (lambda: FIXED_MS))
    return engine, store


def _signal(market, symbol, direction, price, *, ts=FIXED_MS, rule="rsi", mode="auto"):
    return SignalRecord(
        id=None, ts_ms=ts, market=market, symbol=symbol, rule=rule,
        direction=direction, mode=mode, status="pending", trigger_value=price,
    )


# ---------------------------------------------------------------------------
# 开仓 / 平仓全链路（对照手算）
# ---------------------------------------------------------------------------


def test_long_then_close_full_cycle(tmp_path):
    engine, store = _engine(tmp_path, mode="auto")

    # 开仓：价 50000，滑点 5bps -> 成交价 50025.0
    engine.on_signal(_signal("crypto", "BTCUSDT", "long", 50000.0))

    fill_price = 50000.0 * (1 + 5 / 10_000)
    assert fill_price == pytest.approx(50025.0)
    qty = pytest.approx(0.1999, rel=REL)
    amount = 0.1999 * fill_price
    fee = amount * 0.001
    total_cost = amount + fee

    assert engine.positions["BTCUSDT"]["qty"] == qty
    assert engine.accounts["crypto"].cash == pytest.approx(100000.0 - total_cost, rel=REL)

    life = store.query_trade_lifecycle("BTCUSDT")
    assert life["signals"][0].status == "ordered"
    assert life["orders"][0].status == "filled"
    assert life["fills"][0].fill_price == pytest.approx(50025.0)
    assert life["fills"][0].fee == pytest.approx(fee, rel=REL)
    assert life["positions"][0].event == "open"

    # 平仓：价 51000，滑点 5bps -> 成交价 50974.5
    engine.on_signal(_signal("crypto", "BTCUSDT", "close", 51000.0))

    close_price = 51000.0 * (1 - 5 / 10_000)
    assert close_price == pytest.approx(50974.5)
    sell_amount = 0.1999 * close_price
    sell_fee = sell_amount * 0.001
    realized = sell_amount - sell_fee - total_cost

    assert "BTCUSDT" not in engine.positions
    assert engine.accounts["crypto"].realized_pnl == pytest.approx(realized, rel=REL)
    # 现金相对初始的净变化应等于已实现盈亏（无其他资金流）
    assert engine.accounts["crypto"].cash - 100000.0 == pytest.approx(realized, rel=REL)

    life = store.query_trade_lifecycle("BTCUSDT")
    assert [p.event for p in life["positions"]] == ["open", "close"]
    assert life["positions"][1].realized_pnl == pytest.approx(realized, rel=REL)


# ---------------------------------------------------------------------------
# 费率表（对照手算）
# ---------------------------------------------------------------------------


def test_fee_schedule_each_market(tmp_path):
    engine, _ = _engine(tmp_path)
    fees = engine.settings.fees
    amount = 10_000.0

    assert engine._fee("crypto", "buy", amount) == pytest.approx(amount * fees.crypto_taker)
    assert engine._fee("a_share", "buy", amount) == pytest.approx(
        amount * (fees.a_share_commission + fees.a_share_transfer)
    )
    # A股卖出多加印花税
    assert engine._fee("a_share", "sell", amount) == pytest.approx(
        amount * (fees.a_share_commission + fees.a_share_transfer + fees.a_share_stamp_tax)
    )
    assert engine._fee("us", "buy", amount) == pytest.approx(0.0)
    # 港股佣金 + 印花税（双边）
    assert engine._fee("hk", "sell", amount) == pytest.approx(
        amount * (fees.hk_commission + fees.hk_stamp_tax)
    )


def test_size_qty_rules(tmp_path):
    engine, _ = _engine(tmp_path)
    # A股按 100 股向下取整：10000 元 / 1680 元 ≈ 5.95 股 -> 0 手整（<100 股 -> 0）
    assert engine._size_qty(10000.0, 1680.0, "a_share") == 0.0
    # 现金充足：1000 万元 * 10% = 100 万 / 1680 -> 595 股 -> 500 股
    assert engine._size_qty(10_000_000.0, 1680.0, "a_share") == 500.0
    # 美港股整数股：10000 元 * 10% = 1000 / 185.5 -> 5 股
    assert engine._size_qty(10_000.0, 185.5, "us") == 5.0
    # 加密六位小数
    assert engine._size_qty(1000.0, 50000.0, "crypto") == pytest.approx(0.002)


# ---------------------------------------------------------------------------
# 双模式
# ---------------------------------------------------------------------------


def test_manual_mode_enqueues_and_confirm(tmp_path):
    engine, store = _engine(tmp_path, mode="manual")
    engine.on_signal(_signal("crypto", "ETHUSDT", "long", 2000.0))

    assert len(engine.pending) == 1
    assert store.query_trade_lifecycle("ETHUSDT")["orders"] == []  # 未下单
    assert engine.pending_view()[0].symbol == "ETHUSDT"

    engine.confirm_head()
    assert len(engine.pending) == 0
    assert engine.positions["ETHUSDT"]["qty"] > 0


def test_manual_mode_ignore(tmp_path):
    engine, store = _engine(tmp_path, mode="manual")
    sig = _signal("crypto", "ETHUSDT", "long", 2000.0)
    engine.on_signal(sig)
    engine.ignore_head()
    assert len(engine.pending) == 0
    assert store.query_signals(symbol="ETHUSDT")[0].status == "ignored"


def test_manual_mode_timeout(tmp_path):
    now = [FIXED_MS]
    engine, store = _engine(tmp_path, mode="manual", clock=lambda: now[0])
    sig = _signal("crypto", "ETHUSDT", "long", 2000.0, ts=FIXED_MS)
    engine.on_signal(sig)
    now[0] += 300_001  # 超过默认 300s
    engine.expire_pending()
    assert len(engine.pending) == 0
    assert store.query_signals(symbol="ETHUSDT")[0].status == "timeout"


def test_toggle_mode(tmp_path):
    engine, _ = _engine(tmp_path, mode="manual")
    assert engine.toggle_mode() == "auto"
    assert engine.toggle_mode() == "manual"


# ---------------------------------------------------------------------------
# 边界拒绝
# ---------------------------------------------------------------------------


def test_close_without_position_is_no_position(tmp_path):
    engine, store = _engine(tmp_path, mode="auto")
    engine.on_signal(_signal("crypto", "BTCUSDT", "close", 50000.0))
    assert store.query_trade_lifecycle("BTCUSDT")["orders"] == []
    assert store.query_signals(symbol="BTCUSDT")[0].status == "no_position"


def test_duplicate_long_is_ignored(tmp_path):
    engine, store = _engine(tmp_path, mode="auto")
    engine.on_signal(_signal("crypto", "BTCUSDT", "long", 50000.0))
    engine.on_signal(_signal("crypto", "BTCUSDT", "long", 50100.0))
    statuses = [s.status for s in store.query_signals(symbol="BTCUSDT")]
    assert statuses == ["ordered", "ignored"]


def test_insufficient_cash_rejected_with_reason(tmp_path):
    # A股账户仅 1000 元，100 股都买不起 -> 拒绝并落原因
    engine, store = _engine(tmp_path, mode="auto", market_initial={"a_share": 1000.0})
    engine.on_signal(_signal("a_share", "600519.SH", "long", 1680.0))
    life = store.query_trade_lifecycle("600519.SH")
    assert len(life["orders"]) == 1
    assert life["orders"][0].status == "rejected"
    assert life["orders"][0].reason
    assert life["signals"][0].status == "rejected"


# ---------------------------------------------------------------------------
# 账户视图
# ---------------------------------------------------------------------------


def test_accounts_view_reflects_position_and_floating_pnl(tmp_path):
    engine, _ = _engine(tmp_path, mode="auto")
    engine.on_signal(_signal("crypto", "BTCUSDT", "long", 50000.0))
    engine.update_price("BTCUSDT", 55000.0)
    view = engine.accounts_view()["crypto"]
    assert view.open_positions == 1
    assert view.position_market_value == pytest.approx(0.1999 * 55000.0, rel=REL)
    assert view.floating_pnl > 0


def test_restart_recovers_account_and_positions(tmp_path):
    engine, store = _engine(tmp_path, mode="auto")
    engine.on_signal(_signal("crypto", "BTCUSDT", "long", 50000.0))
    cash_after = engine.accounts["crypto"].cash
    store.close()

    # 重启：新 Storage + 新引擎应从库恢复账户与持仓
    store2 = Storage(tmp_path / "paper.db")
    store2.init_schema()
    engine2 = PaperEngine(engine.settings, store2)
    assert engine2.accounts["crypto"].cash == pytest.approx(cash_after, rel=REL)
    assert "BTCUSDT" in engine2.positions
