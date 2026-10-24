"""复盘子命令测试：生命周期回放 / 账户汇总 / 导出格式（禁网）。"""

from __future__ import annotations

import json

from quant.cli.review import build_review_parser, run_review
from quant.config import Settings
from quant.models import SignalRecord
from quant.paper.engine import PaperEngine
from quant.storage.sqlite_store import Storage

BASE_MS = 1_700_000_000_000


def _settings(tmp_path) -> Settings:
    settings = Settings()
    settings.network.proxy = "http://127.0.0.1:1"
    settings.storage.db_path = str(tmp_path / "q.db")
    settings.paper.mode = "auto"
    return settings


def _signal(market, symbol, direction, price, ts) -> SignalRecord:
    return SignalRecord(
        id=None, ts_ms=ts, market=market, symbol=symbol, rule="price_move",
        direction=direction, mode="auto", status="pending", trigger_value=price,
    )


def _populate(tmp_path) -> Settings:
    settings = _settings(tmp_path)
    store = Storage(settings.storage.db_path)
    store.init_schema()
    engine = PaperEngine(settings.paper, store)
    engine.on_signal(_signal("crypto", "BTCUSDT", "long", 50000.0, BASE_MS))
    engine.on_signal(_signal("crypto", "BTCUSDT", "close", 51000.0, BASE_MS + 60_000))
    store.close()
    return settings


def test_review_json_lifecycle(tmp_path, capsys):
    settings = _populate(tmp_path)
    args = build_review_parser().parse_args(["--format", "json", "--symbol", "BTCUSDT"])
    assert run_review(settings, args) == 0
    rows = json.loads(capsys.readouterr().out)
    assert len(rows) == 2
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["side"] == "buy"
    assert rows[0]["order_status"] == "filled"
    assert rows[1]["side"] == "sell"
    assert rows[1]["realized_pnl"] != 0.0


def test_review_table_default(tmp_path, capsys):
    settings = _populate(tmp_path)
    args = build_review_parser().parse_args([])
    assert run_review(settings, args) == 0
    out = capsys.readouterr().out
    assert "信号复盘" in out
    assert "BTCUSDT" in out


def test_review_csv(tmp_path, capsys):
    settings = _populate(tmp_path)
    args = build_review_parser().parse_args(["--format", "csv"])
    assert run_review(settings, args) == 0
    out = capsys.readouterr().out
    assert "symbol" in out
    assert "BTCUSDT" in out


def test_review_account_json(tmp_path, capsys):
    settings = _populate(tmp_path)
    args = build_review_parser().parse_args(["--account", "--format", "json"])
    assert run_review(settings, args) == 0
    data = json.loads(capsys.readouterr().out)
    crypto = [item for item in data if item["market"] == "crypto"][0]
    assert crypto["cash"] > 0


def test_review_rule_filter_returns_empty(tmp_path, capsys):
    settings = _populate(tmp_path)
    args = build_review_parser().parse_args(["--format", "json", "--rule", "nope"])
    assert run_review(settings, args) == 0
    assert json.loads(capsys.readouterr().out) == []


def test_main_routes_to_review(tmp_path, capsys, monkeypatch):
    settings = _populate(tmp_path)
    monkeypatch.setattr("quant.cli.review.load_settings", lambda path=None: settings)
    from quant.cli.main import main

    code = main(["review", "--format", "json", "--symbol", "BTCUSDT"])
    assert code == 0
    assert "BTCUSDT" in capsys.readouterr().out
