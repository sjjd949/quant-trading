"""告警管理器测试：冷却去重/独立冷却/JSONL 落盘/CSV、JSON 导出/降级路径。"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from quant.alerts import AlertManager
from quant.models import Alert
from quant.storage import Storage

#: 固定告警时间（UTC）
TS = datetime(2025, 10, 24, 6, 32, 5, 123000, tzinfo=timezone.utc)


class FakeClock:
    """可操控的单调时钟（测试冷却窗口）。"""

    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_alert(
    symbol: str = "BTCUSDT",
    rule: str = "price_move",
    trigger_value: float = 6.0,
    threshold: float = 5.0,
    level: str = "warning",
) -> Alert:
    """构造测试用告警。"""
    return Alert(
        ts=TS,
        symbol=symbol,
        rule=rule,
        message=f"{symbol} 触发 {rule}",
        trigger_value=trigger_value,
        threshold=threshold,
        level=level,
    )


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def manager(tmp_path: Path, clock: FakeClock) -> AlertManager:
    """无存储、冷却 300s 的告警管理器（JSONL 落 tmp_path）。"""
    return AlertManager(
        cooldown_sec=300.0,
        log_path=tmp_path / "data" / "alerts.jsonl",
        clock=clock,
    )


def read_jsonl(path: Path) -> list[dict]:
    """读取 JSONL 全部行。"""
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# 冷却去重
# ---------------------------------------------------------------------------


def test_publish_within_cooldown_dropped(manager: AlertManager, clock: FakeClock) -> None:
    """冷却窗口内重复触发被丢弃（返回 False）。"""
    assert manager.publish(make_alert()) is True
    clock.advance(299.0)
    assert manager.publish(make_alert()) is False
    assert read_jsonl(manager.log_path) == [] or len(read_jsonl(manager.log_path)) == 1


def test_publish_after_cooldown_passes(manager: AlertManager, clock: FakeClock) -> None:
    """冷却窗口结束后再次触发通过（返回 True）。"""
    assert manager.publish(make_alert()) is True
    clock.advance(299.0)
    assert manager.publish(make_alert()) is False
    clock.advance(2.0)  # 累计 301s
    assert manager.publish(make_alert()) is True
    rows = read_jsonl(manager.log_path)
    assert len(rows) == 2


def test_cooldown_independent_per_symbol_and_rule(manager: AlertManager) -> None:
    """冷却按（币种, 规则）独立计时。"""
    assert manager.publish(make_alert(symbol="BTCUSDT", rule="price_move")) is True
    assert manager.publish(make_alert(symbol="ETHUSDT", rule="price_move")) is True
    assert manager.publish(make_alert(symbol="BTCUSDT", rule="volume_spike")) is True
    assert manager.publish(make_alert(symbol="BTCUSDT", rule="price_move")) is False


def test_zero_cooldown_never_drops(tmp_path: Path) -> None:
    """cooldown_sec=0 时不冷却（每次触发都发布）。"""
    manager = AlertManager(cooldown_sec=0.0, log_path=tmp_path / "alerts.jsonl")
    for _ in range(3):
        assert manager.publish(make_alert()) is True
    assert len(read_jsonl(tmp_path / "alerts.jsonl")) == 3


def test_notify_called_only_on_publish(tmp_path: Path, clock: FakeClock) -> None:
    """notify 回调仅在发布成功时调用。"""
    received: list[Alert] = []
    manager = AlertManager(
        cooldown_sec=300.0,
        log_path=tmp_path / "alerts.jsonl",
        notify=received.append,
        clock=clock,
    )
    assert manager.publish(make_alert()) is True
    clock.advance(10.0)
    assert manager.publish(make_alert()) is False
    assert len(received) == 1


# ---------------------------------------------------------------------------
# JSONL 落盘
# ---------------------------------------------------------------------------


def test_jsonl_content_and_count(manager: AlertManager, clock: FakeClock) -> None:
    """JSONL 内容与行数：字段完整、ts 为 ISO 8601 UTC。"""
    manager.publish(make_alert(symbol="BTCUSDT", rule="price_move"))
    clock.advance(301.0)
    manager.publish(make_alert(symbol="ETHUSDT", rule="rsi", level="info"))
    rows = read_jsonl(manager.log_path)
    assert len(rows) == 2
    first = rows[0]
    assert first["ts"] == "2025-10-24T06:32:05.123Z"
    assert first["symbol"] == "BTCUSDT"
    assert first["rule"] == "price_move"
    assert first["message"] == "BTCUSDT 触发 price_move"
    assert first["trigger_value"] == 6.0
    assert first["threshold"] == 5.0
    assert first["level"] == "warning"
    assert rows[1]["level"] == "info"


def test_jsonl_write_failure_degrades(tmp_path: Path) -> None:
    """JSONL 写入失败降级：publish 仍返回 True 且不抛异常。"""
    # log_path 指向目录 → open() 失败（OSError）
    bad_dir = tmp_path / "alerts.jsonl"
    bad_dir.mkdir()
    manager = AlertManager(cooldown_sec=300.0, log_path=bad_dir)
    assert manager.publish(make_alert()) is True


def test_store_failure_degrades(tmp_path: Path) -> None:
    """SQLite 写入失败降级：publish 仍返回 True。"""

    class BrokenStore:
        def save_alert(self, alert: Alert) -> None:
            raise RuntimeError("模拟入库故障")

        def query_alerts(self, **kwargs):
            raise RuntimeError("模拟查询故障")

    manager = AlertManager(
        cooldown_sec=300.0,
        log_path=tmp_path / "alerts.jsonl",
        store=BrokenStore(),
    )
    assert manager.publish(make_alert()) is True
    assert len(read_jsonl(tmp_path / "alerts.jsonl")) == 1


# ---------------------------------------------------------------------------
# 导出
# ---------------------------------------------------------------------------


@pytest.fixture()
def manager_with_store(tmp_path: Path, clock: FakeClock) -> AlertManager:
    """带 SQLite 存储的告警管理器。"""
    store = Storage(tmp_path / "data" / "quant.db")
    store.init_schema()
    manager = AlertManager(
        cooldown_sec=300.0,
        log_path=tmp_path / "data" / "alerts.jsonl",
        store=store,
        clock=clock,
    )
    manager.publish(make_alert(symbol="BTCUSDT", rule="price_move"))
    clock.advance(301.0)
    manager.publish(make_alert(symbol="ETHUSDT", rule="rsi", level="info"))
    return manager


def test_export_csv_roundtrip(manager_with_store: AlertManager, tmp_path: Path) -> None:
    """CSV 导出：表头 + 行数 + 字段值往返一致。"""
    target = tmp_path / "out" / "alerts.csv"
    count = manager_with_store.export_csv(target)
    assert count == 2
    with target.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["rule"] == "price_move"
    assert rows[0]["ts"] == "2025-10-24T06:32:05.123Z"
    assert float(rows[0]["trigger_value"]) == 6.0
    assert rows[1]["level"] == "info"


def test_export_json_roundtrip(manager_with_store: AlertManager, tmp_path: Path) -> None:
    """JSON 导出：条数 + 字段往返一致。"""
    target = tmp_path / "out" / "alerts.json"
    count = manager_with_store.export_json(target)
    assert count == 2
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert isinstance(payload, list) and len(payload) == 2
    assert payload[0]["rule"] == "price_move"
    assert payload[1]["symbol"] == "ETHUSDT"


def test_export_falls_back_to_jsonl(tmp_path: Path, clock: FakeClock) -> None:
    """无存储时导出回退解析 JSONL。"""
    manager = AlertManager(cooldown_sec=300.0, log_path=tmp_path / "alerts.jsonl", clock=clock)
    manager.publish(make_alert())
    clock.advance(301.0)
    manager.publish(make_alert(symbol="ETHUSDT"))
    assert manager.export_csv(tmp_path / "out.csv") == 2
    assert manager.export_json(tmp_path / "out.json") == 2


def test_export_empty(manager: AlertManager, tmp_path: Path) -> None:
    """无告警时导出 0 条。"""
    assert manager.export_csv(tmp_path / "empty.csv") == 0
    assert manager.export_json(tmp_path / "empty.json") == 0
    assert json.loads((tmp_path / "empty.json").read_text(encoding="utf-8")) == []


def test_export_ignores_bad_jsonl_lines(tmp_path: Path) -> None:
    """JSONL 坏行在导出时被跳过。"""
    log_path = tmp_path / "alerts.jsonl"
    log_path.write_text(
        json.dumps(
            {
                "ts": "2025-10-24T06:32:05.123Z",
                "symbol": "BTCUSDT",
                "rule": "price_move",
                "message": "x",
                "trigger_value": 6.0,
                "threshold": 5.0,
                "level": "warning",
            },
            ensure_ascii=False,
        )
        + "\n坏行\n",
        encoding="utf-8",
    )
    manager = AlertManager(cooldown_sec=300.0, log_path=log_path)
    assert manager.export_json(tmp_path / "out.json") == 1
