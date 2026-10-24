"""压测子命令测试：摘要统计 / 参数默认值 / 不健康采样（禁网）。"""

from __future__ import annotations

import asyncio
import json

from quant.cli.stress import build_stress_parser, run_stress
from quant.feeds.base import FeedStatus


class FakeAdapter:
    name = "fake"

    def __init__(self, events, connected: bool = True) -> None:
        self._events = list(events)
        self._closed = False
        self._connected = connected
        self._status = FeedStatus(connected=connected, reconnect_count=1, detail="fake")

    async def events(self):
        for event in self._events:
            yield event
        while not self._closed:
            await asyncio.sleep(0.01)
        if False:  # pragma: no cover
            yield None

    async def close(self) -> None:
        self._closed = True

    def status(self) -> FeedStatus:
        return self._status


def test_stress_parser_defaults():
    args = build_stress_parser().parse_args([])
    assert args.market == "a_share"
    assert args.duration == 86400.0
    assert args.interval == 5.0


def test_run_stress_collects_summary_and_writes_output(tmp_path):
    adapter = FakeAdapter([1, 2, 3])
    output = tmp_path / "stress.json"
    summary = asyncio.run(
        run_stress(adapter, duration_sec=0.2, sample_interval=0.05, output=str(output))
    )
    assert summary["events"] >= 3
    assert summary["samples"] >= 1
    assert summary["crashed"] is False
    assert output.is_file()
    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["summary"]["events"] >= 3
    assert len(data["samples"]) >= 1


def test_run_stress_counts_unhealthy_samples():
    adapter = FakeAdapter([], connected=False)
    summary = asyncio.run(run_stress(adapter, duration_sec=0.12, sample_interval=0.05))
    assert summary["unhealthy_samples"] >= 1
