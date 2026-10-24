"""压测子命令：长时间运行轮询适配器并输出健康摘要（24h 稳定性验用）。

用法::

    python -m quant stress --market a_share --duration 86400 --output data/stress_a_share.json
    python -m quant stress --market hk_us --duration 600

摘要字段：事件数、健康采样数、最大重连次数、不健康采样数、是否崩溃、最终状态。
每 ``interval`` 秒采集一次适配器状态，落 JSON 供事后分析（24h 连续运行无崩溃、
网络瞬断后自动恢复可从中观察 ``reconnects`` 与 ``unhealthy_samples`` 的变化）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Optional

from quant.config import ConfigError, Settings, load_settings
from quant.feeds.eastmoney_poll import EastmoneyPollAdapter
from quant.feeds.tencent_poll import TencentPollAdapter


def build_stress_parser() -> argparse.ArgumentParser:
    """构造 stress 子命令参数解析器。"""
    parser = argparse.ArgumentParser(
        prog="quant stress", description="轮询适配器稳定性压测（默认 24h）"
    )
    parser.add_argument("--config", default=None, metavar="PATH", help="配置文件路径")
    parser.add_argument(
        "--market", choices=["a_share", "hk_us"], default="a_share", help="压测市场"
    )
    parser.add_argument(
        "--duration", type=float, default=86400.0, help="压测时长（秒，默认 86400=24h）"
    )
    parser.add_argument(
        "--interval", type=float, default=5.0, help="健康采样间隔（秒，默认 5）"
    )
    parser.add_argument("--output", default=None, help="健康摘要 JSON 输出路径")
    return parser


async def run_stress(
    adapter,
    duration_sec: float,
    sample_interval: float = 5.0,
    output: Optional[str] = None,
) -> dict:
    """运行压测并返回健康摘要。

    参数:
        adapter: 轮询适配器（需实现 ``events()`` / ``status()`` / ``close()``）。
        duration_sec: 压测时长（秒）。
        sample_interval: 健康采样间隔（秒）。
        output: 摘要（含采样明细）JSON 输出路径；``None`` 不落盘。
    """
    start = time.monotonic()
    events = 0
    samples: list[dict] = []
    max_reconnects = 0
    unhealthy_samples = 0

    async def _consume() -> None:
        nonlocal events
        async for _ in adapter.events():
            events += 1

    task = asyncio.create_task(_consume())
    crashed = False
    try:
        elapsed = 0.0
        while elapsed < duration_sec:
            step = min(sample_interval, duration_sec - elapsed)
            if step <= 0:
                break
            await asyncio.sleep(step)
            elapsed += step
            status = adapter.status()
            max_reconnects = max(max_reconnects, status.reconnect_count)
            if not status.connected:
                unhealthy_samples += 1
            samples.append(
                {
                    "t": round(elapsed, 1),
                    "connected": status.connected,
                    "reconnects": status.reconnect_count,
                    "events": events,
                    "detail": status.detail,
                }
            )
    finally:
        crashed = (
            task.done() and not task.cancelled() and task.exception() is not None
        )
        await adapter.close()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    status = adapter.status()
    summary = {
        "market": getattr(adapter, "name", ""),
        "duration_sec": duration_sec,
        "events": events,
        "samples": len(samples),
        "max_reconnects": max_reconnects,
        "unhealthy_samples": unhealthy_samples,
        "crashed": crashed,
        "final_detail": status.detail,
    }
    if output:
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps({"summary": summary, "samples": samples}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return summary


def stress_main(argv: list[str]) -> int:
    """stress 子命令入口，返回进程退出码。"""
    args = build_stress_parser().parse_args(argv)
    try:
        settings = load_settings(args.config)
    except ConfigError as exc:
        print(f"配置错误: {exc}", file=sys.stderr)
        return 2
    adapter = _build_adapter(settings, args.market)
    summary = asyncio.run(
        run_stress(adapter, args.duration, args.interval, args.output)
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


def _build_adapter(settings: Settings, market: str):
    """按市场构造轮询适配器（真实网络，使用配置代理）。"""
    if market == "hk_us":
        return TencentPollAdapter(
            watchlist_us=list(settings.market.hk_us.watchlist_us),
            watchlist_hk=list(settings.market.hk_us.watchlist_hk),
            poll_interval_sec=settings.market.hk_us.poll_interval_sec,
            proxy=settings.network.proxy,
        )
    return EastmoneyPollAdapter(
        watchlist=list(settings.market.a_share.watchlist),
        poll_interval_sec=settings.market.a_share.poll_interval_sec,
        proxy=settings.network.proxy,
    )
