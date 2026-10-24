"""复盘子命令：按 标的/规则/时间 回放信号→订单→成交→持仓生命周期。

用法::

    python -m quant review --symbol BTCUSDT --rule rsi --format table
    python -m quant review --start 2025-10-01T00:00:00 --end 2025-10-31T23:59:59 --format json
    python -m quant review --account --format json

复用 M1 存储查询能力；``--format csv`` 便于导出到表格工具。
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from typing import Optional

from rich.console import Console
from rich.table import Table

from quant.config import ConfigError, Settings, load_settings
from quant.models import SignalRecord
from quant.storage.sqlite_store import Storage
from quant.utils.timeutils import from_iso, to_iso

#: 支持的市场（账户汇总顺序）
_MARKETS = ("crypto", "a_share", "us", "hk")


def build_review_parser() -> argparse.ArgumentParser:
    """构造 review 子命令参数解析器。"""
    parser = argparse.ArgumentParser(
        prog="quant review", description="模拟盘信号复盘（生命周期回放 / 账户汇总）"
    )
    parser.add_argument("--config", default=None, metavar="PATH", help="配置文件路径")
    parser.add_argument("--symbol", default=None, help="按标的过滤（如 BTCUSDT / 600519.SH）")
    parser.add_argument("--rule", default=None, help="按规则过滤（如 price_move）")
    parser.add_argument("--start", default=None, help="起始时间（ISO 8601 或 epoch ms）")
    parser.add_argument("--end", default=None, help="结束时间（ISO 8601 或 epoch ms）")
    parser.add_argument("--limit", type=int, default=None, help="最多返回条数")
    parser.add_argument(
        "--format", choices=["table", "json", "csv"], default="table", help="输出格式"
    )
    parser.add_argument("--account", action="store_true", help="输出各市场账户汇总")
    return parser


def review_main(argv: list[str]) -> int:
    """review 子命令入口，返回进程退出码。"""
    args = build_review_parser().parse_args(argv)
    try:
        settings = load_settings(args.config)
    except ConfigError as exc:
        print(f"配置错误: {exc}", file=sys.stderr)
        return 2
    return run_review(settings, args)


def run_review(
    settings: Settings, args: argparse.Namespace, storage: Optional[Storage] = None
) -> int:
    """执行复盘查询并输出，返回退出码。"""
    own_storage = storage is None
    store = storage if storage is not None else Storage(settings.storage.db_path)
    try:
        if own_storage:
            store.init_schema()
        if args.account:
            summaries = [store.account_summary(market) for market in _MARKETS]
            _emit_accounts(summaries, args.format)
            return 0
        start = _parse_time(args.start)
        end = _parse_time(args.end)
        signals = store.query_signals(
            symbol=args.symbol,
            rule=args.rule,
            start_ms=start,
            end_ms=end,
            limit=args.limit,
        )
        rows = _build_rows(store, signals)
        _emit_rows(rows, args.format)
        return 0
    finally:
        if own_storage:
            store.close()


# ---------------------------------------------------------------------------
# 内部
# ---------------------------------------------------------------------------


def _parse_time(text: Optional[str]) -> Optional[int]:
    """解析时间参数：纯数字视为 epoch ms，否则按 ISO 8601 解析。"""
    if text is None:
        return None
    value = text.strip()
    if not value:
        return None
    if value.isdigit():
        return int(value)
    return from_iso(value)


def _build_rows(store: Storage, signals: list[SignalRecord]) -> list[dict]:
    """把信号与订单/成交/持仓事件串联为复盘行。"""
    by_symbol: dict[str, list[SignalRecord]] = {}
    for signal in signals:
        by_symbol.setdefault(signal.symbol, []).append(signal)

    rows: list[dict] = []
    for symbol, symbol_signals in by_symbol.items():
        life = store.query_trade_lifecycle(symbol)
        orders_by_signal = {
            order.signal_id: order
            for order in life["orders"]
            if order.signal_id is not None
        }
        fills_by_order = {fill.order_id: fill for fill in life["fills"]}
        close_events = [p for p in life["positions"] if p.event == "close"]
        for signal in symbol_signals:
            order = orders_by_signal.get(signal.id)
            fill = fills_by_order.get(order.id) if order is not None else None
            realized = 0.0
            if order is not None and order.side == "sell":
                candidates = [p for p in close_events if p.ts_ms >= signal.ts_ms]
                if candidates:
                    realized = min(candidates, key=lambda p: p.ts_ms).realized_pnl
            rows.append(
                {
                    "ts": to_iso(signal.ts_ms),
                    "symbol": signal.symbol,
                    "rule": signal.rule,
                    "direction": signal.direction,
                    "mode": signal.mode,
                    "status": signal.status,
                    "side": order.side if order is not None else "",
                    "order_status": order.status if order is not None else "",
                    "qty": round(fill.qty, 8) if fill is not None else 0.0,
                    "fill_price": round(fill.fill_price, 6) if fill is not None else 0.0,
                    "fee": round(fill.fee, 8) if fill is not None else 0.0,
                    "realized_pnl": round(realized, 8),
                }
            )
    rows.sort(key=lambda row: row["ts"])
    return rows


def _emit_rows(rows: list[dict], fmt: str) -> None:
    """按格式输出复盘行。"""
    if fmt == "json":
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return
    if fmt == "csv":
        if not rows:
            return
        writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        return

    table = Table(title="信号复盘")
    for column in ("时间", "标的", "规则", "方向", "模式", "状态", "买卖", "订单", "数量", "成交价", "费用", "已实现盈亏"):
        table.add_column(column)
    if not rows:
        table.add_row(*["—"] * 12)
    for row in rows:
        table.add_row(
            row["ts"],
            row["symbol"],
            row["rule"],
            row["direction"],
            row["mode"],
            row["status"],
            row["side"],
            row["order_status"],
            f"{row['qty']:.6f}",
            f"{row['fill_price']:.4f}",
            f"{row['fee']:.4f}",
            f"{row['realized_pnl']:.4f}",
        )
    Console(width=200).print(table)


def _emit_accounts(summaries: list[dict], fmt: str) -> None:
    """输出账户汇总。"""
    if fmt == "json":
        print(json.dumps(summaries, ensure_ascii=False, indent=2))
        return
    if fmt == "csv":
        writer = csv.DictWriter(sys.stdout, fieldnames=list(summaries[0].keys()))
        writer.writeheader()
        writer.writerows(summaries)
        return
    for summary in summaries:
        print(
            f"[{summary['market']}] 现金={summary['cash']:.2f} "
            f"已实现盈亏={summary['realized_pnl']:.2f} "
            f"未平仓={summary['open_positions']} 持仓数量={summary['position_qty']:.6f}"
        )
