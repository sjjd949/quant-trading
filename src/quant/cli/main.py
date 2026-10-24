"""CLI 入口：argparse（--config/--check-config）与启动/信号处理。"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from quant.app import QuantApp
from quant.config import ConfigError, format_settings, load_settings
from quant.utils.logging_setup import setup_logging

logger = logging.getLogger("quant.cli.main")


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数解析器。"""
    parser = argparse.ArgumentParser(
        prog="quant",
        description="量化程序：币安实时行情扫描与告警（P0）",
    )
    parser.add_argument(
        "--config",
        default=None,
        metavar="PATH",
        help="配置文件路径（默认 <仓库>/config/config.toml，也可用环境变量 QUANT_CONFIG）",
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="加载并校验配置后打印生效配置（代理脱敏），然后退出",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI 主入口，返回进程退出码。"""
    args = build_parser().parse_args(argv)

    try:
        settings = load_settings(args.config)
    except ConfigError as exc:
        print(f"配置错误: {exc}", file=sys.stderr)
        return 2

    if args.check_config:
        print(format_settings(settings))
        return 0

    log_file = Path(settings.alerts.log_path).resolve().parent / "logs" / "quant.log"
    setup_logging(log_file=log_file)
    logger.info(
        "量化程序启动：币种=%s 代理=%s 冷却=%ss",
        ",".join(settings.market.symbols),
        settings.network.proxy,
        settings.alerts.cooldown_sec,
    )

    app = QuantApp(settings)
    try:
        asyncio.run(_run_app(app))
    except KeyboardInterrupt:
        logger.info("收到键盘中断，正在停止…")
    return 0


async def _run_app(app: QuantApp) -> None:
    """在事件循环中运行应用并处理终止信号（SIGINT/SIGTERM）。"""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, app.stop_event.set)
        except (NotImplementedError, ValueError, AttributeError, OSError):
            # Windows 等平台不支持 add_signal_handler 时退回到 KeyboardInterrupt 路径
            pass
    try:
        await app.start()
    finally:
        await app.stop()


if __name__ == "__main__":
    raise SystemExit(main())
