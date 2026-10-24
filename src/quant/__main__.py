"""``python -m quant`` 入口。

T01 阶段提供 ``--check-config``：加载并校验配置，打印生效配置（代理脱敏）。
T05 起完整 CLI（看板/键盘交互）由 ``quant.cli.main`` 提供，本文件仅做委托。
"""

from __future__ import annotations

import argparse
import sys

from quant.config import ConfigError, format_settings, load_settings


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
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        settings = load_settings(args.config)
    except ConfigError as exc:
        print(f"配置错误: {exc}", file=sys.stderr)
        return 2

    if args.check_config:
        print(format_settings(settings))
        return 0

    print("完整 CLI 看板正在后续任务交付；当前可先使用 --check-config 校验配置。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
