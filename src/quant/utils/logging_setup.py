"""日志初始化：rich 控制台 + 文件双通道，统一文本格式。

格式约定：``%(asctime)s | %(levelname)-8s | %(name)s | %(message)s``。
控制台经 rich ``Console`` 输出（不做 markup 渲染，保证格式稳定），
文件落盘 ``data/logs/quant.log``（UTF-8）。重复调用会先清空根
logger 既有 handler，避免交互式/测试场景重复输出。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from rich.console import Console

#: 统一日志格式
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

#: 控制台/文件共用的时间格式（本地时间，仅用于可读性）
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

#: 默认日志文件：<仓库>/data/logs/quant.log
DEFAULT_LOG_FILE = Path(__file__).resolve().parents[3] / "data" / "logs" / "quant.log"


class RichConsoleHandler(logging.Handler):
    """经 rich ``Console`` 输出的日志处理器，保持统一文本格式。

    与 ``rich.logging.RichHandler`` 的区别：后者自行渲染时间/级别，
    无法严格满足管道分隔的统一格式；本处理器只借用 Console 的
    输出能力（颜色/软换行），文本仍由 ``logging.Formatter`` 生成。
    """

    def __init__(self, console: Optional[Console] = None, level: int = logging.NOTSET) -> None:
        super().__init__(level)
        self._console = console if console is not None else Console()

    def emit(self, record: logging.LogRecord) -> None:
        """格式化并打印一条日志；异常委托 ``handleError``。"""
        try:
            message = self.format(record)
            if record.levelno >= logging.ERROR:
                style = "red"
            elif record.levelno >= logging.WARNING:
                style = "yellow"
            else:
                style = None
            self._console.print(
                message, style=style, markup=False, highlight=False, soft_wrap=True
            )
        except RecursionError:
            raise
        except Exception:
            self.handleError(record)


def setup_logging(
    log_file: str | Path | None = None,
    level: int = logging.INFO,
    console: Optional[Console] = None,
) -> Path:
    """初始化根 logger 双通道日志，返回实际日志文件路径。

    参数:
        log_file: 日志文件路径；``None`` 时用 :data:`DEFAULT_LOG_FILE`。
        level: 根 logger 与两个 handler 的级别。
        console: 可注入的 rich ``Console``（测试用）。

    返回:
        实际写入的日志文件绝对/相对路径（已确保父目录存在）。
    """
    root = logging.getLogger()
    root.setLevel(level)

    # 清理既有 handler，保证重复调用不产生重复输出
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:  # pragma: no cover - 防御性清理
            pass

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    console_handler = RichConsoleHandler(console=console, level=level)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    path = Path(log_file) if log_file is not None else DEFAULT_LOG_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    root.addHandler(file_handler)
    return path
