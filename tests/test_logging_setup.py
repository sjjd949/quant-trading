"""日志初始化测试：双通道、统一格式、重复调用安全。"""

from __future__ import annotations

import logging
import re
from contextlib import ExitStack
from pathlib import Path

from rich.console import Console

from quant.utils.logging_setup import (
    DATE_FORMAT,
    LOG_FORMAT,
    RichConsoleHandler,
    setup_logging,
)

#: 统一格式行：``2025-10-24 06:32:05 | INFO     | quant.test | 消息``
_LINE_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \| INFO\s+\| \S+ \| .+$")


def _make_console(tmp_path: Path, stack: ExitStack, name: str = "console.txt") -> Console:
    """创建写入临时文件的 rich Console（由 ExitStack 统一关闭文件句柄）。"""
    handle = stack.enter_context((tmp_path / name).open("w", encoding="utf-8"))
    return Console(file=handle, force_terminal=False)


def _teardown_root_logger() -> None:
    """清空根 logger handler，避免测试间相互影响。"""
    for handler in logging.getLogger().handlers[:]:
        logging.getLogger().removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass


def test_setup_logging_writes_file_and_format(tmp_path: Path) -> None:
    """日志文件落盘且行格式符合统一约定。"""
    with ExitStack() as stack:
        log_file = tmp_path / "logs" / "quant.log"
        path = setup_logging(log_file=log_file, console=_make_console(tmp_path, stack))
        assert path == log_file
        logging.getLogger("quant.test").info("hello 日志")
        logging.getLogger("quant.test").warning("warn 消息")
        for handler in logging.getLogger().handlers:
            handler.flush()
        lines = log_file.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2
        assert _LINE_RE.match(lines[0]), lines[0]
        assert "WARNING" in lines[1]
        assert "hello 日志" in lines[0]
    _teardown_root_logger()


def test_setup_logging_idempotent(tmp_path: Path) -> None:
    """重复调用不叠加 handler（根 logger 仅保留 2 个通道）。"""
    with ExitStack() as stack:
        log_file = tmp_path / "quant.log"
        console = _make_console(tmp_path, stack)
        setup_logging(log_file=log_file, console=console)
        setup_logging(log_file=log_file, console=console)
        handlers = logging.getLogger().handlers
        assert len(handlers) == 2
        assert any(isinstance(h, RichConsoleHandler) for h in handlers)
        assert any(isinstance(h, logging.FileHandler) for h in handlers)
        logging.getLogger("quant.test").info("second call")
        for handler in handlers:
            handler.flush()
        assert len(log_file.read_text(encoding="utf-8").splitlines()) == 1
    _teardown_root_logger()


def test_default_log_path_under_repo_data() -> None:
    """默认日志路径位于仓库 data/logs/quant.log。"""
    from quant.utils.logging_setup import DEFAULT_LOG_FILE

    assert DEFAULT_LOG_FILE.parts[-3:] == ("data", "logs", "quant.log")
    assert (Path(__file__).resolve().parents[1] / "src").is_dir()


def test_format_constants() -> None:
    """格式常量符合共享约定。"""
    assert LOG_FORMAT == "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    assert DATE_FORMAT == "%Y-%m-%d %H:%M:%S"
