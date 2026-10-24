"""跨平台键盘控制器：独立线程读按键 → asyncio.Queue（不阻塞数据流）。

- Windows：``msvcrt``；Unix：``termios`` + ``select``；
- 按键经 ``loop.call_soon_threadsafe`` 投递，由命令消费协程统一处理；
- 读取器可注入（测试用脚本化假读取器，无需真实 TTY）。
"""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
import time
from collections.abc import Callable
from typing import Optional

logger = logging.getLogger("quant.cli.keyboard")

#: 按键 → 命令映射（命令消费协程据此改状态）
_KEY_COMMANDS: dict[str, str] = {
    "p": "pause",
    "r": "rule_panel",
    "q": "quit",
    "y": "confirm_signal",
    "n": "ignore_signal",
    "m": "toggle_mode",
    "1": "toggle_rule:1",
    "2": "toggle_rule:2",
    "3": "toggle_rule:3",
    "4": "toggle_rule:4",
    "5": "toggle_rule:5",
    "6": "toggle_rule:6",
    "7": "toggle_rule:7",
    "8": "toggle_rule:8",
    "9": "toggle_rule:9",
}

#: 读取器类型：返回单字符按键或 ``None``（无按键）
KeyReader = Callable[[], Optional[str]]


class KeyboardController:
    """键盘控制器：独立线程 + 线程安全投递命令。"""

    def __init__(
        self,
        reader: Optional[KeyReader] = None,
        poll_interval_sec: float = 0.05,
    ) -> None:
        """初始化控制器。

        参数:
            reader: 按键读取器（``None`` 时用平台默认实现）。
            poll_interval_sec: 无按键时的轮询间隔（秒）。
        """
        self._reader = reader
        self._poll_interval_sec = poll_interval_sec
        self._stop = threading.Event()

    def run_in_thread(
        self, loop: asyncio.AbstractEventLoop, command_queue: asyncio.Queue
    ) -> threading.Thread:
        """在独立线程中启动键盘监听，返回线程对象（daemon）。"""
        self._stop.clear()
        thread = threading.Thread(
            target=self._run,
            args=(loop, command_queue),
            name="quant-keyboard",
            daemon=True,
        )
        thread.start()
        return thread

    def stop(self) -> None:
        """请求键盘线程停止（幂等）。"""
        self._stop.set()

    def map_key(self, key: str) -> Optional[str]:
        """按键 → 命令；未绑定键返回 ``None``（大小写不敏感）。"""
        return _KEY_COMMANDS.get(key.strip().lower())

    # ------------------------------------------------------------------
    # 内部实现
    # ------------------------------------------------------------------

    def _run(self, loop: asyncio.AbstractEventLoop, command_queue: asyncio.Queue) -> None:
        """线程主体：轮询读取器并投递命令。"""
        reader = self._reader if self._reader is not None else self._default_reader
        try:
            while not self._stop.is_set():
                key = reader()
                if key:
                    self._dispatch(loop, command_queue, key)
                else:
                    time.sleep(self._poll_interval_sec)
        except Exception as exc:  # 线程内异常不得影响主流程
            logger.warning("键盘控制器退出：%s", exc)

    def _dispatch(
        self, loop: asyncio.AbstractEventLoop, command_queue: asyncio.Queue, key: str
    ) -> None:
        """将按键映射为命令并经 ``call_soon_threadsafe`` 投递。"""
        command = self.map_key(key)
        if command is None:
            logger.debug("未绑定按键：%r", key)
            return
        try:
            loop.call_soon_threadsafe(command_queue.put_nowait, command)
        except RuntimeError as exc:  # 事件循环已关闭
            logger.debug("命令投递失败（循环已关闭）：%s", exc)

    def _default_reader(self) -> Optional[str]:
        """平台默认读取器：Windows msvcrt / Unix termios+select。"""
        if sys.platform.startswith("win"):
            return self._read_windows()
        return self._read_unix()

    @staticmethod
    def _read_windows() -> Optional[str]:
        """Windows：``msvcrt.kbhit`` 非阻塞读单键。"""
        try:
            import msvcrt
        except ImportError:  # 非 Windows 平台
            return None
        if msvcrt.kbhit():
            raw = msvcrt.read(1)
            return raw.decode("utf-8", errors="ignore") if isinstance(raw, bytes) else str(raw)
        return None

    @staticmethod
    def _read_unix() -> Optional[str]:
        """Unix：``termios`` 原始模式 + ``select`` 超时读单键。"""
        try:
            import termios
            import tty
        except ImportError:  # 非 Unix 平台
            return None
        try:
            import select

            fd = sys.stdin.fileno()
            ready, _, _ = select.select([sys.stdin], [], [], 0)
            if not ready:
                return None
            old = termios.tcgetattr(fd)
            try:
                tty.setcbreak(fd)
                char = sys.stdin.read(1)
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
            return char or None
        except (termios.error, ValueError, OSError):
            return None
