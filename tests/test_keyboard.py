"""键盘控制器测试：按键映射 / 线程安全投递 / 停止（无需真实 TTY）。"""

from __future__ import annotations

import asyncio
import threading

import pytest

from quant.cli.keyboard import KeyboardController


def test_map_key_bindings() -> None:
    """p/r/q/y/n/m/1-9 映射到约定命令（大小写不敏感）。"""
    controller = KeyboardController()
    assert controller.map_key("p") == "pause"
    assert controller.map_key("P") == "pause"
    assert controller.map_key("r") == "rule_panel"
    assert controller.map_key("q") == "quit"
    assert controller.map_key("1") == "toggle_rule:1"
    assert controller.map_key("4") == "toggle_rule:4"
    # M2 新增：y 确认 / n 忽略 / m 切模式；规则序号泛化到 1-9
    assert controller.map_key("y") == "confirm_signal"
    assert controller.map_key("n") == "ignore_signal"
    assert controller.map_key("m") == "toggle_mode"
    assert controller.map_key("9") == "toggle_rule:9"


def test_map_key_unbound() -> None:
    """未绑定键返回 None。"""
    controller = KeyboardController()
    assert controller.map_key("x") is None
    assert controller.map_key(" ") is None
    assert controller.map_key("0") is None


def test_run_in_thread_dispatches_commands() -> None:
    """脚本化读取器：按键经 call_soon_threadsafe 有序投递到队列。"""

    async def _run() -> list[str]:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[str] = asyncio.Queue()
        keys = iter(["p", "1", "q", "x"])
        controller = KeyboardController(
            reader=lambda: next(keys, None), poll_interval_sec=0.01
        )
        thread = controller.run_in_thread(loop, queue)
        commands = [await asyncio.wait_for(queue.get(), timeout=2.0) for _ in range(3)]
        controller.stop()
        thread.join(timeout=2.0)
        assert not thread.is_alive()
        return commands

    commands = asyncio.run(_run())
    assert commands == ["pause", "toggle_rule:1", "quit"]


def test_stop_ends_thread() -> None:
    """stop() 能结束轮询线程。"""

    async def _run() -> bool:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[str] = asyncio.Queue()
        controller = KeyboardController(reader=lambda: None, poll_interval_sec=0.01)
        thread = controller.run_in_thread(loop, queue)
        await asyncio.sleep(0.05)
        alive_before = thread.is_alive()
        controller.stop()
        thread.join(timeout=2.0)
        return alive_before and not thread.is_alive()

    assert asyncio.run(_run()) is True


def test_thread_is_daemon() -> None:
    """键盘线程为 daemon（不阻塞进程退出）。"""

    async def _run() -> bool:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[str] = asyncio.Queue()
        controller = KeyboardController(reader=lambda: None, poll_interval_sec=0.01)
        thread = controller.run_in_thread(loop, queue)
        daemon = thread.daemon
        controller.stop()
        thread.join(timeout=2.0)
        return daemon

    assert asyncio.run(_run()) is True


def test_default_reader_windows_no_key() -> None:
    """Windows 默认读取器在无按键时返回 None（不阻塞）。"""
    controller = KeyboardController()
    import sys

    if not sys.platform.startswith("win"):
        pytest.skip("Windows 专用路径")
    assert controller._read_windows() is None
    assert controller._default_reader() is None  # 平台分发到 Windows 读取器


def test_read_unix_returns_none_on_windows() -> None:
    """Windows 上调用 Unix 读取器：termios 缺失时返回 None（不抛异常）。"""
    import sys

    if sys.platform.startswith("win"):
        assert KeyboardController._read_unix() is None
    else:
        pytest.skip("Windows 专用路径")


def test_read_windows_decodes_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Windows 读取器命中按键时解码为字符（msvcrt 打桩）。"""
    import sys
    import types

    if not sys.platform.startswith("win"):
        pytest.skip("Windows 专用路径")
    fake_msvcrt = types.SimpleNamespace(kbhit=lambda: True, read=lambda n: b"p")
    monkeypatch.setitem(sys.modules, "msvcrt", fake_msvcrt)
    assert KeyboardController._read_windows() == "p"


def test_reader_exception_ends_thread_quietly() -> None:
    """读取器抛异常时线程记录警告后安静退出（不影响主流程）。"""

    def boom() -> str:
        raise RuntimeError("模拟读取器故障")

    async def _run() -> bool:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[str] = asyncio.Queue()
        controller = KeyboardController(reader=boom, poll_interval_sec=0.01)
        thread = controller.run_in_thread(loop, queue)
        thread.join(timeout=2.0)
        return not thread.is_alive()

    assert asyncio.run(_run()) is True


def test_dispatch_to_closed_loop_does_not_raise() -> None:
    """事件循环已关闭时投递失败被吞掉（不抛异常）。"""
    loop = asyncio.new_event_loop()
    loop.close()
    queue: asyncio.Queue[str] = asyncio.Queue()
    controller = KeyboardController()
    controller._dispatch(loop, queue, "p")  # 不抛异常即通过
