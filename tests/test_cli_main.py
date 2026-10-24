"""CLI 入口测试：--check-config / 配置错误退出码 / 启动编排（桩应用，零网络）。"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from quant.cli.main import build_parser, main


def test_build_parser_defaults() -> None:
    """解析器默认值：无 --config、无 --check-config。"""
    args = build_parser().parse_args([])
    assert args.config is None
    assert args.check_config is False


def test_check_config_prints_effective_config(tmp_config_path: Path, capsys: pytest.CaptureFixture) -> None:
    """--check-config 返回 0 并打印生效配置（含 .vision 域名）。"""
    exit_code = main(["--check-config", "--config", str(tmp_config_path)])
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "network.proxy" in out
    assert "data-stream.binance.vision" in out


def test_bad_config_returns_2(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    """配置错误返回 2 并输出中文错误（stderr）。"""
    from tests.conftest import write_config

    config_path = write_config(tmp_path, "[market]\nsymbols = []\n")
    exit_code = main(["--check-config", "--config", str(config_path)])
    assert exit_code == 2
    err = capsys.readouterr().err
    assert "配置错误" in err


def test_main_runs_app_with_stub(tmp_config_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() 无 --check-config 时启动编排器并优雅停止（桩应用零网络）。"""
    started: dict[str, bool] = {}

    class StubApp:
        def __init__(self, settings) -> None:
            self.settings = settings
            self.stop_event = asyncio.Event()

        async def start(self) -> None:
            started["start"] = True
            self.stop_event.set()

        async def stop(self) -> None:
            started["stop"] = True

    monkeypatch.setattr("quant.cli.main.QuantApp", StubApp)
    monkeypatch.setattr(
        "quant.cli.main.setup_logging", lambda **kwargs: tmp_config_path.parent / "quant.log"
    )

    exit_code = main(["--config", str(tmp_config_path)])
    assert exit_code == 0
    assert started == {"start": True, "stop": True}
