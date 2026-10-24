"""配置层测试：加载/校验/环境变量覆盖/路径解析/.env/脱敏展示。"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from quant.__main__ import main
from quant.config import (
    DEFAULT_CONFIG_PATH,
    ConfigError,
    format_settings,
    load_settings,
    mask_proxy,
)
from tests.conftest import REPO_ROOT, write_config

#: 需在测试中隔离的环境变量键
_ENV_KEYS = ("QUANT_CONFIG", "QUANT_PROXY", "QUANT_SYMBOLS", "QUANT_LOG_PATH", "QUANT_DB_PATH")


@pytest.fixture(autouse=True)
def _clean_quant_env() -> None:
    """每个用例前清空 QUANT_* 环境变量，用例后恢复原值。

    采用快照-恢复（而非 ``monkeypatch.delenv``）：``_load_dotenv`` 会把
    .env 键写入 ``os.environ``（生产语义），若仅用 delenv，teardown 会把
    用例中泄漏的值恢复并传播给后续测试文件。
    """
    saved = {key: os.environ.get(key) for key in _ENV_KEYS}
    for key in _ENV_KEYS:
        os.environ.pop(key, None)
    yield
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


# ---------------------------------------------------------------------------
# 正常加载
# ---------------------------------------------------------------------------


def test_load_minimal_config(tmp_settings) -> None:
    """最小配置可加载，关键字段与 TOML 一致。"""
    assert tmp_settings.network.proxy == "http://127.0.0.1:63571"
    assert tmp_settings.network.ws_url == "wss://data-stream.binance.vision/stream"
    assert tmp_settings.network.rest_url == "https://data-api.binance.vision"
    assert tmp_settings.market.symbols == ["BTCUSDT", "ETHUSDT"]
    assert tmp_settings.indicators.rsi_length == 14
    assert tmp_settings.indicators.ma_short == 5
    assert tmp_settings.indicators.ma_long == 25
    assert tmp_settings.rules.price_move.threshold_pct == 5.0
    assert tmp_settings.rules.rsi.overbought == 70.0
    assert tmp_settings.alerts.cooldown_sec == 300.0
    assert tmp_settings.cli.refresh_interval_sec == 0.5


def test_missing_keys_fall_back_to_defaults(tmp_path: Path) -> None:
    """TOML 缺失的键回退到数据类默认值。"""
    config_path = write_config(
        tmp_path,
        '[network]\nproxy = "http://127.0.0.1:63571"\n\n[market]\nsymbols = ["BTCUSDT"]\n',
    )
    settings = load_settings(config_path)
    assert settings.indicators.candle_window == 300
    assert settings.indicators.rsi_length == 14
    assert settings.alerts.cooldown_sec == 300.0
    assert settings.rules.volume_spike.enabled is True


def test_repo_default_config_is_valid() -> None:
    """仓库自带 config/config.toml 必须可加载且仅用 .vision 域名（P0-7）。"""
    assert DEFAULT_CONFIG_PATH.is_file()
    settings = load_settings(DEFAULT_CONFIG_PATH)
    assert settings.market.symbols == ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
    assert "data-api.binance.vision" in settings.network.rest_url
    assert "data-stream.binance.vision" in settings.network.ws_url
    # 路径已解析为绝对路径
    assert Path(settings.storage.db_path).is_absolute()
    assert Path(settings.alerts.log_path).is_absolute()


# ---------------------------------------------------------------------------
# 路径解析
# ---------------------------------------------------------------------------


def test_relative_paths_resolved_against_config_parent(tmp_settings, tmp_path: Path) -> None:
    """相对路径基于「配置文件所在目录的上级」解析为绝对路径。"""
    expected_base = tmp_path.resolve()
    assert Path(tmp_settings.storage.db_path) == expected_base / "data" / "quant.db"
    assert Path(tmp_settings.alerts.log_path) == expected_base / "data" / "alerts.jsonl"


def test_absolute_paths_kept(tmp_path: Path) -> None:
    """绝对路径原样保留。"""
    db_path = tmp_path / "custom" / "q.db"
    config_path = write_config(
        tmp_path,
        f'[network]\nproxy = "http://127.0.0.1:63571"\n\n[market]\nsymbols = ["BTCUSDT"]\n\n'
        f'[storage]\ndb_path = "{db_path.as_posix()}"\n',
    )
    settings = load_settings(config_path)
    assert Path(settings.storage.db_path) == db_path.resolve()


# ---------------------------------------------------------------------------
# 环境变量覆盖与 .env
# ---------------------------------------------------------------------------


def test_env_override_proxy_and_symbols(tmp_config_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """QUANT_PROXY / QUANT_SYMBOLS 覆盖 TOML 值。"""
    monkeypatch.setenv("QUANT_PROXY", "http://127.0.0.1:9999")
    monkeypatch.setenv("QUANT_SYMBOLS", "btcusdt, ethusdt ,solusdt")
    settings = load_settings(tmp_config_path)
    assert settings.network.proxy == "http://127.0.0.1:9999"
    assert settings.market.symbols == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def test_env_override_paths(tmp_config_path: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """QUANT_DB_PATH 覆盖并解析为绝对路径。"""
    monkeypatch.setenv("QUANT_DB_PATH", "data/other.db")
    settings = load_settings(tmp_config_path)
    assert Path(settings.storage.db_path) == tmp_path.resolve() / "data" / "other.db"


def test_dotenv_file_applied(tmp_config_path: Path) -> None:
    """与配置同目录的 .env 生效，且不覆盖已有系统环境变量。

    环境隔离由 autouse fixture ``_clean_quant_env`` 快照-恢复保证
    （``_load_dotenv`` 会把键写入 ``os.environ``，需防止跨测试泄漏）。
    """
    dotenv = tmp_config_path.parent / ".env"
    dotenv.write_text(
        "# 注释行\n\nQUANT_PROXY=\"http://127.0.0.1:7777\"\nQUANT_SYMBOLS=BTCUSDT\n",
        encoding="utf-8",
    )
    settings = load_settings(tmp_config_path)
    assert settings.network.proxy == "http://127.0.0.1:7777"
    assert settings.market.symbols == ["BTCUSDT"]


def test_quant_config_env_points_to_file(tmp_config_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """QUANT_CONFIG 环境变量指定配置文件路径。"""
    monkeypatch.setenv("QUANT_CONFIG", str(tmp_config_path))
    settings = load_settings(None)
    assert settings.market.symbols == ["BTCUSDT", "ETHUSDT"]


# ---------------------------------------------------------------------------
# 校验失败
# ---------------------------------------------------------------------------


def test_missing_config_file_raises(tmp_path: Path) -> None:
    """配置文件不存在时报 ConfigError。"""
    with pytest.raises(ConfigError, match="配置文件不存在"):
        load_settings(tmp_path / "not-exist.toml")


def test_missing_proxy_raises(tmp_path: Path) -> None:
    """代理缺失报明确中文错误。"""
    config_path = write_config(tmp_path, '[market]\nsymbols = ["BTCUSDT"]\n')
    with pytest.raises(ConfigError, match="proxy 不能为空"):
        load_settings(config_path)


def test_empty_symbols_raises(tmp_path: Path) -> None:
    """币种为空报错。"""
    config_path = write_config(
        tmp_path,
        '[network]\nproxy = "http://127.0.0.1:63571"\n\n[market]\nsymbols = []\n',
    )
    with pytest.raises(ConfigError, match="symbols 不能为空"):
        load_settings(config_path)


def test_unknown_key_raises(tmp_path: Path) -> None:
    """未知配置键报错（防止拼写错误被静默忽略）。"""
    config_path = write_config(
        tmp_path,
        '[network]\nproxy = "http://127.0.0.1:63571"\nproxie = "x"\n\n[market]\nsymbols = ["BTCUSDT"]\n',
    )
    with pytest.raises(ConfigError, match="未知配置键"):
        load_settings(config_path)


def test_main_domain_rejected(tmp_path: Path) -> None:
    """币安主域名（非 .vision）被拒绝。"""
    config_path = write_config(
        tmp_path,
        '[network]\nproxy = "http://127.0.0.1:63571"\nrest_url = "https://api.binance.com"\n\n'
        '[market]\nsymbols = ["BTCUSDT"]\n',
    )
    with pytest.raises(ConfigError, match="主机必须为 data-api.binance.vision"):
        load_settings(config_path)


def test_bad_indicator_params_raise(tmp_path: Path) -> None:
    """ma_short >= ma_long 等非法指标参数报错。"""
    config_path = write_config(
        tmp_path,
        '[network]\nproxy = "http://127.0.0.1:63571"\n\n[market]\nsymbols = ["BTCUSDT"]\n\n'
        "[indicators]\nma_short = 25\nma_long = 5\n",
    )
    with pytest.raises(ConfigError, match="ma_short 必须小于 ma_long"):
        load_settings(config_path)


@pytest.mark.parametrize(
    ("extra", "pattern"),
    [
        ("[indicators]\ncandle_window = 0\n", "candle_window 必须为正整数"),
        ("[indicators]\nrsi_length = 1\n", "rsi_length 至少为 2"),
        ("[indicators]\nma_short = 0\n", "ma_short/ma_long 至少为 1"),
        ("[indicators]\nvol_ma_length = 0\n", "vol_ma_length 至少为 1"),
        ("[rules.price_move]\nwindow_min = 0\n", "window_min 至少为 1"),
        ("[rules.price_move]\nthreshold_pct = 0\n", "threshold_pct 必须为正数"),
        ("[rules.volume_spike]\nmultiplier = -1\n", "multiplier 必须为正数"),
        ("[rules.rsi]\noversold = 80\noverbought = 70\n", "0 < oversold < overbought < 100"),
        ("[alerts]\ncooldown_sec = -1\n", "cooldown_sec 不能为负"),
        ("[cli]\nrefresh_interval_sec = 0\n", "refresh_interval_sec 必须为正数"),
    ],
)
def test_invalid_values_raise(tmp_path: Path, extra: str, pattern: str) -> None:
    """各节非法值均有明确中文错误。"""
    config_path = write_config(
        tmp_path,
        '[network]\nproxy = "http://127.0.0.1:63571"\n\n[market]\nsymbols = ["BTCUSDT"]\n\n' + extra,
    )
    with pytest.raises(ConfigError, match=pattern):
        load_settings(config_path)


def test_wrong_scheme_rejected(tmp_path: Path) -> None:
    """WS 协议非 wss 被拒绝。"""
    config_path = write_config(
        tmp_path,
        '[network]\nproxy = "http://127.0.0.1:63571"\nws_url = "ws://data-stream.binance.vision/stream"\n\n'
        '[market]\nsymbols = ["BTCUSDT"]\n',
    )
    with pytest.raises(ConfigError, match="协议必须为 wss"):
        load_settings(config_path)


def test_toml_syntax_error_raises(tmp_path: Path) -> None:
    """TOML 语法错误报 ConfigError。"""
    config_path = write_config(tmp_path, "[network\nproxy = x\n")
    with pytest.raises(ConfigError, match="TOML 解析失败"):
        load_settings(config_path)


# ---------------------------------------------------------------------------
# 脱敏与展示
# ---------------------------------------------------------------------------


def test_mask_proxy_hides_password() -> None:
    """代理 URL 中的密码被脱敏。"""
    assert mask_proxy("http://user:secret@127.0.0.1:63571") == "http://user:***@127.0.0.1:63571"
    assert mask_proxy("http://127.0.0.1:63571") == "http://127.0.0.1:63571"


def test_format_settings_masks_proxy(tmp_settings) -> None:
    """--check-config 输出中代理脱敏，且包含各节。"""
    text = format_settings(tmp_settings)
    assert "network.proxy" in text
    assert "market.symbols" in text
    assert "rules.price_move.threshold_pct" in text
    assert "secret" not in text


def test_check_config_cli(tmp_config_path: Path, capsys: pytest.CaptureFixture) -> None:
    """python -m quant --check-config 返回 0 并打印生效配置。"""
    exit_code = main(["--check-config", "--config", str(tmp_config_path)])
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "network.proxy" in out
    assert "data-stream.binance.vision" in out


def test_check_config_cli_bad_config(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    """配置错误时 --check-config 返回 2 并输出中文错误。"""
    config_path = write_config(tmp_path, "[market]\nsymbols = []\n")
    exit_code = main(["--check-config", "--config", str(config_path)])
    assert exit_code == 2
    err = capsys.readouterr().err
    assert "配置错误" in err


def test_repo_root_helper() -> None:
    """conftest 暴露的仓库根目录正确。"""
    assert (REPO_ROOT / "pyproject.toml").is_file()
