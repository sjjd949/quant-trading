"""配置层：TOML 加载 + 数据类树 + 环境变量覆盖 + 相对路径解析。

加载链路（优先级从低到高）：
1. ``config/config.toml`` 默认值（数据类字段默认值兜底）；
2. TOML 文件值（未知键直接报错，防止拼写错误被静默忽略）；
3. 与配置文件同目录的 ``.env`` 文件（不覆盖已存在的系统环境变量）；
4. 操作系统环境变量按 ``QUANT_<节>_<键>`` 约定覆盖（仅实现少量高频键）。

相对路径基于「配置文件所在目录的上级」解析为绝对路径，保证从任意工作
目录启动行为一致。校验失败抛出 :class:`ConfigError`（中文明确错误）。
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from urllib.parse import urlsplit

#: 仓库根目录下的默认配置文件（src/quant/config.py 上溯三级）
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.toml"

#: 与配置文件同目录的点号环境变量文件名
DOTENV_FILENAME = ".env"

#: REST/WS 允许的域名白名单（.vision 镜像域名，主域名被墙）
REST_HOST = "data-api.binance.vision"
WS_HOST = "data-stream.binance.vision"

#: 环境变量覆盖表：环境变量名 -> (节名, 键名, 值类型)
_ENV_OVERRIDES: dict[str, tuple[str, str, str]] = {
    "QUANT_PROXY": ("network", "proxy", "str"),
    "QUANT_SYMBOLS": ("market", "symbols", "csv"),
    "QUANT_LOG_PATH": ("alerts", "log_path", "str"),
    "QUANT_DB_PATH": ("storage", "db_path", "str"),
}


class ConfigError(ValueError):
    """配置加载或校验失败（消息为中文，便于直接展示给用户）。"""


# ---------------------------------------------------------------------------
# 数据类树
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class NetworkSettings:
    """网络层配置：代理与币安 .vision 双端点。"""

    proxy: str = ""
    ws_url: str = f"wss://{WS_HOST}/stream"
    rest_url: str = f"https://{REST_HOST}"
    ws_idle_timeout_sec: float = 60.0
    rest_timeout_sec: float = 10.0
    ws_max_backoff_sec: float = 30.0


@dataclass(slots=True)
class MarketSettings:
    """市场层配置：监控币种清单。"""

    symbols: list[str] = field(
        default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
    )


@dataclass(slots=True)
class IndicatorSettings:
    """指标层配置：窗口容量与各指标周期。"""

    candle_window: int = 300
    rsi_length: int = 14
    ma_short: int = 5
    ma_long: int = 25
    vol_ma_length: int = 20


@dataclass(slots=True)
class PriceMoveRuleSettings:
    """价格异动规则配置：N 分钟涨跌幅超阈值。"""

    enabled: bool = True
    window_min: int = 5
    threshold_pct: float = 5.0


@dataclass(slots=True)
class VolumeSpikeRuleSettings:
    """成交量突增规则配置：当前量超过均量倍数。"""

    enabled: bool = True
    multiplier: float = 3.0


@dataclass(slots=True)
class RsiRuleSettings:
    """RSI 极值规则配置：超买/超卖阈值。"""

    enabled: bool = True
    overbought: float = 70.0
    oversold: float = 30.0


@dataclass(slots=True)
class MaCrossRuleSettings:
    """均线交叉规则配置（周期复用 indicators.ma_short/ma_long）。"""

    enabled: bool = True


@dataclass(slots=True)
class RuleSettings:
    """规则层配置：4 条内置规则的开关与参数。"""

    price_move: PriceMoveRuleSettings = field(default_factory=PriceMoveRuleSettings)
    volume_spike: VolumeSpikeRuleSettings = field(default_factory=VolumeSpikeRuleSettings)
    rsi: RsiRuleSettings = field(default_factory=RsiRuleSettings)
    ma_cross: MaCrossRuleSettings = field(default_factory=MaCrossRuleSettings)


@dataclass(slots=True)
class AlertSettings:
    """告警层配置：冷却时间与 JSONL 日志路径。"""

    cooldown_sec: float = 300.0
    log_path: str = "data/alerts.jsonl"


@dataclass(slots=True)
class StorageSettings:
    """存储层配置：SQLite 数据库路径。"""

    db_path: str = "data/quant.db"


@dataclass(slots=True)
class CliSettings:
    """CLI 层配置：看板刷新周期。"""

    refresh_interval_sec: float = 0.5


@dataclass(slots=True)
class Settings:
    """全局配置树（与 ``config/config.toml`` 的节一一对应）。"""

    network: NetworkSettings = field(default_factory=NetworkSettings)
    market: MarketSettings = field(default_factory=MarketSettings)
    indicators: IndicatorSettings = field(default_factory=IndicatorSettings)
    rules: RuleSettings = field(default_factory=RuleSettings)
    alerts: AlertSettings = field(default_factory=AlertSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)
    cli: CliSettings = field(default_factory=CliSettings)


# ---------------------------------------------------------------------------
# 加载与构建
# ---------------------------------------------------------------------------


def load_settings(path: str | Path | None = None) -> Settings:
    """加载配置并返回校验后的 :class:`Settings`。

    参数:
        path: 配置文件路径；为 ``None`` 时依次取环境变量 ``QUANT_CONFIG``
            与仓库默认 ``config/config.toml``。

    异常:
        ConfigError: 文件不存在、TOML 语法错误、未知键或校验失败。
    """
    config_path = _resolve_config_path(path)
    if not config_path.is_file():
        raise ConfigError(f"配置文件不存在: {config_path}")

    try:
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"配置文件 TOML 解析失败: {config_path}: {exc}") from exc

    # .env 先于环境变量覆盖读取，使 .env 中的 QUANT_* 键生效
    _load_dotenv(config_path.parent / DOTENV_FILENAME)

    settings = _build_settings(raw)
    _apply_env_overrides(settings)

    base_dir = config_path.resolve().parent.parent
    _resolve_paths(settings, base_dir)
    _validate(settings)
    return settings


def _resolve_config_path(path: str | Path | None) -> Path:
    """解析配置文件路径：显式参数 > QUANT_CONFIG > 仓库默认。"""
    if path is not None:
        return Path(path).expanduser()
    env_path = os.environ.get("QUANT_CONFIG")
    if env_path:
        return Path(env_path).expanduser()
    return DEFAULT_CONFIG_PATH


def _build_section(cls: type, data: object, section_name: str):
    """用 TOML 节数据构造数据类；未知键报错，缺失键用默认值兜底。"""
    if not isinstance(data, dict):
        raise ConfigError(f"[{section_name}] 配置节类型错误，应为表/节")
    known = {f.name for f in fields(cls)}
    unknown = sorted(set(data) - known)
    if unknown:
        raise ConfigError(f"[{section_name}] 存在未知配置键: {unknown}")
    return cls(**data)


def _build_settings(raw: dict) -> Settings:
    """将 TOML 原始字典构建为 Settings 数据类树。"""
    rules_raw = raw.get("rules", {})
    return Settings(
        network=_build_section(NetworkSettings, raw.get("network", {}), "network"),
        market=_build_section(MarketSettings, raw.get("market", {}), "market"),
        indicators=_build_section(
            IndicatorSettings, raw.get("indicators", {}), "indicators"
        ),
        rules=RuleSettings(
            price_move=_build_section(
                PriceMoveRuleSettings, rules_raw.get("price_move", {}), "rules.price_move"
            ),
            volume_spike=_build_section(
                VolumeSpikeRuleSettings,
                rules_raw.get("volume_spike", {}),
                "rules.volume_spike",
            ),
            rsi=_build_section(RsiRuleSettings, rules_raw.get("rsi", {}), "rules.rsi"),
            ma_cross=_build_section(
                MaCrossRuleSettings, rules_raw.get("ma_cross", {}), "rules.ma_cross"
            ),
        ),
        alerts=_build_section(AlertSettings, raw.get("alerts", {}), "alerts"),
        storage=_build_section(StorageSettings, raw.get("storage", {}), "storage"),
        cli=_build_section(CliSettings, raw.get("cli", {}), "cli"),
    )


def _load_dotenv(path: Path) -> None:
    """读取 ``.env`` 文件写入环境变量（不覆盖已存在的系统环境变量）。

    仅支持 ``KEY=VALUE`` 简单语法（``#`` 注释、空行、可选引号），
    不引入 python-dotenv 第三方依赖。
    """
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _apply_env_overrides(settings: Settings) -> None:
    """按 ``QUANT_<节>_<键>`` 约定用环境变量覆盖配置。"""
    for env_key, (section_name, key, kind) in _ENV_OVERRIDES.items():
        raw = os.environ.get(env_key)
        if raw is None or raw == "":
            continue
        section = getattr(settings, section_name)
        if kind == "csv":
            value: object = [item.strip().upper() for item in raw.split(",") if item.strip()]
        elif kind == "float":
            value = float(raw)
        else:
            value = raw
        setattr(section, key, value)


def _resolve_paths(settings: Settings, base_dir: Path) -> None:
    """将相对路径基于 ``base_dir`` 解析为绝对路径。"""
    settings.alerts.log_path = str(_resolve_one(base_dir, settings.alerts.log_path))
    settings.storage.db_path = str(_resolve_one(base_dir, settings.storage.db_path))


def _resolve_one(base_dir: Path, raw_path: str) -> Path:
    """单路径解析：绝对路径原样返回，相对路径基于 base_dir 拼接。"""
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


# ---------------------------------------------------------------------------
# 校验
# ---------------------------------------------------------------------------


def _validate(settings: Settings) -> None:
    """校验生效配置，失败抛出中文 :class:`ConfigError`。"""
    if not settings.network.proxy.strip():
        raise ConfigError("[network] proxy 不能为空：所有外网请求必须经本地代理")
    _check_url(
        settings.network.rest_url,
        REST_HOST,
        "https",
        "[network] rest_url",
    )
    _check_url(settings.network.ws_url, WS_HOST, "wss", "[network] ws_url")

    symbols = [s.strip().upper() for s in settings.market.symbols if s.strip()]
    if not symbols:
        raise ConfigError("[market] symbols 不能为空：至少需要一个监控币种")
    settings.market.symbols = symbols

    indicators = settings.indicators
    if indicators.candle_window <= 0:
        raise ConfigError("[indicators] candle_window 必须为正整数")
    if indicators.rsi_length < 2:
        raise ConfigError("[indicators] rsi_length 至少为 2")
    if indicators.ma_short < 1 or indicators.ma_long < 1:
        raise ConfigError("[indicators] ma_short/ma_long 至少为 1")
    if indicators.ma_short >= indicators.ma_long:
        raise ConfigError("[indicators] ma_short 必须小于 ma_long")
    if indicators.vol_ma_length < 1:
        raise ConfigError("[indicators] vol_ma_length 至少为 1")

    price_move = settings.rules.price_move
    if price_move.window_min < 1:
        raise ConfigError("[rules.price_move] window_min 至少为 1")
    if price_move.threshold_pct <= 0:
        raise ConfigError("[rules.price_move] threshold_pct 必须为正数")

    if settings.rules.volume_spike.multiplier <= 0:
        raise ConfigError("[rules.volume_spike] multiplier 必须为正数")

    rsi_rule = settings.rules.rsi
    if not 0 < rsi_rule.oversold < rsi_rule.overbought < 100:
        raise ConfigError("[rules.rsi] 需满足 0 < oversold < overbought < 100")

    if settings.alerts.cooldown_sec < 0:
        raise ConfigError("[alerts] cooldown_sec 不能为负")
    if not settings.alerts.log_path.strip():
        raise ConfigError("[alerts] log_path 不能为空")
    if not settings.storage.db_path.strip():
        raise ConfigError("[storage] db_path 不能为空")

    if settings.cli.refresh_interval_sec <= 0:
        raise ConfigError("[cli] refresh_interval_sec 必须为正数")


def _check_url(url: str, expected_host: str, expected_scheme: str, label: str) -> None:
    """校验 URL 协议与主机（仅允许 .vision 镜像域名）。"""
    try:
        parts = urlsplit(url)
    except ValueError as exc:
        raise ConfigError(f"{label} 不是合法 URL: {url}") from exc
    if parts.scheme != expected_scheme:
        raise ConfigError(
            f"{label} 协议必须为 {expected_scheme}（当前 {parts.scheme or '缺失'}）: {url}"
        )
    if (parts.hostname or "").lower() != expected_host:
        raise ConfigError(
            f"{label} 主机必须为 {expected_host}（币安主域名不可用）: {url}"
        )


# ---------------------------------------------------------------------------
# 展示辅助
# ---------------------------------------------------------------------------


def mask_proxy(proxy: str) -> str:
    """代理地址脱敏：隐藏 URL 中的密码部分（无用户信息时原样返回）。"""
    try:
        parts = urlsplit(proxy)
    except ValueError:
        return "***"
    if not parts.password:
        return proxy
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    user = parts.username or ""
    netloc = f"{user}:***@{host}"
    return parts._replace(netloc=netloc).geturl()


def format_settings(settings: Settings) -> str:
    """将生效配置格式化为多行文本（代理脱敏），用于 --check-config。"""
    lines: list[str] = []
    _format_dataclass(settings, "", lines)
    return "\n".join(lines)


def _format_dataclass(obj: object, prefix: str, lines: list[str]) -> None:
    """递归格式化数据类（嵌套节输出为 ``[a.b]`` 小节标题）。"""
    for f in fields(obj):  # type: ignore[arg-type]
        value = getattr(obj, f.name)
        name = f"{prefix}{f.name}"
        if is_dataclass(value):
            lines.append(f"  [{name}]")
            _format_dataclass(value, f"{name}.", lines)
        else:
            lines.append(f"    {name} = {_format_value(name, value)}")


def _format_value(name: str, value: object) -> str:
    """单值格式化：代理脱敏、列表逗号连接。"""
    if name.endswith("network.proxy"):
        return mask_proxy(str(value))
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    return str(value)
