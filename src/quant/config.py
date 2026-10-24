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
    "QUANT_PAPER_ENABLED": ("paper", "enabled", "bool"),
    "QUANT_PAPER_MODE": ("paper", "mode", "str"),
    "QUANT_PAPER_SLIPPAGE_BPS": ("paper", "slippage_bps", "float"),
    "QUANT_PAPER_POSITION_PCT": ("paper", "position_pct", "float"),
}

#: 系统代理环境变量键名（按优先级排列，大小写均覆盖）
_SYSTEM_PROXY_KEYS = ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy")


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
class AShareSettings:
    """A股市场配置：自选清单与轮询参数。"""

    enabled: bool = True
    watchlist: list[str] = field(
        default_factory=lambda: ["600519.SH", "600036.SH", "000001.SZ", "300750.SZ"]
    )
    poll_interval_sec: float = 3.0


@dataclass(slots=True)
class HkUsSettings:
    """美股/港股市场配置：自选清单（分市场）与轮询参数。"""

    enabled: bool = True
    watchlist_us: list[str] = field(
        default_factory=lambda: ["AAPL.US", "MSFT.US", "NVDA.US", "TSLA.US"]
    )
    watchlist_hk: list[str] = field(
        default_factory=lambda: ["0700.HK", "9988.HK", "3690.HK"]
    )
    poll_interval_sec: float = 3.0


@dataclass(slots=True)
class MarketSettings:
    """市场层配置：加密货币币种清单 + 跨市场子节。"""

    symbols: list[str] = field(
        default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
    )
    a_share: AShareSettings = field(default_factory=AShareSettings)
    hk_us: HkUsSettings = field(default_factory=HkUsSettings)


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
    """规则层配置：4 条内置规则的开关与参数 + 自定义规则登记。"""

    price_move: PriceMoveRuleSettings = field(default_factory=PriceMoveRuleSettings)
    volume_spike: VolumeSpikeRuleSettings = field(default_factory=VolumeSpikeRuleSettings)
    rsi: RsiRuleSettings = field(default_factory=RsiRuleSettings)
    ma_cross: MaCrossRuleSettings = field(default_factory=MaCrossRuleSettings)
    #: 自定义规则登记：``"模块路径:类名"`` 列表（P1-5 插件化）
    custom: list[str] = field(default_factory=list)


@dataclass(slots=True)
class InitialCashSettings:
    """模拟盘各市场初始虚拟资金（计价货币：USDT/CNY/USD/HKD）。"""

    crypto: float = 100000.0
    a_share: float = 100000.0
    us: float = 100000.0
    hk: float = 100000.0


@dataclass(slots=True)
class FeeSettings:
    """模拟盘各市场费率表（按 PRD-M2 §5.4 / §7 默认值，可配置）。"""

    crypto_taker: float = 0.001
    a_share_commission: float = 0.00025
    a_share_stamp_tax: float = 0.001
    a_share_transfer: float = 0.00001
    us_commission: float = 0.0
    hk_commission: float = 0.0003
    hk_stamp_tax: float = 0.001


@dataclass(slots=True)
class PaperSettings:
    """模拟盘配置：总开关/模式/初始资金/滑点/费率/仓位约束。"""

    enabled: bool = True
    mode: str = "manual"
    initial_cash: InitialCashSettings = field(default_factory=InitialCashSettings)
    slippage_bps: float = 5.0
    fees: FeeSettings = field(default_factory=FeeSettings)
    position_pct: float = 10.0
    confirm_timeout_sec: float = 300.0
    one_position_per_symbol: bool = True


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
    paper: PaperSettings = field(default_factory=PaperSettings)


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
    # 代理解析：QUANT_PROXY > 系统代理环境变量 > config.toml 值（可为空）
    settings.network.proxy = resolve_proxy(settings.network.proxy)

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


def _ensure_no_unknown(data: dict, cls: type, section_name: str) -> None:
    """校验顶层节数据不含未知键（嵌套节由 :func:`_build_section` 处理）。"""
    if not isinstance(data, dict):
        raise ConfigError(f"[{section_name}] 配置节类型错误，应为表/节")
    known = {f.name for f in fields(cls)}
    unknown = sorted(set(data) - known)
    if unknown:
        raise ConfigError(f"[{section_name}] 存在未知配置键: {unknown}")


def _build_settings(raw: dict) -> Settings:
    """将 TOML 原始字典构建为 Settings 数据类树。"""
    rules_raw = raw.get("rules", {})
    market_raw = raw.get("market", {})
    paper_raw = raw.get("paper", {})
    # 顶层未知键检查（嵌套节由 _build_section 内部校验）
    _ensure_no_unknown(market_raw, MarketSettings, "market")
    _ensure_no_unknown(paper_raw, PaperSettings, "paper")
    return Settings(
        network=_build_section(NetworkSettings, raw.get("network", {}), "network"),
        market=MarketSettings(
            symbols=market_raw.get(
                "symbols", ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
            ),
            a_share=_build_section(
                AShareSettings, market_raw.get("a_share", {}), "market.a_share"
            ),
            hk_us=_build_section(
                HkUsSettings, market_raw.get("hk_us", {}), "market.hk_us"
            ),
        ),
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
            custom=list(rules_raw.get("custom", [])),
        ),
        alerts=_build_section(AlertSettings, raw.get("alerts", {}), "alerts"),
        storage=_build_section(StorageSettings, raw.get("storage", {}), "storage"),
        cli=_build_section(CliSettings, raw.get("cli", {}), "cli"),
        paper=PaperSettings(
            enabled=paper_raw.get("enabled", True),
            mode=paper_raw.get("mode", "manual"),
            initial_cash=_build_section(
                InitialCashSettings,
                paper_raw.get("initial_cash", {}),
                "paper.initial_cash",
            ),
            slippage_bps=paper_raw.get("slippage_bps", 5.0),
            fees=_build_section(FeeSettings, paper_raw.get("fees", {}), "paper.fees"),
            position_pct=paper_raw.get("position_pct", 10.0),
            confirm_timeout_sec=paper_raw.get("confirm_timeout_sec", 300.0),
            one_position_per_symbol=paper_raw.get("one_position_per_symbol", True),
        ),
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
    """按 ``QUANT_<节>_<键>`` 约定用环境变量覆盖配置（空白值视为未设置）。"""
    for env_key, (section_name, key, kind) in _ENV_OVERRIDES.items():
        raw = os.environ.get(env_key)
        if raw is None or raw.strip() == "":
            continue  # 未设置或空白：保留下层配置（与 resolve_proxy 语义一致）
        section = getattr(settings, section_name)
        if kind == "csv":
            value: object = [item.strip().upper() for item in raw.split(",") if item.strip()]
        elif kind == "float":
            value = float(raw)
        elif kind == "bool":
            value = raw.strip().lower() in ("1", "true", "yes", "on")
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
        raise ConfigError(
            "[network] proxy 不能为空：请设置系统代理环境变量"
            "（HTTPS_PROXY/HTTP_PROXY）、或设置 QUANT_PROXY、"
            "或在 config.toml 的 [network] proxy 填写代理地址"
        )
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

    paper = settings.paper
    if paper.mode not in ("auto", "manual"):
        raise ConfigError("[paper] mode 必须为 'auto' 或 'manual'")
    if paper.position_pct <= 0:
        raise ConfigError("[paper] position_pct 必须为正数")
    if paper.confirm_timeout_sec < 0:
        raise ConfigError("[paper] confirm_timeout_sec 不能为负")
    if paper.slippage_bps < 0:
        raise ConfigError("[paper] slippage_bps 不能为负")


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


def resolve_proxy(proxy: str | None = None) -> str:
    """解析生效代理地址（三级优先级，从高到低）。

    1. ``QUANT_PROXY`` 环境变量（项目专用显式配置）；
    2. 系统代理环境变量 ``HTTPS_PROXY``/``https_proxy``/``HTTP_PROXY``/``http_proxy``；
    3. 配置文件 ``[network] proxy`` 的值（可为空，表示自动使用系统代理）。

    参数:
        proxy: 配置文件中的 proxy 值（``None`` 或空字符串表示未配置）。

    返回:
        生效代理地址（已去首尾空白）；三级来源均为空时返回空字符串。
    """
    quant_proxy = os.environ.get("QUANT_PROXY")
    if quant_proxy and quant_proxy.strip():
        return quant_proxy.strip()
    for key in _SYSTEM_PROXY_KEYS:
        value = os.environ.get(key)
        if value and value.strip():
            return value.strip()
    return (proxy or "").strip()


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
