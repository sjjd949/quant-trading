"""QA 独立验证测试（严过关）：不复用工程师测试辅助，自建伪造对象。

覆盖团队下达的重点核查项：
- A. 模块可独立导入（循环导入修复）+ CLI 入口 --check-config
- B. 禁用约束（src 无主域名/OKX/Yahoo、无 TODO/FIXME）
- C. 配置缺失/非法值（中文 ConfigError）+ 配置与代码分离 + 环境变量覆盖
- D. REST 重试：5xx/429/网络错误共 4 次请求、退避 [0.5,1,2]、429 Retry-After、4xx 直抛
- E. WS：退避 1→2→4→8→16→30 封顶、看门狗重连、gap 修复 startTime/上限 5、去重
- F. 指标：RSI/MA/量均与 pandas-ta 参考实现误差 < 1e-9；RingBuffer 边界
- G. 规则：4 条边界语义 + 单规则异常隔离 + 配置独立开关
- H. 告警：冷却去重（默认 300s）、JSONL、CSV/JSON 导出、SQLite 过滤查询
- I. 存储：upsert 幂等 + 双连接并发边界
- J. DashboardState.snapshot() 浅拷贝语义 + 渲染循环与数据流解耦
- K. 端到端最小链路：Tick/Candle → 指标 → 规则 → 告警落盘（自建 fake adapter）
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

import httpx
import pandas as pd
import pytest

from quant.alerts.manager import AlertManager
from quant.cli.dashboard import DashboardRenderer, DashboardState
from quant.config import ConfigError, Settings, load_settings
from quant.feeds.base import FeedStatus
from quant.feeds.binance_rest import BinanceRestClient
from quant.feeds.binance_ws import BinanceWSAdapter
from quant.indicators.buffers import RingBuffer
from quant.indicators.engine import IndicatorEngine
from quant.models import Alert, Candle, IndicatorSnapshot, Tick
from quant.scanner.base import Rule, RuleContext
from quant.scanner.engine import RuleEngine
from quant.scanner.rules import (
    MaCrossRule,
    PriceMoveRule,
    RsiRule,
    VolumeSpikeRule,
)
from quant.storage.sqlite_store import Storage
from quant.utils.timeutils import MS_PER_MINUTE, ms_to_datetime

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
REPO_ROOT = Path(__file__).resolve().parents[1]

#: 子进程环境：继承当前环境（Windows Python 启动需要），仅覆盖 PYTHONPATH
#: 并剔除 QUANT_* 防止外部配置干扰
SUBPROCESS_ENV = {
    **os.environ,
    "PYTHONPATH": str(SRC_ROOT),
    **{key: "" for key in os.environ if key.startswith("QUANT_")},
}
for key in [key for key in SUBPROCESS_ENV if key.startswith("QUANT_")]:
    del SUBPROCESS_ENV[key]

# 固定时间基线（2025-10-24 06:32:00 UTC），测试不依赖 datetime.now()
BASE_OPEN_MS = 1761287520000
BASE_CLOSE_MS = BASE_OPEN_MS + MS_PER_MINUTE - 1


# ---------------------------------------------------------------------------
# 公共构造函数（自建，独立于工程师测试代码）
# ---------------------------------------------------------------------------


def make_candle(
    symbol: str = "BTCUSDT",
    index: int = 0,
    close: float = 100.0,
    volume: float = 10.0,
    is_closed: bool = True,
) -> Candle:
    """构造第 ``index`` 分钟的 1min K 线（固定时间基线）。"""
    open_ms = BASE_OPEN_MS + index * MS_PER_MINUTE
    return Candle(
        symbol=symbol,
        open_time_ms=open_ms,
        close_time_ms=open_ms + MS_PER_MINUTE - 1,
        open=close - 1.0,
        high=close + 2.0,
        low=close - 2.0,
        close=close,
        volume=volume,
        quote_volume=close * volume,
        trades=7,
        is_closed=is_closed,
    )


def make_window(count: int, closes: list[float] | None = None) -> list[Candle]:
    """构造 ``count`` 根已收盘 K 线序列（可指定收盘价序列）。"""
    candles = []
    for i in range(count):
        close = closes[i] if closes is not None and i < len(closes) else 100.0
        candles.append(make_candle(index=i, close=close))
    return candles


# ---------------------------------------------------------------------------
# A. 模块可独立导入 + CLI 入口
# ---------------------------------------------------------------------------

#: src/quant 下全部模块（相对点号路径，__init__ 归约为包名）
def _all_module_names() -> list[str]:
    names: set[str] = set()
    for path in SRC_ROOT.glob("quant/**/*.py"):
        parts = list(path.relative_to(SRC_ROOT).with_suffix("").parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        if parts:  # 排除 quant/__init__.py 归约后的空列表
            names.add(".".join(parts))
    return sorted(names)


ALL_MODULES = _all_module_names()


@pytest.mark.parametrize("module_name", ALL_MODULES)
def test_module_imports_independently(module_name: str) -> None:
    """每个模块在全新解释器中可独立导入（循环导入修复验证）。"""
    result = subprocess.run(
        [sys.executable, "-c", f"import {module_name}; print('ok')"],
        capture_output=True,
        text=True,
        timeout=120,
        env=SUBPROCESS_ENV,
    )
    assert result.returncode == 0, f"{module_name} 独立导入失败:\n{result.stderr}"
    assert "ok" in result.stdout


def test_cli_package_does_not_export_main() -> None:
    """quant.cli 包级 __all__ 不再导出 main（避免 app→cli→main→app 循环导入）。

    注意：进程内其他测试若已 ``import quant.cli.main``，Python 会在包对象上
    绑定 ``main`` 属性（标准行为），故进程内只断言 ``__all__`` 契约；
    新鲜解释器下的导入语义由子进程测试覆盖。
    """
    import quant.cli as cli_pkg

    assert "main" not in cli_pkg.__all__
    assert "DashboardState" in cli_pkg.__all__


def test_importing_cli_does_not_pull_in_app_or_main() -> None:
    """import quant.cli 不得连带导入 quant.app / quant.cli.main。"""
    code = (
        "import sys; import quant.cli;"
        "assert 'quant.app' not in sys.modules, 'quant.cli 牵连导入了 quant.app';"
        "assert 'quant.cli.main' not in sys.modules, 'quant.cli 牵连导入了 quant.cli.main';"
        "print('ok')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
        env=SUBPROCESS_ENV,
    )
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout


def test_check_config_cli_masks_proxy_and_prints_symbols() -> None:
    """python -m quant --check-config 打印生效配置且代理脱敏（T01/P0-7）。"""
    result = subprocess.run(
        [sys.executable, "-m", "quant", "--check-config"],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(REPO_ROOT),
        env=SUBPROCESS_ENV,
    )
    assert result.returncode == 0, result.stderr
    assert "BTCUSDT" in result.stdout
    assert "http://127.0.0.1:63571" in result.stdout  # 无密码代理原样展示
    assert "password" not in result.stdout


def test_check_config_masks_proxy_password() -> None:
    """代理带密码时 --check-config 输出脱敏为 ***。"""
    from quant.config import mask_proxy

    masked = mask_proxy("http://user:secret@127.0.0.1:63571")
    assert "secret" not in masked
    assert "***" in masked
    assert mask_proxy("http://127.0.0.1:63571") == "http://127.0.0.1:63571"


# ---------------------------------------------------------------------------
# B. 禁用约束（负例扫描）
# ---------------------------------------------------------------------------


def test_no_forbidden_domains_in_src() -> None:
    """src/ 内无 api.binance.com / stream.binance.com / OKX / Yahoo 域名。"""
    forbidden = ("api.binance.com", "stream.binance.com", "okex", "okx", "yahoo")
    violations = []
    for path in SRC_ROOT.glob("quant/**/*.py"):
        text = path.read_text(encoding="utf-8").lower()
        for token in forbidden:
            if token in text:
                violations.append(f"{path.name}: {token}")
    assert not violations, f"发现禁用域名: {violations}"


def test_no_todo_fixme_placeholder_in_src() -> None:
    """src/ 内无 TODO/FIXME/占位符/待实现标记。"""
    markers = ("todo", "fixme", "placeholder", "待实现", "未实现", "not implemented")
    violations = []
    for path in SRC_ROOT.glob("quant/**/*.py"):
        text = path.read_text(encoding="utf-8").lower()
        for token in markers:
            if token in text:
                violations.append(f"{path.name}: {token}")
    assert not violations, f"发现占位标记: {violations}"


# ---------------------------------------------------------------------------
# C. 配置缺失/非法值 + 配置分离 + 环境变量覆盖
# ---------------------------------------------------------------------------


def _write_config(tmp_path: Path, content: str) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    path = config_dir / "config.toml"
    path.write_text(content, encoding="utf-8")
    return path


def _config_text(
    *,
    proxy: str = '"http://127.0.0.1:63571"',
    rest_url: str = '"https://data-api.binance.vision"',
    ws_url: str = '"wss://data-stream.binance.vision/stream"',
    symbols: str = '["BTCUSDT"]',
    extra: str = "",
) -> str:
    """完整合法配置文本（可覆盖单键；``extra`` 追加在末尾同节语义下慎用）。"""
    return (
        "[network]\n"
        f"proxy = {proxy}\n"
        f"rest_url = {rest_url}\n"
        f"ws_url = {ws_url}\n"
        "[market]\n"
        f"symbols = {symbols}\n"
        f"{extra}"
    )


def test_missing_proxy_raises_chinese_error(tmp_path: Path) -> None:
    """代理缺失：中文 ConfigError。"""
    path = _write_config(tmp_path, _config_text(proxy='""'))
    with pytest.raises(ConfigError, match="proxy"):
        load_settings(path)


def test_illegal_rest_host_rejected(tmp_path: Path) -> None:
    """币安主域名 REST 地址被拒绝（数据源约束）。"""
    path = _write_config(tmp_path, _config_text(rest_url='"https://api.binance.com"'))
    with pytest.raises(ConfigError, match="data-api.binance.vision"):
        load_settings(path)


def test_illegal_ws_scheme_rejected(tmp_path: Path) -> None:
    """WS 协议非 wss 被拒绝。"""
    path = _write_config(tmp_path, _config_text(ws_url='"ws://data-stream.binance.vision/stream"'))
    with pytest.raises(ConfigError, match="wss"):
        load_settings(path)


def test_empty_symbols_rejected(tmp_path: Path) -> None:
    """币种为空被拒绝。"""
    path = _write_config(tmp_path, _config_text(symbols='["", "  "]'))
    with pytest.raises(ConfigError, match="symbols"):
        load_settings(path)


def test_rsi_thresholds_inverted_rejected(tmp_path: Path) -> None:
    """oversold >= overbought 被拒绝。"""
    path = _write_config(
        tmp_path,
        _config_text(
            extra="[rules.rsi]\noverbought = 30.0\noversold = 70.0\n",
        ),
    )
    with pytest.raises(ConfigError, match="oversold"):
        load_settings(path)


def test_unknown_config_key_rejected(tmp_path: Path) -> None:
    """未知配置键直接报错（防拼写错误被静默忽略）。"""
    path = _write_config(tmp_path, _config_text(extra='proxyy = "http://x"\n'))
    with pytest.raises(ConfigError, match="未知配置键"):
        load_settings(path)


def test_config_separation_changes_symbols_without_code(tmp_path: Path) -> None:
    """改 config.toml 不改代码即可改币种（P0-7 配置与代码分离）。"""
    path = _write_config(
        tmp_path, _config_text(symbols='["BTCUSDT", "XRPUSDT"]')
    )
    settings = load_settings(path)
    assert settings.market.symbols == ["BTCUSDT", "XRPUSDT"]


def test_env_override_symbols(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """QUANT_SYMBOLS 环境变量覆盖 TOML（自动大写、去空白）。"""
    path = _write_config(tmp_path, _config_text())
    monkeypatch.setenv("QUANT_SYMBOLS", " btcusdt , ethusdt ")
    settings = load_settings(path)
    assert settings.market.symbols == ["BTCUSDT", "ETHUSDT"]


def test_relative_paths_resolved_against_config_parent(tmp_path: Path) -> None:
    """相对路径基于配置文件所在目录的上级解析为绝对路径。"""
    path = _write_config(
        tmp_path,
        _config_text(
            extra='[alerts]\nlog_path = "data/alerts.jsonl"\n[storage]\ndb_path = "data/quant.db"\n',
        ),
    )
    settings = load_settings(path)
    base = tmp_path.resolve()
    assert settings.alerts.log_path == str(base / "data" / "alerts.jsonl")
    assert settings.storage.db_path == str(base / "data" / "quant.db")


def test_default_cooldown_is_300s(tmp_path: Path) -> None:
    """默认冷却 300s（P0-6 可配，默认值核对）。"""
    path = _write_config(tmp_path, _config_text())
    settings = load_settings(path)
    assert settings.alerts.cooldown_sec == 300.0


# ---------------------------------------------------------------------------
# D. REST 重试独立验证
# ---------------------------------------------------------------------------


def _rest_client(handler, sleeps: list[float]) -> BinanceRestClient:
    """用 MockTransport + 退避记录器构造 REST 客户端（禁网）。"""

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(
        transport=transport, base_url="https://data-api.binance.vision"
    )
    return BinanceRestClient(client=client, sleep_func=fake_sleep)


def _kline_row(open_ms: int = BASE_OPEN_MS) -> list:
    return [
        open_ms,
        "99.0",
        "101.0",
        "98.0",
        "100.0",
        "10.0",
        open_ms + MS_PER_MINUTE - 1,
        "1000.0",
        7,
    ]


def test_rest_5xx_exhausted_4_requests_backoff_sequence() -> None:
    """持续 5xx：共 4 次请求，退避序列 [0.5, 1, 2]，最终抛出。"""
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503, json={"msg": "unavailable"})

    sleeps: list[float] = []
    client = _rest_client(handler, sleeps)

    async def _run() -> None:
        try:
            await client.klines("BTCUSDT")
        finally:
            await client.aclose()

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        asyncio.run(_run())
    assert exc_info.value.response.status_code == 503
    assert calls["n"] == 4
    assert sleeps == [0.5, 1.0, 2.0]


def test_rest_network_error_4_requests_backoff_sequence() -> None:
    """持续网络错误：共 4 次请求，退避序列 [0.5, 1, 2]，最终抛出。"""
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("模拟网络中断")

    sleeps: list[float] = []
    client = _rest_client(handler, sleeps)

    async def _run() -> None:
        try:
            await client.klines("BTCUSDT")
        finally:
            await client.aclose()

    with pytest.raises(httpx.ConnectError):
        asyncio.run(_run())
    assert calls["n"] == 4
    assert sleeps == [0.5, 1.0, 2.0]


def test_rest_429_respects_retry_after_then_success() -> None:
    """429 尊重 Retry-After 头；恢复后成功，共 2 次请求。"""
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "2.5"})
        return httpx.Response(200, json=[_kline_row()])

    sleeps: list[float] = []
    client = _rest_client(handler, sleeps)

    async def _run() -> list:
        try:
            return await client.klines("BTCUSDT")
        finally:
            await client.aclose()

    candles = asyncio.run(_run())
    assert calls["n"] == 2
    assert sleeps == [2.5]
    assert len(candles) == 1
    assert candles[0].is_closed is True
    assert candles[0].close == 100.0


def test_rest_4xx_no_retry() -> None:
    """4xx（400）直接抛出，仅 1 次请求、零退避。"""
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, json={"msg": "bad request"})

    sleeps: list[float] = []
    client = _rest_client(handler, sleeps)

    async def _run() -> None:
        try:
            await client.klines("BTCUSDT")
        finally:
            await client.aclose()

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_run())
    assert calls["n"] == 1
    assert sleeps == []


def test_rest_klines_request_params() -> None:
    """请求 path/params 构造正确（symbol 大写、startTime 映射）。"""
    seen: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        return httpx.Response(200, json=[_kline_row(BASE_OPEN_MS + 60_000)])

    client = _rest_client(handler, [])

    async def _run() -> None:
        try:
            await client.klines(
                "btcusdt", interval="1m", limit=5, start_ms=BASE_OPEN_MS + 60_000
            )
        finally:
            await client.aclose()

    asyncio.run(_run())
    assert seen["path"] == "/api/v3/klines"
    assert seen["params"]["symbol"] == "BTCUSDT"
    assert seen["params"]["interval"] == "1m"
    assert seen["params"]["limit"] == "5"
    assert seen["params"]["startTime"] == str(BASE_OPEN_MS + 60_000)


# ---------------------------------------------------------------------------
# E. WS 适配器独立验证
# ---------------------------------------------------------------------------


class QaFakeWS:
    """伪造 WS 连接：recv 按脚本返回，耗尽后阻塞或抛错。"""

    def __init__(self, frames: list[str], on_exhausted: str = "block") -> None:
        self._frames = list(frames)
        self._on_exhausted = on_exhausted
        self.closed = False

    async def recv(self) -> str:
        if self._frames:
            return self._frames.pop(0)
        if self._on_exhausted == "raise":
            raise ConnectionError("模拟连接中断")
        await asyncio.Event().wait()  # 阻塞：由外层取消/超时收尾
        raise AssertionError("unreachable")

    async def close(self) -> None:
        self.closed = True


class QaFakeConnector:
    """伪造连接工厂：按脚本产出连接或抛错，记录调用次数。"""

    def __init__(self, script: list) -> None:
        self._script = list(script)
        self.calls = 0

    async def __call__(self, url: str):
        self.calls += 1
        if not self._script:
            raise ConnectionError("脚本耗尽")
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class QaStopLoop(Exception):
    """退避记录器达到上限时抛出，用于收敛无限重连循环。"""


def _sleep_recorder(limit: int):
    recorded: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)
        if len(recorded) >= limit:
            raise QaStopLoop

    return recorded, fake_sleep


def _ws_frame(stream: str, payload: dict) -> str:
    return json.dumps({"stream": stream, "data": payload})


def _trade_frame(symbol: str, price: float, trade_ms: int) -> str:
    return _ws_frame(
        f"{symbol.lower()}@trade",
        {"e": "trade", "s": symbol, "p": str(price), "q": "1.0", "T": trade_ms, "m": True},
    )


def _kline_frame(symbol: str, open_ms: int, close: float, is_closed: bool) -> str:
    return _ws_frame(
        f"{symbol.lower()}@kline_1m",
        {
            "e": "kline",
            "s": symbol,
            "k": {
                "t": open_ms,
                "T": open_ms + MS_PER_MINUTE - 1,
                "s": symbol,
                "o": str(close - 1),
                "h": str(close + 1),
                "l": str(close - 2),
                "c": str(close),
                "v": "10.0",
                "q": "1000.0",
                "n": 7,
                "x": is_closed,
            },
        },
    )


def test_ws_stream_url_multiplexed() -> None:
    """组合流 URL：4 币种 × trade + kline_1m，小写币种。"""
    adapter = BinanceWSAdapter(
        ["BTCUSDT", "ETHUSDT"], ws_url="wss://data-stream.binance.vision/stream"
    )
    url = adapter.stream_url()
    assert url.startswith("wss://data-stream.binance.vision/stream?streams=")
    assert "btcusdt@trade" in url
    assert "btcusdt@kline_1m" in url
    assert "ethusdt@trade" in url


def test_ws_backoff_sequence_capped_at_30() -> None:
    """持续连接失败：退避序列 1→2→4→8→16→30→30（封顶）。"""

    async def _run() -> list[float]:
        recorded, fake_sleep = _sleep_recorder(limit=7)
        connector = QaFakeConnector([ConnectionError("模拟失败")] * 10)
        adapter = BinanceWSAdapter(
            ["BTCUSDT"],
            ws_url="wss://data-stream.binance.vision/stream",
            connector=connector,
            sleep_func=fake_sleep,
        )
        try:
            async for _ in adapter.events():
                pass
        except QaStopLoop:
            pass
        return recorded

    recorded = asyncio.run(_run())
    assert recorded == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


def test_ws_idle_watchdog_triggers_reconnect() -> None:
    """60s 级空闲看门狗（测试缩小阈值）：超时无消息 → 关闭旧连接并重连。"""

    async def _run() -> tuple:
        first = QaFakeWS([], on_exhausted="block")
        second = QaFakeWS([], on_exhausted="block")
        recorded, fake_sleep = _sleep_recorder(limit=2)
        connector = QaFakeConnector([first, second])
        adapter = BinanceWSAdapter(
            ["BTCUSDT"],
            ws_url="wss://data-stream.binance.vision/stream",
            connector=connector,
            idle_timeout_sec=0.05,
            sleep_func=fake_sleep,
        )
        try:
            async for _ in adapter.events():
                pass
        except QaStopLoop:
            pass
        return recorded, connector.calls, first.closed, adapter.status()

    recorded, calls, first_closed, status = asyncio.run(_run())
    assert recorded == [1.0, 1.0]  # 两次重连均从初始 1s 退避
    assert calls == 2
    assert first_closed is True
    assert "空闲超时" in status.detail


def test_ws_gap_repair_start_time_and_limit() -> None:
    """重连后 gap 修复：startTime=最后收盘开盘+1m，limit=5，先补洞后实时流。"""

    class QaRest:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def klines(self, symbol, interval="1m", limit=300, start_ms=None, end_ms=None):
            self.calls.append({"symbol": symbol, "start_ms": start_ms, "limit": limit})
            return [
                make_candle(symbol, index=1),
                make_candle(symbol, index=2),
                make_candle(symbol, index=3),
            ]

    last_open = BASE_OPEN_MS
    live_open = BASE_OPEN_MS + 5 * MS_PER_MINUTE
    rest = QaRest()
    # 第一次连接：先给一根已收盘 K 线（记录 last_kline_open），随后连接中断
    first = QaFakeWS([_kline_frame("BTCUSDT", last_open, 100.0, True)], on_exhausted="raise")
    # 第二次连接：实时流（更晚的已收盘 K 线）
    second = QaFakeWS(
        [_kline_frame("BTCUSDT", live_open, 100.0, True)], on_exhausted="block"
    )
    recorded, fake_sleep = _sleep_recorder(limit=2)
    connector = QaFakeConnector([first, second])
    adapter = BinanceWSAdapter(
        ["BTCUSDT"],
        ws_url="wss://data-stream.binance.vision/stream",
        connector=connector,
        idle_timeout_sec=5.0,
        rest_client=rest,
        sleep_func=fake_sleep,
        now_func=lambda: live_open + 10 * MS_PER_MINUTE,
    )

    async def _run() -> list[Candle]:
        events: list[Candle] = []
        try:
            async for event in adapter.events():
                events.append(event)
                if len(events) >= 5:
                    break
        except QaStopLoop:
            pass
        return events

    events = asyncio.run(_run())
    closed = [e for e in events if isinstance(e, Candle) and e.is_closed]
    # 首连接实时 1 根（index 0）+ 补洞 3 根（index 1/2/3）+ 二次连接实时 1 根（index 5）
    assert [c.open_time_ms for c in closed] == [
        BASE_OPEN_MS,
        BASE_OPEN_MS + 1 * MS_PER_MINUTE,
        BASE_OPEN_MS + 2 * MS_PER_MINUTE,
        BASE_OPEN_MS + 3 * MS_PER_MINUTE,
        live_open,
    ]
    # REST 调用参数：startTime = 最后收盘开盘 + 1m，limit=5
    assert rest.calls[0]["start_ms"] == last_open + MS_PER_MINUTE
    assert rest.calls[0]["limit"] == 5
    assert rest.calls[0]["symbol"] == "BTCUSDT"


def test_ws_gap_repair_dedup_no_repeat() -> None:
    """gap 补齐的 K 线若实时流重复出现，去重不重复 yield。"""

    class QaRest:
        async def klines(self, symbol, interval="1m", limit=300, start_ms=None, end_ms=None):
            # 补洞返回 index=2（与稍后实时流重复）
            return [make_candle(symbol, index=2)]

    dup_open = BASE_OPEN_MS + 2 * MS_PER_MINUTE
    first = QaFakeWS([_kline_frame("BTCUSDT", BASE_OPEN_MS, 100.0, True)], on_exhausted="raise")
    second = QaFakeWS(
        [
            _kline_frame("BTCUSDT", dup_open, 100.0, True),  # 与补洞重复 → 应被跳过
            _kline_frame("BTCUSDT", dup_open + MS_PER_MINUTE, 100.0, True),
        ],
        on_exhausted="block",
    )
    recorded, fake_sleep = _sleep_recorder(limit=2)
    adapter = BinanceWSAdapter(
        ["BTCUSDT"],
        ws_url="wss://data-stream.binance.vision/stream",
        connector=QaFakeConnector([first, second]),
        idle_timeout_sec=5.0,
        rest_client=QaRest(),
        sleep_func=fake_sleep,
        now_func=lambda: dup_open + 10 * MS_PER_MINUTE,
    )

    async def _run() -> list[int]:
        opens: list[int] = []
        try:
            async for event in adapter.events():
                if isinstance(event, Candle) and event.is_closed:
                    opens.append(event.open_time_ms)
                    if len(opens) >= 3:
                        break
        except QaStopLoop:
            pass
        return opens

    opens = asyncio.run(_run())
    # 补洞 1 根 + 实时 2 根（重复的那根被去重）
    assert opens == [
        BASE_OPEN_MS,
        dup_open,
        dup_open + MS_PER_MINUTE,
    ]


def test_ws_unclosed_kline_yielded_but_not_deduped() -> None:
    """未收盘 K 线正常 yield（供展示），不进入去重/补洞记账。"""
    frame = _kline_frame("BTCUSDT", BASE_OPEN_MS, 100.0, False)
    ws = QaFakeWS([frame, _trade_frame("BTCUSDT", 101.0, BASE_OPEN_MS + 500)], on_exhausted="block")
    adapter = BinanceWSAdapter(
        ["BTCUSDT"],
        ws_url="wss://data-stream.binance.vision/stream",
        connector=QaFakeConnector([ws]),
        idle_timeout_sec=5.0,
    )

    async def _run() -> list:
        events = []
        try:
            async for event in adapter.events():
                events.append(event)
                if len(events) >= 2:
                    break
        except Exception:  # noqa: BLE001 - 外层断开即收敛
            pass
        await adapter.close()
        return events

    events = asyncio.run(_run())
    assert len(events) == 2
    assert isinstance(events[0], Candle) and events[0].is_closed is False
    assert isinstance(events[1], Tick)
    assert events[1].price == 101.0
    assert events[1].is_buyer_maker is True


def test_ws_events_never_raises_on_invalid_frames() -> None:
    """非法帧/未知事件被跳过，events() 不抛异常。"""
    frames = [
        "not-json{{",
        json.dumps({"stream": "x", "data": {"e": "unknownEvent"}}),
        json.dumps([1, 2, 3]),  # 非对象信封
        json.dumps({"stream": "x"}),  # 缺 data
        _trade_frame("BTCUSDT", 99.5, BASE_OPEN_MS + 100),
    ]
    ws = QaFakeWS(frames, on_exhausted="block")
    adapter = BinanceWSAdapter(
        ["BTCUSDT"],
        ws_url="wss://data-stream.binance.vision/stream",
        connector=QaFakeConnector([ws]),
        idle_timeout_sec=5.0,
    )

    async def _run() -> list:
        events = []
        async for event in adapter.events():
            events.append(event)
            break
        await adapter.close()
        return events

    events = asyncio.run(_run())
    assert len(events) == 1
    assert isinstance(events[0], Tick)
    assert events[0].price == 99.5


# ---------------------------------------------------------------------------
# F. 指标与 pandas-ta 参考实现一致性 + RingBuffer 边界
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine() -> IndicatorEngine:
    return IndicatorEngine()


def _reference_indicators(closes: list[float], volumes: list[float]) -> dict:
    """pandas-ta 参考实现（与引擎调用方式一致的独立复算）。"""
    frame = pd.DataFrame({"close": closes, "volume": volumes}, dtype="float64")
    return {
        "rsi": float(frame.ta.rsi(length=14).iloc[-1]),
        "ma_short": float(frame.ta.sma(length=5).iloc[-1]),
        "ma_long": float(frame.ta.sma(length=25).iloc[-1]),
        "vol_ma": float(frame["volume"].rolling(20).mean().iloc[-1]),
    }


def test_indicators_match_pandas_ta_reference_within_1e_9(engine: IndicatorEngine) -> None:
    """RSI/MA/量均与 pandas-ta 参考实现误差 < 1e-9（P0-3）。"""
    closes = [100.0 + (i % 7) * 0.5 - (i % 3) * 0.25 for i in range(60)]
    volumes = [10.0 + (i % 5) for i in range(60)]
    candles = [
        make_candle(index=i, close=closes[i], volume=volumes[i]) for i in range(60)
    ]
    snapshot = None
    for candle in candles:
        snapshot = engine.update("BTCUSDT", candle)
    assert snapshot is not None
    reference = _reference_indicators(closes, volumes)
    assert snapshot.rsi == pytest.approx(reference["rsi"], abs=1e-9)
    assert snapshot.ma_short == pytest.approx(reference["ma_short"], abs=1e-9)
    assert snapshot.ma_long == pytest.approx(reference["ma_long"], abs=1e-9)
    assert snapshot.vol_ma == pytest.approx(reference["vol_ma"], abs=1e-9)


def test_indicators_warmup_matches_reference(engine: IndicatorEngine) -> None:
    """预热路径同样与参考实现一致（冷启动即有效值）。"""
    closes = [50.0 + i * 0.8 for i in range(40)]
    volumes = [5.0 + (i % 4) * 2.0 for i in range(40)]
    engine.warmup(
        "ETHUSDT", [make_candle("ETHUSDT", i, closes[i], volumes[i]) for i in range(40)]
    )
    snapshot = engine.latest("ETHUSDT")
    assert snapshot is not None
    reference = _reference_indicators(closes, volumes)
    assert snapshot.rsi == pytest.approx(reference["rsi"], abs=1e-9)
    assert snapshot.ma_short == pytest.approx(reference["ma_short"], abs=1e-9)


def test_indicators_insufficient_window_returns_none(engine: IndicatorEngine) -> None:
    """窗口不足所需最长周期（25）时返回 None。"""
    for i in range(24):
        assert engine.update("BTCUSDT", make_candle(index=i)) is None
    snapshot = engine.update("BTCUSDT", make_candle(index=24))
    assert snapshot is not None


def test_indicators_ignores_unclosed(engine: IndicatorEngine) -> None:
    """未收盘 K 线不入窗口。"""
    for i in range(30):
        assert engine.update("BTCUSDT", make_candle(index=i, is_closed=False)) is None
    assert engine.latest("BTCUSDT") is None
    assert engine.windows.get("BTCUSDT") is None  # 未创建窗口


def test_ring_buffer_boundary_eviction_order() -> None:
    """RingBuffer 边界：超容量淘汰最旧，to_list 为浅拷贝。"""
    buffer: RingBuffer[int] = RingBuffer(maxlen=3)
    for value in (1, 2, 3, 4, 5):
        buffer.append(value)
    assert buffer.to_list() == [3, 4, 5]
    assert len(buffer) == 3
    snapshot = buffer.to_list()
    snapshot.append(99)
    assert buffer.to_list() == [3, 4, 5]  # 快照不影响缓冲
    buffer.clear()
    assert len(buffer) == 0
    assert buffer.to_list() == []


def test_ring_buffer_invalid_maxlen() -> None:
    """maxlen 非正数抛 ValueError。"""
    with pytest.raises(ValueError):
        RingBuffer(maxlen=0)
    with pytest.raises(ValueError):
        RingBuffer(maxlen=-1)


def test_ring_buffer_single_capacity() -> None:
    """容量 1 时只保留最新元素。"""
    buffer: RingBuffer[str] = RingBuffer(maxlen=1)
    buffer.append("a")
    buffer.append("b")
    assert buffer.to_list() == ["b"]


# ---------------------------------------------------------------------------
# G. 规则边界语义 + 异常隔离 + 配置开关
# ---------------------------------------------------------------------------


def _context(
    candle: Candle, window: list[Candle], snapshot: IndicatorSnapshot, prev=None
) -> RuleContext:
    return RuleContext(
        symbol=candle.symbol,
        candle=candle,
        window=window,
        indicators=snapshot,
        prev_indicators=prev,
    )


def _snapshot(**overrides) -> IndicatorSnapshot:
    base = dict(
        symbol="BTCUSDT",
        close=100.0,
        rsi=50.0,
        ma_short=100.0,
        ma_long=100.0,
        vol_ma=10.0,
        volume=10.0,
        ts_ms=BASE_CLOSE_MS,
    )
    base.update(overrides)
    return IndicatorSnapshot(**base)


def test_price_move_boundary_at_threshold_triggers() -> None:
    """价格异动边界：涨跌幅恰好等于阈值即触发（>= 语义）。"""
    rule = PriceMoveRule(window_min=5, threshold_pct=5.0)
    window = make_window(6)
    window[0] = make_candle(index=0, close=100.0)
    current = make_candle(index=5, close=105.0)  # 恰好 +5%
    result = rule.evaluate(_context(current, window, _snapshot()))
    assert result.triggered is True
    assert result.threshold == 5.0
    assert result.trigger_value == pytest.approx(5.0, abs=1e-9)


def test_price_move_below_threshold_not_triggered() -> None:
    """价格异动：低于阈值不触发。"""
    rule = PriceMoveRule(window_min=5, threshold_pct=5.0)
    window = make_window(6)
    current = make_candle(index=5, close=104.99)
    result = rule.evaluate(_context(current, window, _snapshot()))
    assert result.triggered is False


def test_price_move_insufficient_window() -> None:
    """价格异动：窗口不足 window_min+1 根时不触发。"""
    rule = PriceMoveRule(window_min=5)
    window = make_window(5)  # 需要 6 根
    result = rule.evaluate(_context(window[-1], window, _snapshot()))
    assert result.triggered is False


def test_volume_spike_strictly_greater_than_multiplier() -> None:
    """量突增边界：恰好等于倍数不触发（严格 >）。"""
    rule = VolumeSpikeRule(multiplier=3.0)
    snapshot = _snapshot(vol_ma=10.0)
    # 恰好 3 倍 → 不触发
    result = rule.evaluate(_context(make_candle(volume=30.0), make_window(5), snapshot))
    assert result.triggered is False
    # 3.01 倍 → 触发
    result = rule.evaluate(_context(make_candle(volume=30.1), make_window(5), snapshot))
    assert result.triggered is True
    assert result.trigger_value == pytest.approx(3.01, abs=1e-9)


def test_volume_spike_without_baseline_not_triggered() -> None:
    """量突增：无量均基线不触发。"""
    rule = VolumeSpikeRule()
    result = rule.evaluate(_context(make_candle(volume=100.0), make_window(5), _snapshot(vol_ma=None)))
    assert result.triggered is False


def test_rsi_boundary_overbought_oversold() -> None:
    """RSI 边界：恰好等于超买/超卖阈值均触发（>=/<= 语义）。"""
    rule = RsiRule(overbought=70.0, oversold=30.0)
    result = rule.evaluate(_context(make_candle(), make_window(5), _snapshot(rsi=70.0)))
    assert result.triggered is True
    assert "超买" in result.message
    result = rule.evaluate(_context(make_candle(), make_window(5), _snapshot(rsi=30.0)))
    assert result.triggered is True
    assert "超卖" in result.message
    result = rule.evaluate(_context(make_candle(), make_window(5), _snapshot(rsi=30.01)))
    assert result.triggered is False


def test_ma_cross_golden_and_death() -> None:
    """均线交叉：金叉（短上穿长）与死叉（短下穿长）触发，平行不触发。"""
    rule = MaCrossRule(ma_short=5, ma_long=25)
    prev = _snapshot(ma_short=99.0, ma_long=100.0)
    current = _snapshot(ma_short=101.0, ma_long=100.0)
    result = rule.evaluate(_context(make_candle(), make_window(5), current, prev))
    assert result.triggered is True
    assert "金叉" in result.message

    prev = _snapshot(ma_short=101.0, ma_long=100.0)
    current = _snapshot(ma_short=99.0, ma_long=100.0)
    result = rule.evaluate(_context(make_candle(), make_window(5), current, prev))
    assert result.triggered is True
    assert "死叉" in result.message

    prev = _snapshot(ma_short=101.0, ma_long=100.0)
    current = _snapshot(ma_short=102.0, ma_long=100.0)
    result = rule.evaluate(_context(make_candle(), make_window(5), current, prev))
    assert result.triggered is False


def test_ma_cross_uses_snapshot_prev_fields_when_no_prev_ctx() -> None:
    """无 ctx.prev_indicators 时回退到快照自带 prev_ma_* 字段。"""
    rule = MaCrossRule()
    current = _snapshot(ma_short=101.0, ma_long=100.0, prev_ma_short=99.0, prev_ma_long=100.0)
    result = rule.evaluate(_context(make_candle(), make_window(5), current, None))
    assert result.triggered is True
    assert "金叉" in result.message


def test_rule_engine_single_rule_exception_isolated() -> None:
    """单规则评估抛异常不中断扫描，其余规则照常产出。"""

    class ExplodingRule(Rule):
        name = "exploding"
        level = "warning"

        def __init__(self) -> None:
            self.enabled = True

        def evaluate(self, ctx: RuleContext):
            raise RuntimeError("模拟规则内部错误")

    class AlwaysRule(Rule):
        name = "always"
        level = "info"

        def __init__(self) -> None:
            self.enabled = True

        def evaluate(self, ctx: RuleContext):
            from quant.models import RuleResult

            return RuleResult(triggered=True, message="永远触发", trigger_value=1.0, threshold=0.0)

        def reset(self) -> None:
            pass

    exploding = ExplodingRule()
    exploding.evaluate = exploding.evaluate  # 显式引用避免 lint 误判
    engine_obj = RuleEngine(rules=[exploding, AlwaysRule()])
    alerts = engine_obj.scan(_context(make_candle(), make_window(30), _snapshot()))
    assert len(alerts) == 1
    assert alerts[0].rule == "always"
    assert alerts[0].symbol == "BTCUSDT"
    # Alert.ts 取 candle.close_time_ms 对应 UTC 时间
    assert alerts[0].ts == ms_to_datetime(BASE_CLOSE_MS)


def test_rule_engine_set_enabled_and_names() -> None:
    """引擎 set_enabled 独立开关；禁用规则零产出。"""
    engine_obj = RuleEngine(rules=build_rules_from_defaults())
    assert engine_obj.rule_names() == ["price_move", "volume_spike", "rsi", "ma_cross"]
    assert engine_obj.set_enabled("price_move", False) is True
    assert engine_obj.set_enabled("nonexistent", False) is False
    # 禁用 price_move 后，价格异动场景不再产出该规则告警
    window = make_window(6)
    current = make_candle(index=5, close=110.0)
    alerts = engine_obj.scan(_context(current, window, _snapshot()))
    assert all(a.rule != "price_move" for a in alerts)


def build_rules_from_defaults() -> list[Rule]:
    """按默认配置构造 4 条规则（验证配置驱动开关）。"""
    from quant.config import Settings
    from quant.scanner.rules import build_default_rules

    settings = Settings()
    # 配置独立开关：关掉 rsi 规则
    settings.rules.rsi.enabled = False
    rules = build_default_rules(settings)
    by_name = {rule.name: rule for rule in rules}
    assert by_name["rsi"].enabled is False
    assert by_name["price_move"].enabled is True
    assert by_name["price_move"].threshold_pct == 5.0
    assert by_name["volume_spike"].multiplier == 3.0
    return rules


# ---------------------------------------------------------------------------
# H. 告警冷却去重 / JSONL / CSV / JSON / SQLite 过滤
# ---------------------------------------------------------------------------


def _alert(symbol: str = "BTCUSDT", rule: str = "price_move", ts_ms: int = BASE_CLOSE_MS) -> Alert:
    return Alert(
        ts=ms_to_datetime(ts_ms),
        symbol=symbol,
        rule=rule,
        message="测试告警",
        trigger_value=5.0,
        threshold=5.0,
        level="warning",
    )


def test_alert_cooldown_dedup_default_300s(tmp_path: Path) -> None:
    """冷却去重：默认 300s 内同币种同规则丢弃；跨币种/跨规则独立。"""
    manager = AlertManager(
        cooldown_sec=300.0,
        log_path=tmp_path / "alerts.jsonl",
        clock=_FakeClock(1000.0).now,
    )
    assert manager.publish(_alert()) is True  # t=1000
    assert manager.publish(_alert()) is False  # t=1000（冷却中）
    assert manager.publish(_alert(rule="rsi")) is True  # 同币种不同规则
    assert manager.publish(_alert(symbol="ETHUSDT")) is True  # 不同币种
    manager._clock = _FakeClock(1000.0 + 300.0).now
    assert manager.publish(_alert()) is True  # 恰好 300s 后放行
    assert manager.publish(_alert()) is False  # 再次冷却


class _FakeClock:
    def __init__(self, start: float) -> None:
        self.current = start

    def now(self) -> float:
        return self.current


def test_alert_zero_cooldown_never_dedups(tmp_path: Path) -> None:
    """cooldown_sec=0 表示不冷却。"""
    manager = AlertManager(cooldown_sec=0.0, log_path=tmp_path / "a.jsonl")
    assert manager.publish(_alert()) is True
    assert manager.publish(_alert()) is True


def test_alert_jsonl_persisted(tmp_path: Path) -> None:
    """JSONL 落盘：每行一条、字段完整、ISO 时间。"""
    log_path = tmp_path / "nested" / "alerts.jsonl"
    manager = AlertManager(cooldown_sec=0.0, log_path=log_path)
    manager.publish(_alert())
    manager.publish(_alert(symbol="ETHUSDT", ts_ms=BASE_CLOSE_MS + MS_PER_MINUTE))
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    row = json.loads(lines[0])
    assert row["symbol"] == "BTCUSDT"
    assert row["rule"] == "price_move"
    assert row["trigger_value"] == 5.0
    assert row["threshold"] == 5.0
    assert row["level"] == "warning"
    assert row["ts"].endswith("Z")
    assert row["ts"] == "2025-10-24T06:32:59.999Z"[: len(row["ts"])]


def test_alert_export_csv_and_json_roundtrip(tmp_path: Path) -> None:
    """CSV/JSON 导出：条数正确、内容可解析（P0-6）。"""
    log_path = tmp_path / "alerts.jsonl"
    manager = AlertManager(cooldown_sec=0.0, log_path=log_path)
    for i in range(3):
        manager.publish(_alert(ts_ms=BASE_CLOSE_MS + i * MS_PER_MINUTE))

    csv_path = tmp_path / "out" / "alerts.csv"
    json_path = tmp_path / "out" / "alerts.json"
    assert manager.export_csv(csv_path) == 3
    assert manager.export_json(json_path) == 3

    with csv_path.open(encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 3
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["rule"] == "price_move"

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert isinstance(payload, list) and len(payload) == 3
    assert payload[0]["trigger_value"] == 5.0


def test_alert_manager_persists_to_sqlite(tmp_path: Path) -> None:
    """告警经 AlertManager 写入 SQLite（存储不可用时降级不抛异常）。"""
    db_path = tmp_path / "quant.db"
    store = Storage(db_path)
    store.init_schema()
    manager = AlertManager(cooldown_sec=0.0, log_path=tmp_path / "alerts.jsonl", store=store)
    manager.publish(_alert())
    manager.publish(_alert(symbol="ETHUSDT", ts_ms=BASE_CLOSE_MS + MS_PER_MINUTE))
    rows = store.query_alerts()
    assert len(rows) == 2
    assert rows[0].symbol == "BTCUSDT"
    store.close()


def test_alert_store_failure_degrades_to_jsonl(tmp_path: Path) -> None:
    """SQLite 写入失败时降级：publish 仍成功，JSONL 已落盘。"""

    class BrokenStore:
        def save_alert(self, alert: Alert) -> None:
            raise RuntimeError("模拟存储故障")

        def query_alerts(self, **kwargs):
            raise RuntimeError("模拟存储故障")

    manager = AlertManager(
        cooldown_sec=0.0,
        log_path=tmp_path / "alerts.jsonl",
        store=BrokenStore(),
    )
    assert manager.publish(_alert()) is True
    assert (tmp_path / "alerts.jsonl").is_file()
    # 导出回退 JSONL 仍可用
    csv_path = tmp_path / "alerts.csv"
    assert manager.export_csv(csv_path) == 1


# ---------------------------------------------------------------------------
# I. 存储边界：upsert 幂等 + 双连接并发
# ---------------------------------------------------------------------------


def test_storage_upsert_idempotent(tmp_path: Path) -> None:
    """重复 save_candles 同一 (symbol, open_time_ms) 不产生重复行（T02 验收）。"""
    db_path = tmp_path / "quant.db"
    store = Storage(db_path)
    store.init_schema()
    candles = [make_candle(index=i) for i in range(5)]
    store.save_candles("BTCUSDT", candles)
    store.save_candles("BTCUSDT", candles)  # 重复写入
    store.save_candles("BTCUSDT", [make_candle(index=0, close=200.0)])  # upsert 更新
    count = store.conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    assert count == 5
    loaded = store.load_candles("BTCUSDT", limit=10)
    assert len(loaded) == 5
    assert loaded[0].open_time_ms == BASE_OPEN_MS  # 升序返回
    assert loaded[0].close == 200.0  # upsert 生效
    store.close()


def test_storage_query_alerts_filters(tmp_path: Path) -> None:
    """query_alerts 按 symbol/rule/时间过滤（T02 验收）。"""
    db_path = tmp_path / "quant.db"
    store = Storage(db_path)
    store.init_schema()
    t0 = BASE_CLOSE_MS
    store.save_alert(_alert(rule="price_move", ts_ms=t0))
    store.save_alert(_alert(rule="rsi", ts_ms=t0 + MS_PER_MINUTE))
    store.save_alert(_alert(symbol="ETHUSDT", rule="rsi", ts_ms=t0 + 2 * MS_PER_MINUTE))

    assert len(store.query_alerts(symbol="BTCUSDT")) == 2
    assert len(store.query_alerts(rule="rsi")) == 2
    assert len(store.query_alerts(symbol="ETHUSDT", rule="rsi")) == 1
    # 时间范围（含边界，naive 按 UTC）
    start = ms_to_datetime(t0 + MS_PER_MINUTE)
    end = ms_to_datetime(t0 + MS_PER_MINUTE)
    rows = store.query_alerts(start=start, end=end)
    assert len(rows) == 1
    assert rows[0].rule == "rsi"
    # 无过滤全量
    assert len(store.query_alerts()) == 3
    store.close()


def test_storage_two_connections_concurrent_writes(tmp_path: Path) -> None:
    """双连接并发边界：两个 Storage 实例交替写不互相阻塞/不损坏。"""
    db_path = tmp_path / "shared.db"
    first = Storage(db_path)
    first.init_schema()
    second = Storage(db_path)
    second.init_schema()
    for i in range(10):
        if i % 2 == 0:
            first.save_candles("BTCUSDT", [make_candle(index=i)])
        else:
            second.save_candles("BTCUSDT", [make_candle(index=i)])
    rows = first.conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    assert rows == 10
    # 后开连接可见先开连接的写入
    assert len(second.load_candles("BTCUSDT", limit=100)) == 10
    first.close()
    second.close()


def test_storage_load_candles_invalid_limit(tmp_path: Path) -> None:
    """load_candles limit 非正整数抛 ValueError。"""
    store = Storage(tmp_path / "quant.db")
    store.init_schema()
    with pytest.raises(ValueError):
        store.load_candles("BTCUSDT", limit=0)
    store.close()


def test_storage_context_manager(tmp_path: Path) -> None:
    """Storage 上下文管理器：进入建表、退出关闭。"""
    db_path = tmp_path / "quant.db"
    with Storage(db_path) as store:
        store.save_alert(_alert())
        assert len(store.query_alerts()) == 1
    with pytest.raises(Exception):  # sqlite3.ProgrammingError: 连接已关闭
        store.conn.execute("SELECT 1")


# ---------------------------------------------------------------------------
# J. DashboardState 浅拷贝语义 + 渲染解耦
# ---------------------------------------------------------------------------


def test_snapshot_shallow_copy_semantics() -> None:
    """snapshot() 浅拷贝：原状态后续改动不影响快照。"""
    state = DashboardState(symbols=["BTCUSDT"])
    state.update_tick(Tick("BTCUSDT", 100.0, 1.0, BASE_OPEN_MS, True, "binance_ws"))
    state.rules_enabled = {"price_move": True}
    snapshot = state.snapshot()

    # 快照后修改原状态
    state.update_tick(Tick("BTCUSDT", 200.0, 1.0, BASE_OPEN_MS + 1, True, "binance_ws"))
    state.push_alert(_alert())
    state.rules_enabled["price_move"] = False
    state.paused = True
    state.set_feed_status(FeedStatus(connected=True, reconnect_count=5))

    # 快照保持不变
    assert snapshot.symbols["BTCUSDT"].last_price == 100.0
    assert snapshot.alerts == []
    assert snapshot.rules_enabled == {"price_move": True}
    assert snapshot.paused is False
    assert snapshot.feed_status.connected is False
    # 快照内部容器类型：deque→list、dict→copy
    assert isinstance(snapshot.alerts, list)
    assert snapshot.symbols is not state.symbols


def test_render_loop_decoupled_from_data_flow() -> None:
    """渲染循环与数据流解耦：渲染任务只读快照，stop_event 即停。"""
    state = DashboardState(symbols=["BTCUSDT"])
    renderer = DashboardRenderer(refresh_interval_sec=0.01)

    async def _run() -> float:
        stop = asyncio.Event()
        task = asyncio.create_task(renderer.render_loop(state, stop))
        await asyncio.sleep(0.05)
        # 渲染运行期间数据流持续写入（不互相阻塞）
        state.update_tick(Tick("BTCUSDT", 123.0, 1.0, BASE_OPEN_MS, True, "binance_ws"))
        await asyncio.sleep(0.02)
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)
        return state.snapshot().symbols["BTCUSDT"].last_price

    price = asyncio.run(_run())
    assert price == 123.0


def test_renderer_builds_all_four_regions() -> None:
    """看板四区：状态栏/行情表/告警日志/规则面板均可构建。"""
    state = DashboardState(symbols=["BTCUSDT"])
    state.set_feed_status(FeedStatus(connected=True, reconnect_count=1))
    state.rules_enabled = {"price_move": True, "rsi": False}
    state.push_alert(_alert())
    state.rule_panel_open = True
    renderer = DashboardRenderer(refresh_interval_sec=0.5)
    renderable = renderer.build_renderable(state.snapshot())
    from rich.console import Group

    assert isinstance(renderable, Group)
    assert len(renderable.renderables) == 4  # 含规则面板
    # 关闭规则面板时仅三区
    state.rule_panel_open = False
    renderable = renderer.build_renderable(state.snapshot())
    assert len(renderable.renderables) == 3


# ---------------------------------------------------------------------------
# K. 端到端最小链路（自建 fake adapter）
# ---------------------------------------------------------------------------


class QaScriptedAdapter:
    """脚本化行情适配器：按序 yield 预设事件后结束。"""

    name = "qa_scripted"

    def __init__(self, events: list) -> None:
        self._events = list(events)
        self.status_calls = 0

    async def connect(self) -> None:
        pass

    async def events(self):
        for event in self._events:
            yield event

    async def close(self) -> None:
        pass

    def status(self) -> FeedStatus:
        self.status_calls += 1
        return FeedStatus(connected=True, reconnect_count=0, detail="脚本")


def test_end_to_end_minimal_pipeline(tmp_path: Path) -> None:
    """端到端：预热+实时收盘 K 线 → 指标 → 规则 → 告警 → JSONL+SQLite。"""
    from quant.app import QuantApp

    settings = Settings()
    settings.network.proxy = "http://127.0.0.1:63571"
    settings.market.symbols = ["BTCUSDT"]
    settings.alerts.cooldown_sec = 300.0
    settings.alerts.log_path = str(tmp_path / "alerts.jsonl")
    settings.storage.db_path = str(tmp_path / "quant.db")

    # 预热 40 根平稳 K 线（RSI≈中位、均量 10），实时 K 线放量拉升触发 price_move
    warmup = [make_candle(index=i, close=100.0, volume=10.0) for i in range(40)]
    live_tick = Tick("BTCUSDT", 100.0, 1.0, BASE_OPEN_MS + 40 * MS_PER_MINUTE, False, "qa")
    live_candle = make_candle(index=40, close=112.0, volume=50.0)  # 5min +12% → 触发

    adapter = QaScriptedAdapter([live_tick, live_candle])

    class QaRest:
        async def klines(self, symbol, interval="1m", limit=300, start_ms=None, end_ms=None):
            return list(warmup)

        async def aclose(self) -> None:
            pass

    app = QuantApp(
        settings,
        adapter=adapter,
        rest_client=QaRest(),
        storage=Storage(settings.storage.db_path),
        renderer=DashboardRenderer(refresh_interval_sec=0.01),
    )
    app.storage.init_schema()

    async def _run() -> None:
        await app._warmup()  # 启动预热：灌入 40 根历史 K 线
        stop = asyncio.Event()
        feed_task = asyncio.create_task(app._feed_consumer())
        render_task = asyncio.create_task(app._render_loop())
        await asyncio.sleep(0.3)
        stop.set()
        for task in (feed_task, render_task):
            task.cancel()
        await asyncio.gather(feed_task, render_task, return_exceptions=True)

    asyncio.run(_run())

    # 告警落盘（JSONL + SQLite）：该场景同时触发 4 条规则（价涨 12% / 放量 5x / RSI 极值 / 金叉）
    log_rows = (tmp_path / "alerts.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(log_rows) == 4
    rules_fired = [json.loads(line)["rule"] for line in log_rows]
    assert rules_fired == ["price_move", "volume_spike", "rsi", "ma_cross"]
    row = json.loads(log_rows[0])
    assert row["symbol"] == "BTCUSDT"
    assert row["trigger_value"] == pytest.approx(12.0, abs=0.01)

    stored = app.storage.query_alerts(symbol="BTCUSDT")
    assert len(stored) == 4
    price_alert = [a for a in stored if a.rule == "price_move"][0]
    assert price_alert.threshold == 5.0

    # 看板状态：最新价来自实时 K 线，信号列被最后一条告警（ma_cross）更新
    view = app.state.snapshot().symbols["BTCUSDT"]
    assert view.last_price == 112.0
    assert view.last_signal == "ma_cross"
    app.storage.close()


def test_end_to_end_pause_stops_scanning_but_not_data(tmp_path: Path) -> None:
    """暂停：停止扫描产出告警，但数据/指标仍更新。"""
    from quant.app import QuantApp

    settings = Settings()
    settings.network.proxy = "http://127.0.0.1:63571"
    settings.market.symbols = ["BTCUSDT"]
    settings.alerts.log_path = str(tmp_path / "alerts.jsonl")
    settings.storage.db_path = str(tmp_path / "quant.db")

    warmup = [make_candle(index=i, close=100.0, volume=10.0) for i in range(40)]
    live_candle = make_candle(index=40, close=112.0, volume=50.0)
    adapter = QaScriptedAdapter([live_candle])

    class QaRest:
        async def klines(self, symbol, interval="1m", limit=300, start_ms=None, end_ms=None):
            return list(warmup)

        async def aclose(self) -> None:
            pass

    app = QuantApp(
        settings,
        adapter=adapter,
        rest_client=QaRest(),
        storage=Storage(settings.storage.db_path),
    )
    app.storage.init_schema()
    app.state.paused = True  # 启动前暂停

    async def _run() -> None:
        await app._warmup()
        task = asyncio.create_task(app._feed_consumer())
        await asyncio.sleep(0.2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(_run())
    # 无告警产出
    assert not (tmp_path / "alerts.jsonl").exists()
    # 指标与展示仍更新
    assert app.state.symbols["BTCUSDT"].last_price == 112.0
    assert app.indicators.latest("BTCUSDT") is not None
    app.storage.close()
