"""pytest 公共 fixtures 与辅助函数。

约定：
- 所有测试禁网（外部 IO 全部 mock/录制回放）；
- 时间相关测试注入固定 epoch ms，不依赖 ``datetime.now()``；
- 异步测试沿用 ``asyncio.run()`` 同步外裹模式（不引 pytest-asyncio）。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from quant.config import Settings, load_settings

#: 仓库根目录（tests/ 的上一级）
REPO_ROOT = Path(__file__).resolve().parents[1]

#: 测试资产目录（WS 录制帧）
RECORDINGS_DIR = Path(__file__).resolve().parent / "recordings"

#: 测试用最小配置（路径均为相对路径，基于配置文件目录的上级解析）
MINIMAL_CONFIG = """\
[network]
proxy = "http://127.0.0.1:63571"
ws_url = "wss://data-stream.binance.vision/stream"
rest_url = "https://data-api.binance.vision"

[market]
symbols = ["BTCUSDT", "ETHUSDT"]

[indicators]
candle_window = 300
rsi_length = 14
ma_short = 5
ma_long = 25
vol_ma_length = 20

[rules.price_move]
enabled = true
window_min = 5
threshold_pct = 5.0

[rules.volume_spike]
enabled = true
multiplier = 3.0

[rules.rsi]
enabled = true
overbought = 70.0
oversold = 30.0

[rules.ma_cross]
enabled = true

[alerts]
cooldown_sec = 300.0
log_path = "data/alerts.jsonl"

[storage]
db_path = "data/quant.db"

[cli]
refresh_interval_sec = 0.5
"""


@pytest.fixture()
def tmp_config_path(tmp_path: Path) -> Path:
    """将最小配置写入 ``tmp_path/config/config.toml`` 并返回路径。

    放置于 ``config/`` 子目录是为了让「相对路径基于配置文件目录的上级」
    解析后落在 ``tmp_path`` 内，避免污染仓库 ``data/`` 目录。
    """
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "config.toml"
    config_path.write_text(MINIMAL_CONFIG, encoding="utf-8")
    return config_path


@pytest.fixture()
def tmp_settings(tmp_config_path: Path) -> Settings:
    """基于最小配置加载并校验后的 Settings（路径已解析到 tmp_path 内）。"""
    return load_settings(tmp_config_path)


def write_config(tmp_path: Path, content: str) -> Path:
    """将自定义配置内容写入 ``tmp_path/config/config.toml`` 并返回路径。"""
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "config.toml"
    config_path.write_text(content, encoding="utf-8")
    return config_path


#: WS 录制帧文件（JSONL）
WS_FRAMES_PATH = RECORDINGS_DIR / "binance_ws_frames.jsonl"


def load_ws_frames() -> list:
    """加载 WS 录制帧的解析后对象列表（帧对象或非法帧字符串）。"""
    frames: list = []
    if not WS_FRAMES_PATH.is_file():
        return frames
    for line in WS_FRAMES_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            frames.append(json.loads(line))
    return frames


def load_ws_raw_frames() -> list[str]:
    """加载 WS 录制帧并还原为原始文本帧（与 ``ws.recv()`` 返回值同形态）。

    录制格式（JSONL，每行一帧）：
    - 组合流帧以 JSON 对象存储，还原时经 ``json.dumps`` 序列化为原始文本；
    - 非法帧以 JSON 字符串形式存储（如 ``"not-json"``），还原后原样作为
      原始文本喂给适配器，验证容错跳过路径。
    """
    raw_frames: list[str] = []
    for frame in load_ws_frames():
        raw_frames.append(frame if isinstance(frame, str) else json.dumps(frame))
    return raw_frames
