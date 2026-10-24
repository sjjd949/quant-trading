# 量化程序（quant-trading）

个人量化交易程序：实时扫描加密货币与股票行情，发现交易机会并告警，为模拟盘与实盘打基础。

## 当前能力（M1 + M2 · 已完成）

**M1 — 加密行情扫描闭环**
- **币安实时行情**：WebSocket 组合流（trade + 1min K 线），指数退避自动重连，重连后自动修复数据缺口
- **4 条扫描规则**（可独立开关、参数可配）：价格异动、成交量突增、RSI 超买/超卖、均线金叉/死叉
- **rich 终端看板**：行情表 + 信号列 + 告警日志 + 连接状态，0.5s 刷新
- **告警输出**：终端高亮 + JSONL + CSV/JSON 导出 + SQLite，同币同规则冷却去重（默认 300s）

**M2 — 跨市场 + 模拟盘**
- **三市场并行**：币安 WS + A股（东财主/新浪备，~3s 轮询，主备自动切换）+ 美港股（腾讯，延迟约 15min，看板与告警三处标注）
- **模拟盘引擎**：每市场 10 万计价货币、滑点 5bps、按市场费率精确计费、`auto`/`manual` 双模式、待确认 FIFO（`y` 确认 / `n` 忽略 / 超时 300s 失效）
- **信号与复盘**：`signals/orders/fills/positions` 四表 + 账户当前态；`review` 子命令按标的/规则/时间回放生命周期
- **规则插件化**：`[rules].custom` 登记 `"模块路径:类名"` 即可加载自定义规则，引擎核心零改动
- **测试体系**：387+ 测试全绿，零真实网络（录制回放 + Mock）；`stress` 子命令支持 24h 稳定性压测

## 环境要求

- Python 3.13+

## 快速开始

```bash
# 克隆仓库
git clone https://github.com/sjjd949/quant-trading.git
cd quant-trading

# 创建虚拟环境
python -m venv .venv
source .venv/Scripts/activate  # Windows Git Bash

# 安装依赖（国内网络建议加镜像：-i https://mirrors.aliyun.com/pypi/simple/）
pip install -r requirements.txt
pip install -e .

# 校验配置后启动
python -m quant --check-config
python -m quant
```

### M2 使用说明（跨市场 + 模拟盘）

启动后三市场并行：币安 WS、A股轮询、美港股轮询（延迟约 15min，看板与告警标注 `⏱~15min` / `[延迟约15min]`）。

**看板按键**：`p` 暂停扫描 · `r` 规则面板 · `1-9` 切换规则开关 · `y` 确认队首待下单 · `n` 忽略队首 · `m` 切换 auto/manual · `q` 退出。

```bash
# 复盘：按标的/规则/时间回放 信号→订单→成交→平仓
python -m quant review --symbol BTCUSDT --format table
python -m quant review --rule price_move --start 2025-10-01T00:00:00 --format json
python -m quant review --account            # 各市场账户汇总

# 24h 稳定性压测（A股/美港股），健康摘要落 data/stress_*.json
python -m quant stress --market a_share --duration 86400 --output data/stress_a_share.json
python -m quant stress --market hk_us   --duration 600
```

模拟盘参数（初始资金/滑点/费率/仓位比例/确认超时）与自选清单均在 `config/config.toml` 的 `[paper]`、`[market.a_share]`、`[market.hk_us]` 调整。

## 网络代理配置（重要）

行情请求支持三级代理解析，优先级：`QUANT_PROXY` 环境变量 > `HTTPS_PROXY`/`HTTP_PROXY` 系统环境变量 > `config/config.toml` 的 `proxy` 值。

- 有代理工具（环境变量已设置）→ 直接运行即可
- 无系统代理 → 设置 `QUANT_PROXY=http://127.0.0.1:端口`，或填写 `config/config.toml`
- 三者均为空时会以中文错误提示退出并列出配置方式

默认监控币种（BTC/ETH/SOL/BNB）、规则参数、冷却时间等均在 `config/config.toml` 中调整，无需改代码；敏感项用 `.env`（参考 `config/.env.example`）。

## 项目结构

```
量化程序/
├── config/
│   ├── config.toml      # 全部运行配置（代理/币种/规则/路径）
│   └── .env.example     # 环境变量覆盖示例
├── src/quant/
│   ├── config.py        # 配置加载（tomllib + .env + 环境变量覆盖）
│   ├── models.py        # 统一数据模型（Tick/Candle/Alert 等）
│   ├── app.py           # QuantApp 多适配器编排（行情/命令/渲染/待确认看门狗）
│   ├── feeds/           # 行情适配器（币安 WS + A股/美港股轮询）
│   ├── paper/           # 模拟盘引擎（账户/滑点/费率/双模式/盈亏）
│   ├── indicators/      # 指标引擎（RSI/MA/量均，RingBuffer 窗口）
│   ├── scanner/         # 规则引擎与 4 条内置规则 + 插件加载
│   ├── alerts/          # 告警管理（冷却去重/导出/持久化）
│   ├── storage/         # SQLite 存储（告警/K线/信号/订单/成交/持仓）
│   ├── cli/             # 看板、键盘、review 复盘、stress 压测
│   └── utils/           # 时间与日志工具
├── tests/               # 测试（network 标记测试默认排除，-m network 单独跑真实冒烟）
├── docs/                # PRD 与系统架构设计
├── data/                # 运行数据（SQLite/告警日志，不入库）
└── requirements.txt
```

## 开发规范

- 遵循 `AGENT.md`：每次改动必须建立对应 Git commit；每次改动必须编写/更新测试，交付前测试全部通过
- 提交格式：`<type>(<scope>): 中文描述`
- 测试禁网：外部 IO 用 mock/录制回放，真实连通性测试打 `network` 标记
- 数据文件（行情、数据库）不入库，见 `.gitignore`

## 路线图

- **M2（已完成）**：A 股（东财/新浪）与美股/港股（腾讯）适配器、模拟盘引擎（auto/manual 双模式）、信号记录与复盘、规则插件化、历史数据评估
- **M3**：回测引擎（基于 `docs/historical-data-assessment.md` 的 Parquet 混合方案）、Web 看板、实盘对接

## 许可

待定。
