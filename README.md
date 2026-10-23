# 量化程序（quant-trading）

个人量化交易程序项目。

## 简介

本项目用于量化交易策略的研究、回测与实盘交易，涵盖数据获取、因子计算、策略回测、风险控制和交易执行等模块。

## 环境要求

- Python 3.13+
- Git

## 快速开始

```bash
# 克隆仓库
git clone https://github.com/sjjd949/quant-trading.git
cd quant-trading

# 创建虚拟环境
python -m venv .venv
source .venv/Scripts/activate  # Windows Git Bash

# 安装依赖（待补充）
pip install -r requirements.txt
```

## 项目结构

```
量化程序/
├── data/           # 数据文件（原始数据不入库）
├── src/            # 源代码
├── strategies/     # 交易策略
├── backtest/       # 回测框架与结果
├── notebooks/      # 研究笔记
└── tests/          # 测试用例
```

## 开发规范

- 提交信息使用清晰的中文或英文描述
- 敏感配置（API 密钥等）通过 `.env` 管理，切勿提交
- 大数据文件（行情、数据库）不入库，见 `.gitignore`

## 许可

待定。
