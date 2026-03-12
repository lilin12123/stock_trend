# 日内波动提示器（Futu OpenD）

本项目订阅 Futu OpenD 的港股 1m/5m K 线，基于技术指标与规则引擎生成**日内波动提示**，并支持 Telegram/本地弹窗通知与本地 Web 面板展示。指标与规则完全模块化，便于你快速调整。

## 功能要点

- 1m/5m K 线实时订阅
- 规则触发提示（开盘区间突破、VWAP 偏离、波动率收缩后放大）
- 提示等级分级（Lv0-Lv5，按触发规则动态计算）
- Telegram/本地弹窗通知
- 本地 Web 面板（最近信号列表 + 过滤器）

## 环境准备

1. 确保本地已运行 Futu OpenD，并已具备港股行情权限。
2. 安装依赖：

```bash
pip install -r requirements.txt
```

## 配置

复制配置文件并按需修改：

```bash
cp config.example.yaml config.yaml
```

重点字段：
- `app.symbols`：监控股票池
- `rules.cooldown_seconds`：同一规则冷却时间（秒）
- `rules.*`：各条规则的开关与参数

Telegram（可选）：

```bash
export TELEGRAM_BOT_TOKEN="..."
export TELEGRAM_CHAT_ID="..."
```

## 运行（实时）

```bash
python -m src.main --config config.yaml
```

## Web 面板

默认地址：

```
http://127.0.0.1:8088
```

## 端口占用排查与一键关闭

查询 8088 端口并关闭占用进程（单条命令）：

```bash
lsof -nP -iTCP:8088 -sTCP:LISTEN && kill $(lsof -t -iTCP:8088 -sTCP:LISTEN)
```

## 回测（历史回放）

回测会调用 OpenD 的历史 K 线接口进行日内回放，输出每条信号：

```bash
python -m src.backtest --config config.yaml --symbol HK.00981 --date 2026-03-10
```

你可以将输出重定向到文件：

```bash
python -m src.backtest --config config.yaml --symbol HK.00981 --date 2026-03-10 > backtest.txt
```

## 备注

- 本项目仅用于提示，不会下单。
- 若提示过于频繁，建议提高 `rules.cooldown_seconds`，或收紧对应规则的量能/ATR/RSI 等参数。
