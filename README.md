# 日内趋势雷达（Futu OpenD）

本项目订阅 Futu OpenD 的盘中 1m/5m K 线，基于技术指标与规则引擎生成**日内趋势提示**，并支持 Telegram/本地弹窗通知与本地 Web 控制台展示。当前版本已支持本地 SQLite 持久化、简易登录、全局股票池、用户自选池、规则覆盖和本地回测入口。

## 文档入口

- 快速查看产品使用方式，请阅读 [项目功能使用说明书.md](/Users/lilin/develop/workspace/codex/trending_and_chances/项目功能使用说明书.md)
- Web 控制台右上角也提供了“使用说明”入口，可直接打开同一份文档
- EC2 部署模板、`systemd`、`nginx` 与生产配置样例见 [deploy/README.md](/Users/lilin/develop/workspace/codex/trending_and_chances/deploy/README.md)

## 功能要点

- 1m/5m K 线实时订阅
- 规则触发提示（开盘区间突破、VWAP 偏离、波动率收缩后放大）
- 同一根 bar 最多输出 1 条最终信号，`up/down` 会先互相抵消
- 提示等级分级（Lv0-Lv5，按净方向触发指标数动态计算）
- Telegram/本地弹窗通知
- 本地 Web 控制台（登录、配置中心、用户管理、回测）
- SQLite 持久化（用户、股票池、规则配置、信号、回测任务）

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
- `app.auth.bootstrap_admin`：首个本地管理员账号
- `app.auth.public_registration`：未登录页面的自助注册开关与人数上限
- `app.symbols`：全局股票池初始化值
- `app.storage.db_path`：SQLite 数据库文件
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

首次启动会用 `config.yaml` 初始化 SQLite 和管理员账号；若你使用示例配置，默认账号为 `admin / admin`。

未登录页面支持简易注册；默认最多允许 `200` 个普通用户账号，数量由 `app.auth.public_registration.max_users` 控制。每个普通用户的自选股票池当前最多保留 `1` 支股票。

当前信号作用范围：
- 全局股票池只由 `global profile` 产出统一信号，所有用户看到的全局股票信号一致
- 用户个人规则只作用在自己的自选股票池，不会覆盖全局股票池
- 监控页中的同一根 K 线会先做方向抵消，再输出一条最终信号

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
- Web 控制台运行时以 SQLite 为真源；`config.yaml` 主要负责首启种子配置。
- 更详细的页面说明、信号逻辑和回测说明，请查看 [项目功能使用说明书.md](/Users/lilin/develop/workspace/codex/trending_and_chances/项目功能使用说明书.md)。
