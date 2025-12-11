Languages: [English](README.md) | 中文

# 简介
本项目实现了南华期货指数的实时行情获取与数据分发服务。

系统通过 WebSocket 连接至 [行情数据源](https://webhq.nanhua.net)，订阅指定品种/指数，并将推送的最新数据自动保存至本地（支持 CSV 与 JSON 格式）。

此外，项目提供了一个轻量级的 HTTP 数据服务，可作为跨语言的数据访问层，为其它语言（如 Python、C++、Java、R 等）提供实时与历史行情查询接口。

# 安装

```bash
npm i
```

# 使用方法

## 独立运行

只要用npm 

```bash
npm start
```

该方式会根据 `config.json` 的配置 **同时启动实时数据加载器和 HTTP 数据服务器**。

## **命令行模式**

你可以手动启动各个组件。

*命令行参数将覆盖 `config.json` 中的配置。*

### 启动实时数据加载器（订阅 + 写入文件）

```bash
node nanhua_auto_data.js [port]
```

等价于：

```bash
npm start [port]
npm run start [port]
```

如果指定了 `port`，则会 **自动启动 HTTP 数据服务器** 并使用该端口。

### 仅启动 HTTP 数据服务器

```bash
node nanhua_dataserver.js [port]
```

等价于：

```bash
npm run server [port]
```

## 使用数据服务

使用 HTTP GET 请求以下URL:

```url
http://127.0.0.1:[port]/?ticker=[ticker]&freq=[freq]
```

**port:** 数据服务器绑定的端口

**ticker:** 南华期货的指数符号，如PP_NH

**freq:** 数据频率

**支持的数据频率**

`INFO`, `TICK`, `MIN1`, `MIN5`, `MIN15`, `MIN30`, `MIN60`, `MIN120`, `MIN240`, `DAY1`, `WEEK1`, `MONTH1` 


# 配置

```json
{
    "dataserver": {
        "port": 13200
    },
    "autodata": {
        "use_dataserver": true,
        "enable_file_output": true,
        "data_dir": "./nanhua_data/",
        "subscribe_tickers": [
            {"code": "PP_NH", "freq": "MIN1"},
            {"code": "RB_NH", "freq": "MIN1"},
            {"code": "I_NH", "freq": "MIN1"},
            {"code": "J_NH", "freq": "MIN1"},
            {"code": "HC_NH", "freq": "MIN1"}
        ]
    }
}
```

**字段说明**

| 字段                         | 类型    | 描述                    |
| ---------------------------- | ------- | ----------------------- |
| `dataserver.port`            | number  | HTTP 数据服务器端口     |
| `autodata.use_dataserver`    | boolean | 是否自动启动数据服务器  |
| `enable_file_output`         | boolean | 是否写入文件            |
| `autodata.data_dir`          | string  | 抓取数据存储目录        |
| `autodata.subscribe_tickers` | array   | 所订阅的品种与周期列表  |
| `code`                       | string  | 需要订阅的指数/品种代码 |
| `freq`                       | string  | 时间周期                |

可选周期包括：`INFO`, `TICK`, `MIN1`, `MIN5`, `MIN15`, `MIN30`, `MIN60`, `MIN120`, `MIN240`, `DAY1`, `WEEK1`, `MONTH1`。
