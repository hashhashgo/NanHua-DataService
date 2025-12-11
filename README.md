Languages: English | [中文](README.zh_CN.md)

# Overview
This project provides a real-time market data acquisition and distribution service for indices from Nanhua Futures.
The system connects to the [market data source](https://webhq.nanhua.net) via WebSocket, subscribes to specified symbols, and automatically persists incoming updates to local storage (CSV & JSON).

In addition, it includes a lightweight HTTP data service, acting as a cross-language access layer for real-time and historical market data.

# Installation

```bash
npm i
```

# Usage

## Standalone Mode

Simply start the service:

```bash
npm start
```

This launches both **the realtime data loader and the HTTP data server** based on the settings in `config.json`.

## **Command-Line Mode**

You may start each component manually.

*Command-line arguments override settings in `config.json`.*

### Start the realtime data loader (subscribe + write to file)

```bash
node nanhua_auto_data.js [port]
```

It equals to:

```bash
npm start [port]
npm run start [port]
```

If a `port` is specified, the HTTP data server will **automatically start** using the port.

### Start the HTTP data server only

```bash
node nanhua_dataserver.js [port]
```

It equals to:

```bash
npm run server [port]
```


## Using Data Service

Just using http get request to url:

```url
http://127.0.0.1:[port]/?ticker=[ticker]&freq=[freq]
```

**port:** the port of the data server

**ticker:** the symbol of NanHua Index, such as PP_NH

**freq:** data frequency

**Supported Data Frequency**

`INFO`, `TICK`, `MIN1`, `MIN5`, `MIN15`, `MIN30`, `MIN60`, `MIN120`, `MIN240`, `DAY1`, `WEEK1`, `MONTH1` 


# Configuration

The configuration file is located at: `config.json`

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

**Field Description**

| Field                        | Type    | Description                                     |
| ---------------------------- | ------- | ----------------------------------------------- |
| `dataserver.port`            | number  | Port of the HTTP data server                    |
| `autodata.use_dataserver`    | boolean | Whether to automatically launch the data server |
| `enable_file_output`         | boolean | Whether to write data to files                  |
| `autodata.data_dir`          | string  | Directory for storing scraped data              |
| `autodata.subscribe_tickers` | array   | Symbols and frequency subscriptions             |
| `code`                       | string  | Index/Futures code to subscribe                 |
| `freq`                       | string  | Time interval                                   |

Time interval can be selected from `INFO`, `TICK`, `MIN1`, `MIN5`, `MIN15`, `MIN30`, `MIN60`, `MIN120`, `MIN240`, `DAY1`, `WEEK1`, `MONTH1`.
