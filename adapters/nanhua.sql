CREATE DATABASE IF NOT EXISTS market;

-- Kafka engine table
CREATE TABLE IF NOT EXISTS market.nanhua_kafka
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'nanhua_market_data',
    kafka_group_name = 'clickhouse_market_nanhua_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 4;

-- Raw archive table
CREATE TABLE IF NOT EXISTS market.nanhua_raw_archive
(
    ingest_time DateTime DEFAULT now(),
    raw String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_time)
ORDER BY ingest_time;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_raw_archive
TO market.nanhua_raw_archive
AS
SELECT raw
FROM market.nanhua_kafka;

-- Processed TICK data table
CREATE TABLE IF NOT EXISTS market.nanhua_tick
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    last_price Float32,
    tag UInt8,
    posi_delta Int32,
    tick_volume Int32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_to_tick
TO market.nanhua_tick
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'quoteTime') / 1000, 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'rt_last') AS last_price,
    JSONExtract(raw, 'rt_tag', 'UInt8') AS tag,
    JSONExtract(raw, 'rt_posiDelta', 'Int32') AS posi_delta,
    JSONExtract(raw, 'rt_tickVolume', 'Int32') AS tick_volume
FROM market.nanhua_kafka
WHERE JSONExtractString(raw, 'freq') = 'TICK';

-- Processed MIN1 data table
CREATE TABLE IF NOT EXISTS market.nanhua_min1
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume Int32,
    amount Float32,
    open_interest Int32,
    pre_close Float32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_to_min1
TO market.nanhua_min1
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'freqTime'), 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'open') AS open,
    JSONExtractFloat(raw, 'high') AS high,
    JSONExtractFloat(raw, 'low') AS low,
    JSONExtractFloat(raw, 'close') AS close,
    JSONExtract(raw, 'volume', 'Int32') AS volume,
    JSONExtract(raw, 'turnOver', 'Float32') AS amount,
    JSONExtract(raw, 'posi', 'Int32') AS open_interest,
    JSONExtractFloat(raw, 'preClose') AS pre_close
FROM market.nanhua_kafka
WHERE JSONExtractString(raw, 'freq') = 'MIN1';

-- Processed MIN5 data table
CREATE TABLE IF NOT EXISTS market.nanhua_min5
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume Int32,
    amount Float32,
    open_interest Int32,
    pre_close Float32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_to_min5
TO market.nanhua_min5
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'freqTime'), 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'open') AS open,
    JSONExtractFloat(raw, 'high') AS high,
    JSONExtractFloat(raw, 'low') AS low,
    JSONExtractFloat(raw, 'close') AS close,
    JSONExtract(raw, 'volume', 'Int32') AS volume,
    JSONExtract(raw, 'turnOver', 'Float32') AS amount,
    JSONExtract(raw, 'posi', 'Int32') AS open_interest,
    JSONExtractFloat(raw, 'preClose') AS pre_close
FROM market.nanhua_kafka
WHERE JSONExtractString(raw, 'freq') = 'MIN5';

-- Processed MIN15 data table
CREATE TABLE IF NOT EXISTS market.nanhua_min15
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume Int32,
    amount Float32,
    open_interest Int32,
    pre_close Float32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_to_min15
TO market.nanhua_min15
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'freqTime'), 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'open') AS open,
    JSONExtractFloat(raw, 'high') AS high,
    JSONExtractFloat(raw, 'low') AS low,
    JSONExtractFloat(raw, 'close') AS close,
    JSONExtract(raw, 'volume', 'Int32') AS volume,
    JSONExtract(raw, 'turnOver', 'Float32') AS amount,
    JSONExtract(raw, 'posi', 'Int32') AS open_interest,
    JSONExtractFloat(raw, 'preClose') AS pre_close
FROM market.nanhua_kafka
WHERE JSONExtractString(raw, 'freq') = 'MIN15';

-- Processed MIN30 data table
CREATE TABLE IF NOT EXISTS market.nanhua_min30
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume Int32,
    amount Float32,
    open_interest Int32,
    pre_close Float32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_to_min30
TO market.nanhua_min30
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'freqTime'), 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'open') AS open,
    JSONExtractFloat(raw, 'high') AS high,
    JSONExtractFloat(raw, 'low') AS low,
    JSONExtractFloat(raw, 'close') AS close,
    JSONExtract(raw, 'volume', 'Int32') AS volume,
    JSONExtract(raw, 'turnOver', 'Float32') AS amount,
    JSONExtract(raw, 'posi', 'Int32') AS open_interest,
    JSONExtractFloat(raw, 'preClose') AS pre_close
FROM market.nanhua_kafka
WHERE JSONExtractString(raw, 'freq') = 'MIN30';

-- Processed MIN60 data table
CREATE TABLE IF NOT EXISTS market.nanhua_min60
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume Int32,
    amount Float32,
    open_interest Int32,
    pre_close Float32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_to_min60
TO market.nanhua_min60
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'freqTime'), 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'open') AS open,
    JSONExtractFloat(raw, 'high') AS high,
    JSONExtractFloat(raw, 'low') AS low,
    JSONExtractFloat(raw, 'close') AS close,
    JSONExtract(raw, 'volume', 'Int32') AS volume,
    JSONExtract(raw, 'turnOver', 'Float32') AS amount,
    JSONExtract(raw, 'posi', 'Int32') AS open_interest,
    JSONExtractFloat(raw, 'preClose') AS pre_close
FROM market.nanhua_kafka
WHERE JSONExtractString(raw, 'freq') = 'MIN60';

-- Processed MIN120 data table
CREATE TABLE IF NOT EXISTS market.nanhua_min120
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume Int32,
    amount Float32,
    open_interest Int32,
    pre_close Float32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_to_min120
TO market.nanhua_min120
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'freqTime'), 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'open') AS open,
    JSONExtractFloat(raw, 'high') AS high,
    JSONExtractFloat(raw, 'low') AS low,
    JSONExtractFloat(raw, 'close') AS close,
    JSONExtract(raw, 'volume', 'Int32') AS volume,
    JSONExtract(raw, 'turnOver', 'Float32') AS amount,
    JSONExtract(raw, 'posi', 'Int32') AS open_interest,
    JSONExtractFloat(raw, 'preClose') AS pre_close
FROM market.nanhua_kafka
WHERE JSONExtractString(raw, 'freq') = 'MIN120';

-- Processed MIN240 data table
CREATE TABLE IF NOT EXISTS market.nanhua_min240
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume Int32,
    amount Float32,
    open_interest Int32,
    pre_close Float32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_to_min240
TO market.nanhua_min240
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'freqTime'), 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'open') AS open,
    JSONExtractFloat(raw, 'high') AS high,
    JSONExtractFloat(raw, 'low') AS low,
    JSONExtractFloat(raw, 'close') AS close,
    JSONExtract(raw, 'volume', 'Int32') AS volume,
    JSONExtract(raw, 'turnOver', 'Float32') AS amount,
    JSONExtract(raw, 'posi', 'Int32') AS open_interest,
    JSONExtractFloat(raw, 'preClose') AS pre_close
FROM market.nanhua_kafka
WHERE JSONExtractString(raw, 'freq') = 'MIN240';

-- Processed DAY1 data table
CREATE TABLE IF NOT EXISTS market.nanhua_day1
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume Int32,
    amount Float32,
    open_interest Int32,
    pre_close Float32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_to_day1
TO market.nanhua_day1
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'freqTime'), 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'open') AS open,
    JSONExtractFloat(raw, 'high') AS high,
    JSONExtractFloat(raw, 'low') AS low,
    JSONExtractFloat(raw, 'close') AS close,
    JSONExtract(raw, 'volume', 'Int32') AS volume,
    JSONExtract(raw, 'turnOver', 'Float32') AS amount,
    JSONExtract(raw, 'posi', 'Int32') AS open_interest,
    JSONExtractFloat(raw, 'preClose') AS pre_close
FROM market.nanhua_kafka
WHERE JSONExtractString(raw, 'freq') = 'DAY1';

-- Processed WEEK1 data table
CREATE TABLE IF NOT EXISTS market.nanhua_week1
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume Int32,
    amount Float32,
    open_interest Int32,
    pre_close Float32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYear(timestamp)
ORDER BY (symbol, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_to_week1
TO market.nanhua_week1
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'freqTime'), 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'open') AS open,
    JSONExtractFloat(raw, 'high') AS high,
    JSONExtractFloat(raw, 'low') AS low,
    JSONExtractFloat(raw, 'close') AS close,
    JSONExtract(raw, 'volume', 'Int32') AS volume,
    JSONExtract(raw, 'turnOver', 'Float32') AS amount,
    JSONExtract(raw, 'posi', 'Int32') AS open_interest,
    JSONExtractFloat(raw, 'preClose') AS pre_close
FROM market.nanhua_kafka
WHERE JSONExtractString(raw, 'freq') = 'WEEK1';

-- Processed MONTH1 data table
CREATE TABLE IF NOT EXISTS market.nanhua_month1
(
    symbol LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume Int32,
    amount Float32,
    open_interest Int32,
    pre_close Float32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYear(timestamp)
ORDER BY (symbol, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.mv_nanhua_to_month1
TO market.nanhua_month1
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'freqTime'), 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'open') AS open,
    JSONExtractFloat(raw, 'high') AS high,
    JSONExtractFloat(raw, 'low') AS low,
    JSONExtractFloat(raw, 'close') AS close,
    JSONExtract(raw, 'volume', 'Int32') AS volume,
    JSONExtract(raw, 'turnOver', 'Float32') AS amount,
    JSONExtract(raw, 'posi', 'Int32') AS open_interest,
    JSONExtractFloat(raw, 'preClose') AS pre_close
FROM market.nanhua_kafka
WHERE JSONExtractString(raw, 'freq') = 'MONTH1';
