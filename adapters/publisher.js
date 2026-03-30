/*
This file is used to publish data.

You should export two functions:
- `initialize`: to initialize the database, create tables, etc.
- `backfill`: to backfill historical data, which will be called after `initialize`.
- `publish`: to publish real-time data, which will be called for each new data point.

**Note**
If using databases like ClickHouse, you cannot insert history data directly,
because tables are usually partitioned by date, and inserting history data may cause 
"Too many partitions for single INSERT block (more than 100)" error.
Therefore, there is a separate `backfill` function to handle historical data, and you can insert them into the database in batches.
For real-time data, you can implement the `publish` function to insert data into the database immediately when new data arrives.
*/

import config from "../config.json" with { type: "json" };

////////// kafka //////////
import { Kafka, logLevel } from 'kafkajs';

const BROKERS = config?.publisher?.kafka?.brokers ?? ['kafka:9092'];
const TOPIC = config?.publisher?.kafka?.topic ?? "nanhua_index_data";

const kafka = new Kafka({
  clientId: config?.publisher?.kafka?.clientId ?? 'quotes-service',
  brokers: BROKERS,
  logLevel: config?.publisher?.kafka?.logLevel ? logLevel[config.publisher.kafka.logLevel] : logLevel.WARN,
});

const kafka_admin = kafka.admin();
await kafka_admin.connect();
if (!(await kafka_admin.listTopics()).includes(TOPIC)) {
    await kafka_admin.createTopics({
        topics: [
            {
            topic: TOPIC,
            numPartitions: 4,
            replicationFactor: 1,
            },
        ],
    });
}
kafka_admin.disconnect();

const kafka_producer = kafka.producer({
    allowAutoTopicCreation: true,
    transactionTimeout: 30000
});
await kafka_producer.connect();

////////// queue and forward to publisher //////////
const queue = [];
const FLUSH_MS = 20;
const MAX_BATCH = 500;

setInterval(() => {
  if (queue.length === 0) return;

   do {
    const batch = queue.splice(0, MAX_BATCH);
    kafka_producer.send({
      topic: TOPIC,
      messages: batch,
    }).catch(err => {
      console.error("kafka batch send failed", err);
    });
  } while (queue.length > MAX_BATCH);

}, FLUSH_MS);

////////// ClickHouse client //////////
import { createClient } from '@clickhouse/client'

const clickhouse_client = createClient({
    url: config?.publisher?.clickhouse?.url ?? 'http://clickhouse:8123',
    username: process.env.CLICKHOUSE_USER ?? 'default',
    password: process.env.CLICKHOUSE_PASSWORD ?? '',
});

const database = config?.publisher?.clickhouse?.database ?? "default";
async function clickhouse_command(command) {
    return await clickhouse_client.command({
        query: command,
        clickhouse_settings: {
            wait_end_of_query: 1,
        },
    })
}
async function clickhouse_create_table_by_freq(freq, partition_by = "toYYYYMM(timestamp)") {
    const freq_ = freq.toLowerCase();
    await clickhouse_command(`
CREATE TABLE IF NOT EXISTS ${database}.nanhua_index_${freq_}
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
PARTITION BY ${partition_by}
ORDER BY (symbol, timestamp);`);
    await clickhouse_command(`
CREATE MATERIALIZED VIEW IF NOT EXISTS ${database}.mv_nanhua_index_to_${freq_}
TO ${database}.nanhua_index_${freq_}
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
FROM ${database}.nanhua_index_kafka
WHERE JSONExtractString(raw, 'freq') = '${freq_.toUpperCase()}';`);
}

////////// utils //////////
function flattenObject(obj, prefix = '', res = {}) {
    for (const key in obj) {
        if (!obj.hasOwnProperty(key)) continue;

        const value = obj[key];
        const newKey = prefix ? `${prefix}_${key}` : key;

        if (
            value &&
            typeof value === 'object' &&
            !Array.isArray(value)
        ) {
            flattenObject(value, newKey, res);
        } else {
            res[newKey] = value;
        }
    }
    return res;
}

////////// exported functions //////////
export async function initialize() {
    // Initialize the database, create tables, etc.
    await clickhouse_command(`CREATE DATABASE IF NOT EXISTS ${database}`);
    await clickhouse_command(`
CREATE TABLE IF NOT EXISTS ${database}.nanhua_index_kafka
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '` + BROKERS.join(',') + `',
    kafka_topic_list = '` + TOPIC + `',
    kafka_group_name = 'clickhouse_market_nanhua_index_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 4;`);
    await clickhouse_command(`
CREATE TABLE IF NOT EXISTS ${database}.nanhua_index_raw_archive
(
    ingest_time DateTime DEFAULT now(),
    raw String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_time)
ORDER BY ingest_time;`);
    await clickhouse_command(`
CREATE MATERIALIZED VIEW IF NOT EXISTS ${database}.mv_nanhua_index_raw_archive
TO ${database}.nanhua_index_raw_archive
AS
SELECT raw
FROM ${database}.nanhua_index_kafka;`);
    await clickhouse_command(`
CREATE TABLE IF NOT EXISTS ${database}.nanhua_index_tick
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
ORDER BY (symbol, timestamp);`);
    await clickhouse_command(`
CREATE MATERIALIZED VIEW IF NOT EXISTS ${database}.mv_nanhua_index_to_tick
TO ${database}.nanhua_index_tick
AS
SELECT
    JSONExtractString(raw, 'code') AS symbol,
    toDateTime64(JSONExtractUInt(raw, 'quoteTime') / 1000, 3, 'UTC') AS timestamp,
    JSONExtractFloat(raw, 'rt_last') AS last_price,
    JSONExtract(raw, 'rt_tag', 'UInt8') AS tag,
    JSONExtract(raw, 'rt_posiDelta', 'Int32') AS posi_delta,
    JSONExtract(raw, 'rt_tickVolume', 'Int32') AS tick_volume
FROM ${database}.nanhua_index_kafka
WHERE JSONExtractString(raw, 'freq') = 'TICK';`);
    await clickhouse_create_table_by_freq("min1");
    await clickhouse_create_table_by_freq("min5");
    await clickhouse_create_table_by_freq("min15");
    await clickhouse_create_table_by_freq("min30");
    await clickhouse_create_table_by_freq("min60");
    await clickhouse_create_table_by_freq("min120");
    await clickhouse_create_table_by_freq("min240");
    await clickhouse_create_table_by_freq("day1");
    await clickhouse_create_table_by_freq("week1", "toYear(timestamp)");
    await clickhouse_create_table_by_freq("month1", "toYear(timestamp)");

    console.info("Database initialized");
}

export async function backfill(rows) {
    // Backfill historical data, which will be called after `initialize`.
    // The `rows` parameter is an array of historical data, you can insert them into the database in batches.
    console.info(`Backfilling ${rows.length} records`);

    const buckets = {};
    rows.forEach(row => {
        const bucketKey = row.quoteTime ? Math.floor(row.quoteTime / 100000000000) : 0;
        const data = {};
        switch (row.freq) {
            case "TICK": {
                data.symbol = row.code;
                data.timestamp = row.quoteTime;
                data.last_price = row.rt.last;
                data.tag = row.rt.tag;
                data.posi_delta = row.rt.posiDelta;
                data.tick_volume = row.rt.tickVolume;
                break;
            }
            case "INFO": break; // no need to backfill INFO data
            default: {
                data.symbol = row.code;
                data.timestamp = row.freqTime * 1000;
                data.open = row.open;
                data.high = row.high;
                data.low = row.low;
                data.close = row.close;
                data.volume = row.volume;
                data.amount = row.turnOver;
                data.open_interest = row.posi;
                data.pre_close = row.preClose;
                break;
            }
        }
        if (Object.keys(data).length > 0) {
            if (!buckets[bucketKey]) buckets[bucketKey] = {};
            if (!buckets[bucketKey][row.freq]) buckets[bucketKey][row.freq] = [];
            buckets[bucketKey][row.freq].push(data);
        }
    });

    const promises = [];
    for (const bucketKey in buckets) {
        for (const freq in buckets[bucketKey]) {
            promises.push(clickhouse_client.insert({
                table: `${database}.nanhua_index_${freq.toLocaleLowerCase()}`,
                values: buckets[bucketKey][freq],
                format: 'JSONEachRow'
            }));
        }
    }

    await Promise.all(promises);
    console.info(`Data divided into ${Object.keys(buckets).length} buckets based on quoteTime`);
    console.info("Backfilling completed");
}

export async function publish(row) {
    // Publish real-time data, which will be called for each new data point.
    // The `row` parameter is a single data point, you can insert it into the database immediately.
    queue.push({ key: `${row.code}`, value: JSON.stringify(flattenObject(row)) });
}