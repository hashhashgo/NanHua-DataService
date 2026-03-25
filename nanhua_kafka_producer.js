import config from "./config.json" with { type: "json" };

////////// data server //////////
import dataserver from "./nanhua_dataserver.js";
import { getKLineData, subscribe, QuotationFreq } from "./nanhua.js";

const dataserver_port = config?.dataserver?.port ?? 13200;
if (config?.kafka_producer?.use_dataserver) {
    dataserver.listen(dataserver_port, () => {
        console.info(`Server is listening on port ${dataserver_port}`);
    });
}

////////// initialize database //////////
import { initialize, backfill } from "./db.js";

await initialize();

////////// redis //////////
import { createClient as createRedisClient } from 'redis';

const redis_client = config?.kafka_producer?.redis ? createRedisClient({ url: config.kafka_producer.redis }) : null;
if (redis_client) {
    redis_client.on('error', (err) => console.error('Redis Client Error', err));
    await redis_client.connect();
}

const redis_sha_insert_max = await redis_client?.scriptLoad("\
    local current = server.call('GET', KEYS[1]);\
    if not current or tonumber(current) < tonumber(ARGV[1]) then\
        server.call('SET', KEYS[1], ARGV[1]);\
        return ARGV[1];\
    end;\
    return current;")
const redis_insert_max = (!redis_client || !redis_sha_insert_max) ? undefined : async (client, key, value) => {
    if (typeof key !== 'string' || typeof value !== 'number') {
        throw new Error('Invalid key or value type for redis_insert_max');
    }
    return Number.parseInt(await client.evalSha(redis_sha_insert_max, {
        keys: [key],
        arguments: [value.toString()]
    }))
};

////////// kafka producer //////////
import { Kafka, logLevel } from 'kafkajs';

const TOPIC = config?.kafka_producer?.topic ?? "nanhua_market_data";

const kafka = new Kafka({
  clientId: config?.kafka_producer?.clientId ?? 'quotes-service',
  brokers: config?.kafka_producer?.brokers ?? ['kafka:9092'],
  logLevel: config?.kafka_producer?.logLevel ? logLevel[config.kafka_producer.logLevel] : logLevel.WARN,
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

////////// queue and forward to kafka //////////
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

////////// subscribe //////////
const subscribe_tickers = new Set(config?.kafka_producer?.subscribe_tickers ?? []);

var initialized = new Set();

if(subscribe_tickers.size > 0) {
    let {
        unsubscribe
    } = subscribe("NH", Array.from(subscribe_tickers), async x => {
        // 订阅会返回所有频率数据
        // 如果没有x.freq，是面板信息，无用
        if (x.freq) {
            x.freq = QuotationFreq[x.freq] || x.freq;
            queue.push({ key: `${x.code}`, value: JSON.stringify(flattenObject(x)) });

            const redis_key = `nanhua_${x.code}_${x.freq}`;
            if (redis_client && x.freqTime && initialized.has(redis_key)) {
                redis_insert_max(redis_client, redis_key, x.freqTime).catch(err => {
                    console.error("Failed to set Redis key", redis_key, err);
                });
            }
        }
    });
}

////////// initialize data //////////
const all_data = [];
subscribe_tickers.forEach(symbol => {
    Object.values(QuotationFreq).forEach(async freq => {
        const redis_key = `nanhua_${symbol}_${freq}`;
        const last_freqTime = (await redis_client?.get(redis_key)) ?? 0;

        var data = [];
        var tempData = (await getKLineData(symbol, 500, freq, null))[0];
        if (tempData.quotation && tempData.quotation.length > 0) {
            var earliestfreq = tempData.quotation[tempData.quotation.length - 1].freqTime;
            data.push(...tempData.quotation);
            while (
                earliestfreq && earliestfreq > last_freqTime &&
                (tempData = (await getKLineData(symbol, 500, freq, earliestfreq))[0]) &&
                tempData.quotation && tempData.quotation.length > 0 &&
                earliestfreq > tempData.quotation[tempData.quotation.length - 1].freqTime
            ) {
                if (earliestfreq == tempData.quotation[0].freqTime) data.push(...tempData.quotation.slice(1));
                else data.push(...tempData.quotation);
                earliestfreq = tempData.quotation[tempData.quotation.length - 1].freqTime;
            }

            var latestFreqTime = null;
            data.forEach(x => {
                if (!x.freqTime || x.freqTime > last_freqTime) {
                    x.freq = QuotationFreq[x.freq] || x.freq;
                    all_data.push({ key: `${x.code}`, value: JSON.stringify(flattenObject(x)) });
                }
                if (x.freqTime) latestFreqTime = Math.max(latestFreqTime ?? 0, x.freqTime);
            });
        }

        console.info(`Initializing ${initialized.size} / ${subscribe_tickers.size * Object.values(QuotationFreq).length}: ` + 
            `Initialized data for ${symbol} ${freq}, total ${data.length} records` +
            `, time range: ${data.length > 0 ? new Date(data[data.length - 1].quoteTime).toISOString() : 'N/A'} - ${data.length > 0 ? new Date(data[0].quoteTime).toISOString() : 'N/A'}` +
            (last_freqTime ? `, last freqTime: ${new Date(latestFreqTime * 1000 ?? 0).toISOString()}` : ''));

        if (redis_client && latestFreqTime) {
            redis_insert_max(redis_client, redis_key, latestFreqTime).catch(err => {
                console.error("Failed to set Redis key", redis_key, err);
            });
        }

        initialized.add(redis_key);
        if (initialized.size === subscribe_tickers.size * Object.values(QuotationFreq).length) {
            console.info("All initial data loaded, backfilling to database...");
            await backfill(all_data);
            console.info("Initialization completed");
        }
    });
});
