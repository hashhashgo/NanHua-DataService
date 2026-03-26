import config from "../config.json" with { type: "json" };

////////// data server //////////
import dataserver from "./nanhua_dataserver.js";
import { getKLineData, subscribe, QuotationFreq } from "./nanhua.js";

const dataserver_port = config?.dataserver?.port ?? 13200;
if (config?.producer?.use_dataserver) {
    dataserver.listen(dataserver_port, () => {
        console.info(`Server is listening on port ${dataserver_port}`);
    });
}

////////// initialize database //////////
import { initialize, backfill, publish } from "../adapters/publisher.js";

await initialize();

////////// redis //////////
import { createClient as createRedisClient } from 'redis';

const redis_client = config?.producer?.redis ? createRedisClient({ url: config.producer.redis }) : null;
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

////////// subscribe //////////
const subscribe_tickers = new Set(config?.producer?.subscribe_tickers ?? []);

var initialized = new Set();

if(subscribe_tickers.size > 0) {
    let {
        unsubscribe
    } = subscribe("NH", Array.from(subscribe_tickers), async x => {
        // 订阅会返回所有频率数据
        // 如果没有x.freq，是面板信息，无用
        if (x.freq) {
            x.freq = QuotationFreq[x.freq] || x.freq;
            await publish(x);

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
const promises = [];
subscribe_tickers.forEach(symbol => {
    Object.values(QuotationFreq).forEach(freq => {
        promises.push((async freq => {
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
                        all_data.push(x);
                    }
                    if (x.freqTime) latestFreqTime = Math.max(latestFreqTime ?? 0, x.freqTime);
                });
            }

            console.info(`Initializing ${initialized.size} / ${subscribe_tickers.size * Object.values(QuotationFreq).length}: ` + 
                `Initialized data for ${symbol} ${freq}, total ${data.length} records` +
                `, time range: ${data.length > 0 ? new Date(data[data.length - 1].quoteTime).toISOString() : 'N/A'} - ${data.length > 0 ? new Date(data[0].quoteTime).toISOString() : 'N/A'}` +
                (latestFreqTime ? `, latest freqTime: ${new Date(latestFreqTime * 1000 ?? 0).toISOString()}` : ''));

            if (redis_client && latestFreqTime) {
                redis_insert_max(redis_client, redis_key, latestFreqTime).catch(err => {
                    console.error("Failed to set Redis key", redis_key, err);
                });
            }

            initialized.add(redis_key);
        })(freq));
    });
});

Promise.all(promises).catch(err => {
    console.error("Error during initialization", err);
}).then(() => {
    if (initialized.size === subscribe_tickers.size * Object.values(QuotationFreq).length) {
        console.info("All initial data loaded, backfilling to database...");
        backfill(all_data).then(() => {
            console.info("Initialization completed");
        }).catch(err => {
            console.error("Error during backfill", err);
        });
    } else {
        console.warn("Initialization completed with missing data, initialized " + initialized.size + " / " + (subscribe_tickers.size * Object.values(QuotationFreq).length));
    }
});
