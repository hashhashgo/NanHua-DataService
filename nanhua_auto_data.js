import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

import config from "./config.json" with { type: "json" };

import dataserver from "./nanhua_dataserver.js";
import { getTickData, getAllData, data_to_csv, QuotationFreq, subscribe } from "./nanhua.js";

const dataserver_port =
    (process.argv && process.argv.length > 2 && process.argv[1] === fileURLToPath(import.meta.url)) ?
    parseInt(process.argv[2]) : (config?.dataserver?.port ? config.dataserver.port : 13200);
if ((process.argv && process.argv.length > 2) || config?.autodata?.use_dataserver) {
    dataserver.listen(dataserver_port, () => {
        console.log(`Server is listening at http://0.0.0.0:${dataserver_port}`);
    });
}

const data_dir = config?.autodata?.data_dir ? config.autodata.data_dir : "./nanhua_data/";
if (config?.autodata?.enable_file_output) fs.mkdirSync(data_dir, { recursive: true });

const subscribe_tickers = config?.autodata?.subscribe_tickers ? config.autodata.subscribe_tickers : [
    {code: 'PP_NH', freq: "MIN1"},
    {code: 'RB_NH', freq: "MIN1"},
    {code: 'I_NH', freq: "MIN1"},
    {code: 'J_NH', freq: "MIN1"},
    {code: 'HC_NH', freq: "MIN1"}
];

const data = {};

function cache_data(symbol_freq, data) {
    if (!config?.autodata?.enable_file_output) return;
    fs.writeFileSync(path.join(data_dir, symbol_freq + ".json"), JSON.stringify(data));
    data_to_csv(path.join(data_dir, symbol_freq + ".csv"), data);
}

subscribe_tickers.forEach(item => {
    let symbol = item.code;
    let freq = item.freq ? item.freq.toUpperCase() : "MIN1";
    if (!freq) freq = "MIN1";
    let symbol_freq = `${symbol}_${freq}`;
    getAllData(symbol, freq).then(result => {
        data[symbol_freq] = result;
        console.log(`完成初始数据获取：${symbol} ${freq} 共${data[symbol_freq].length}条`);
        let {
            unsubscribe
        } = subscribe(freq, symbol, x => {
            // 订阅会返回所有频率数据，要筛选
            if (x.code === symbol && x.freq && QuotationFreq[x.freq] === freq) {
                data[symbol_freq].unshift(x);
                console.log(`${(new Date(x.quoteTime)).toString()} 收到数据：${symbol} ${freq}`);
                cache_data(symbol_freq, data[symbol_freq]);
            }
        });
        cache_data(symbol_freq, data[symbol_freq]);
    });
    
});
