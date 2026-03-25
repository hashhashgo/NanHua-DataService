import { JSDOM } from 'jsdom';
import fs from 'fs';

const dom = new JSDOM(`<!DOCTYPE html><p>Hello</p>`, {
    url: "https://example.com",
});

globalThis.localStorage = {
    _data: {},
    getItem(key) {
        return this._data[key] || null;
    },
    setItem(key, value) {
        this._data[key] = String(value);
    },
    removeItem(key) {
        delete this._data[key];
    },
    clear() {
        this._data = {};
    }
};

globalThis.window = dom.window;
globalThis.document = dom.window.document;
globalThis.self = dom.window;
globalThis.location = {
    href: "https://example.com/",
    reload() {
        console.log("[fake] location.reload() called (ignored)");
    }
};

await import("./vendor.95d3d7b1.js");
await import("./main.5eddcbfb.js");

var req;
globalThis.self.webpackChunkquota_kline.push([
    [Math.random()],
    {},
    (r) => { req = r; }
]);

const QuotationFreq = {
    1: "INFO",
    2: "TICK",
    3: "MIN1",
    4: "MIN5",
    5: "MIN15",
    6: "MIN30",
    7: "MIN60",
    8: "MIN120",
    9: "MIN240",
    10: "DAY1",
    11: "WEEK1",
    12: "MONTH1"
};

const {
    hO: _,
    It: broadcastSubscribe, // namespace, callback
    zu: closeConnection,
    dq: connectStatusSubscribe, // t
    CB: getContractBaseInfo_version_web, // all information include symbols
    BG: getContractCategory_version_web,
    SD: getKLineData, // symbol: "PP_NH", data_count: 1000, quotationFreq: "DAY1", endTime: 1764918000 | null
    sE: getTickData, // symbol: "PP_NH", infoDay: 0 -> today, 1 -> 2days, ... (max 5)
    v0: getTradeStatus, // symbol: "PP_NH"
    bA: getTickData_Detail, // symbol: "PP_NH", data_count: 1000 (only one day minute data)
    bl: getTraderStatus,
    il: subscribe, // namespace, codes, callback
    TH: unsubscribeAll, // namespace
    nE: subscribeFull, // namespace, codes, callback (return unsubscribe function)
    V2: unsubscribe, // namespace, codes
    lv: reConnect
} = req(8382);

async function getAllData(ticker, freq="DAY1") {
    var data = [];
    var tempData = (await getKLineData(ticker, 500, freq, null))[0];
    if (!tempData.quotation || tempData.quotation.length === 0) {
        return data;
    }
    var earliestfreq = tempData.quotation[tempData.quotation.length - 1].freqTime;
    data.push(...tempData.quotation);
    while (
        (tempData = (await getKLineData(ticker, 500, freq, earliestfreq))[0]) &&
        tempData.quotation && tempData.quotation.length > 0 &&
        earliestfreq > tempData.quotation[tempData.quotation.length - 1].freqTime
    ) {
        if (earliestfreq == tempData.quotation[0].freqTime) data.push(...tempData.quotation.slice(1));
        else data.push(...tempData.quotation);
        earliestfreq = tempData.quotation[tempData.quotation.length - 1].freqTime;
    }
    return data;
}

function data_to_csv(filename, data) {
    var keys = Object.keys(data[0]);
    var csv_content = keys.join(",") + "\n";
    for (var row of data) {
        var row_content = keys.map(k => row[k]).join(",");
        csv_content += row_content + "\n";
    }
    fs.writeFileSync(filename, csv_content);
}

export {
    broadcastSubscribe, // namespace, callback
    closeConnection,
    connectStatusSubscribe, // t
    getContractBaseInfo_version_web,
    getContractCategory_version_web,
    getKLineData, // symbol: "PP_NH", data_count: 1000, quotationFreq: "DAY1", endTime: 1764918000 | null
    getTickData, // symbol: "PP_NH", infoDay: 0 -> today, 1 -> 2days, ... (max 5)
    getTradeStatus, // symbol: "PP_NH"
    getTickData_Detail, // symbol: "PP_NH", data_count: 1000 (only one day minute data)
    getTraderStatus,
    subscribe, // namespace, codes, callback
    unsubscribeAll, // namespace
    subscribeFull, // namespace, codes, callback (return unsubscribe function)
    unsubscribe, // namespace, codes
    reConnect,
    QuotationFreq,
    getAllData,
    data_to_csv
}
// namespace不重要，只是用来分类的，方便unsubscribeAll使用，但是不能为空
// subscribe订阅只和code有关，订阅后立即收到INFO的数据，然后服务器有任何新数据都会回传
// 比如到了分钟时间点，服务器会同时推送最新的分钟数据和INFO数据（似乎TICK和MIN1是一样的）
// 比如到了整点小时，服务器会同时推送MIN60数据、MIN30数据、MIN15数据、MIN5数据、MIN1数据、INFO数据
// callback每次只会收到一个object, 要根据freq字段判断数据频率
// * getAllData获取的INFO数据有freq，但subscribe收到的INFO数据没有freq，所以callback里要判断
