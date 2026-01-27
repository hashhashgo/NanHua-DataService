import express from 'express';
import { getAllData, getContractBaseInfo_version_web, getContractCategory_version_web } from "./nanhua.js";
import { fileURLToPath } from 'url';

import config from "./config.json" with { type: "json" };

const app = express();

const contract_base_info = await getContractBaseInfo_version_web();
const contract_category = await getContractCategory_version_web();

app.get('/', (req, res) => {
  console.log(req.query);
  if (!Object.keys(req.query).length) { res.send('Nanhua Data Server is running!'); return; }
  if (req.query.ticker === undefined) { res.status(400).send('Ticker parameter is missing'); return; }
  if (req.query.freq === undefined) req.query.freq = "DAY1";
  getAllData(req.query.ticker, req.query.freq).then(data => {
    res.json(data);
  }).catch(err => {
    res.status(500).send('Error retrieving data: ' + err.message);
  });
});

app.get('/contracts', (req, res) => {
  res.json({ base_info: contract_base_info, category: contract_category });
});

if (process.argv && process.argv.length > 1 && process.argv[1] === fileURLToPath(import.meta.url)) {
  var port = 3000;
  if (process.argv.length > 2) port = parseInt(process.argv[2], 10);
  else if (config.dataserver.port) port = config.dataserver.port;
  if (Number.isNaN(port)) port = 3000;
  app.listen(port, () => {
    console.log(`Server is listening at http://localhost:${port}`);
  });
}

export default app;