/*
This file is used to connect to the database when initiallizing.
You can write schema creation and data insertion code here.

Also, if using databases like ClickHouse, you cannot insert history data directly,
because tables are usually partitioned by date, and inserting history data may cause 
"Too many partitions for single INSERT block (more than 100)" error.

You should export two functions:
- `initialize`: to initialize the database, create tables, etc.
- `backfill`: to backfill historical data, which will be called after `initialize`.

The `backfill` function will receive an array of historical data, and you can insert them into the database in batches.

When using docker-compose, this file can be overridden by mounting a local file to `/app/db.js` in the container,
so you can easily customize the database initialization and backfilling logic.
Don't forget to install necessary database client libraries in `package.json` if you need to connect to a database.
*/

export async function initialize() {
    // Initialize the database, create tables, etc.
    console.log("Database initialized");
}

export async function backfill(data) {
    // Backfill historical data, which will be called after `initialize`.
    // The `data` parameter is an array of historical data, you can insert them into the database in batches.
    console.log(`Backfilling ${data.length} records`);
}