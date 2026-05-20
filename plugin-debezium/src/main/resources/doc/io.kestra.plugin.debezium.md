# How to use the Debezium plugin

Capture change data (CDC) from relational databases and MongoDB into Kestra flows using Debezium connectors.

## Common properties

Set `hostname`, `port`, `username`, and `password` to connect to the source database. Control output shape with `format` (`INLINE` by default, also `RAW` or `WRAP`), `deleted` (how to handle deletes — `ADD_FIELD` by default), `key` and `metadata` (both `ADD_FIELD` by default), and `splitTable` (`TABLE` by default — splits output by `database.table`). Filter captured data with `includedDatabases`, `excludedDatabases`, `includedTables`, `excludedTables`, `includedColumns`, and `excludedColumns`. Bound capture batches with `maxRecords`, `maxDuration`, and `maxWait` (default 10 seconds). Control snapshotting with `snapshotMode` (default `INITIAL` for all databases). Offset state is stored in Kestra's KV store under `stateName` (default `debezium-state`).

Apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Database-specific required properties

Each database connector requires one additional identifier:

- **MySQL** — set `serverId` (required, a unique numeric client ID for the MySQL binary log)
- **PostgreSQL** — set `database` (required). Configure `pluginName` (default `PGOUTPUT`), `slotName` (default `kestra`), `publicationName` (default `kestra_publication`), and `sslMode` (default `DISABLE`) with optional SSL certificate fields
- **SQL Server** — set `database` (required)
- **Oracle** — set `sid` (required, the System Identifier or CDB name). Optionally set `pluggableDatabase`
- **MongoDB** — set `connectionString` (required, the full MongoDB connection URI) instead of hostname/port/username/password. Filter by `includedCollections` and `excludedCollections`
- **DB2** — set `database` (required)

## Tasks

Each database has a `Capture` task that reads a bounded batch of change events and writes them to Kestra's internal storage. The output includes `size` (row count), `uris` (map of table name → file URI), `stateOffsetKey`, and `stateHistoryKey`.

## Triggers

Each database has a `Trigger` (polling, runs `Capture` on an interval — default 60 seconds, starts one execution per batch) and a `RealtimeTrigger` (streams change events as they arrive, starts one execution per event). The `RealtimeTrigger` supports `offsetsCommitMode`: `ON_STOP` (default) or `ON_EACH_BATCH`.
