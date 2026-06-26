# How to use the Debezium Db2 plugin

Stream change data capture (CDC) events from Db2 using [Debezium](https://debezium.io/) and write them to Kestra's internal storage.

## Tasks

- `Capture`: run a one-off capture that collects CDC events until a record count, duration, or wait limit is reached, then writes them to internal storage.
- `Trigger`: poll for CDC events on a schedule and start a flow when new events arrive.
- `RealtimeTrigger`: stream CDC events continuously and start one execution per event.

## Connection

Provide the Db2 connection details (hostname, port, username, password, database) via [Kestra secrets](https://kestra.io/docs/concepts/secret) for credentials. Db2 requires change data capture to be enabled on the captured tables.

## Notes

Debezium tracks progress with an offset and database history stored under a state name, so a restarted task resumes from the last committed position rather than re-reading the whole log from the start.
