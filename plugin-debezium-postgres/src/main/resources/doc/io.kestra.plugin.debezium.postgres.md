# How to use the Debezium PostgreSQL plugin

Stream change data capture (CDC) events from PostgreSQL using [Debezium](https://debezium.io/) and write them to Kestra's internal storage.

## Tasks

- `Capture`: run a one-off capture that collects CDC events until a record count, duration, or wait limit is reached, then writes them to internal storage.
- `Trigger`: poll for CDC events on a schedule and start a flow when new events arrive.
- `RealtimeTrigger`: stream CDC events continuously and start one execution per event.

## Connection

Provide the PostgreSQL connection details (hostname, port, username, password, database) via [Kestra secrets](https://kestra.io/docs/concepts/secret) for credentials. PostgreSQL requires a logical replication slot and wal_level set to logical.

## Notes

Debezium tracks progress with an offset and database history stored under a state name, so a restarted task resumes from the last committed position rather than re-reading the whole log from the start.

## Replication Slots and Operational Considerations

Each task or trigger automatically derives a unique, isolated PostgreSQL replication slot (`kestra_<hash>`) based on its namespace, flow ID, and task ID, unless explicitly configured via `slotName`. For backward compatibility, existing tasks with pre-existing offset state continue to use the legacy default slot (`kestra`).

### WAL Retention and Resource Sizing

PostgreSQL logical replication slots retain write-ahead logs (WAL) on the database server until consumed and acknowledged. If a replication slot becomes inactive or is orphaned, PostgreSQL will hold WAL files on disk indefinitely, which can lead to disk space exhaustion.

Because each CDC task or trigger now provisions its own isolated replication slot by default, ensure your PostgreSQL server configuration is sized accordingly:
- `max_replication_slots`: Must be set high enough to accommodate the total number of concurrent tasks and triggers across all flows connecting to the database.
- `max_wal_senders`: Must be equal to or greater than `max_replication_slots` to allow concurrent streaming connections.

### Post-Upgrade and Lifecycle Maintenance

Administrators should periodically inspect PostgreSQL's `pg_replication_slots` view:
```sql
SELECT slot_name, plugin, active, wal_status FROM pg_replication_slots;
```
Orphaned or unused replication slots may remain behind in scenarios such as:
- Upgrading to this plugin version when a previous deployment created the legacy `kestra` slot that is no longer used.
- Renaming a flow or renaming a task (which causes a new slot name to be derived).
- Deleting or retiring a flow or task.

Genuinely obsolete or orphaned slots should be dropped manually using PostgreSQL's replication slot cleanup function:
```sql
SELECT pg_drop_replication_slot('slot_name_to_remove');
```
