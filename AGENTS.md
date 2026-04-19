# Kestra Debezium Plugin

## What

- Provides plugin components under `io.kestra.plugin`.
- Keeps the implementation focused on the integration scope exposed by this repository.

## Why

- What user problem does this solve? Teams need a reliable way to operate Debezium from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Debezium steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Debezium.

## How

### Architecture

This is a **multi-module** plugin with 7 submodules:

- `plugin-debezium`
- `plugin-debezium-db2`
- `plugin-debezium-mongodb`
- `plugin-debezium-mysql`
- `plugin-debezium-oracle`
- `plugin-debezium-postgres`
- `plugin-debezium-sqlserver`

Infrastructure dependencies (Docker Compose services):

- `mongodb`
- `mysql`
- `oracle`
- `postgres`
- `sqlserver`

### Key Plugin Classes

**plugin-debezium-db2:**

- `io.kestra.plugin.debezium.db2.Capture`
- `io.kestra.plugin.debezium.db2.RealtimeTrigger`
- `io.kestra.plugin.debezium.db2.Trigger`
**plugin-debezium-mongodb:**

- `io.kestra.plugin.debezium.mongodb.Capture`
- `io.kestra.plugin.debezium.mongodb.RealtimeTrigger`
- `io.kestra.plugin.debezium.mongodb.Trigger`
**plugin-debezium-mysql:**

- `io.kestra.plugin.debezium.mysql.Capture`
- `io.kestra.plugin.debezium.mysql.RealtimeTrigger`
- `io.kestra.plugin.debezium.mysql.Trigger`
**plugin-debezium-oracle:**

- `io.kestra.plugin.debezium.oracle.Capture`
- `io.kestra.plugin.debezium.oracle.RealtimeTrigger`
- `io.kestra.plugin.debezium.oracle.Trigger`
**plugin-debezium-postgres:**

- `io.kestra.plugin.debezium.postgres.Capture`
- `io.kestra.plugin.debezium.postgres.RealtimeTrigger`
- `io.kestra.plugin.debezium.postgres.Trigger`
**plugin-debezium-sqlserver:**

- `io.kestra.plugin.debezium.sqlserver.Capture`
- `io.kestra.plugin.debezium.sqlserver.RealtimeTrigger`
- `io.kestra.plugin.debezium.sqlserver.Trigger`

### Project Structure

```
plugin-debezium/
├── plugin-debezium/
│   └── src/main/java/...
├── ...                                    # Other submodules
├── build.gradle
├── settings.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
