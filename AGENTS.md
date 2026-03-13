# Kestra Debezium Plugin

## What

Kestra plugin providing integration with Debezium. Exposes 18 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Debezium, allowing orchestration of Debezium-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Run a specific submodule's tests
./gradlew :plugin-debezium:test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
