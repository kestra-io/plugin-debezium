package io.kestra.plugin.debezium.postgres;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface PostgresInterface {
    @Schema(
        title = "The name of the PostgreSQL database from which to stream the changes."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getDatabase();

    @Schema(
        title = "The name of the [PostgreSQL logical decoding](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-output-plugin) plug-in installed on the PostgreSQL server.",
        description = "If you are using a `wal2json` plug-in and transactions are very large, the JSON batch event " +
            "that contains all transaction changes might not fit into the hard-coded memory buffer, which has a size " +
            "of 1 GB. In such cases, switch to a streaming plug-in, by setting the plugin-name property to " +
            "`wal2json_streaming` or `wal2json_rds_streaming`. With a streaming plug-in, PostgreSQL sends the " +
            "connector a separate message for each change in a transaction."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    PluginName getPluginName();

    @Schema(
        title = "The name of the PostgreSQL publication created for streaming changes when using `PGOUTPUT`.",
        description = "This publication is created at start-up if it does not already exist and it includes all tables. " +
            "Debezium then applies its own include/exclude list filtering, if configured, to limit the publication to " +
            "change events for the specific tables of interest. The connector user must have superuser permissions to " +
            "create this publication, so it is usually preferable to create the publication before starting the connector for the first time.\n" +
            "\n" +
            "If the publication already exists, either for all tables or configured with a subset of tables, Debezium " +
            "uses the publication as it is defined."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getPublicationName();

    @Schema(
        title = "The name of the PostgreSQL logical decoding slot that was created for streaming changes from a particular plug-in for a particular database/schema.",
        description = "The server uses this slot to stream events to the Debezium connector that you are configuring.\n" +
            "Slot names must conform to [PostgreSQL replication slot naming rules](https://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION), " +
            "which state: \"Each replication slot has a name, which can contain lower-case letters, numbers, and the underscore character.\""
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getSlotName();

    @Schema(
        title = "Whether to use an encrypted connection to the PostgreSQL server. Options include:\n" +
            "- `DISABLE` uses an unencrypted connection.\n" +
            "- `REQUIRE` uses a secure (encrypted) connection, and fails if one cannot be established.\n" +
            "- `VERIFY_CA` behaves like require but also verifies the server TLS certificate against the configured Certificate Authority (CA) certificates, or fails if no valid matching CA certificates are found.\n" +
            "- `VERIFY_FULL` behaves like verify-ca but also verifies that the server certificate matches the host to which the connector is trying to connect.\n\n" +
            "See the [PostgreSQL documentation](https://www.postgresql.org/docs/current/static/libpq-connect.html) for more information."
    )
    @PluginProperty(dynamic = false)
    SslMode getSslMode();

    @Schema(
        title = "The root certificate(s) against which the server is validated.",
        description = "Must be a PEM encoded certificate."
    )
    @PluginProperty(dynamic = true)
    String getSslRootCert();

    @Schema(
        title = "The SSL certificate for the client.",
        description = "Must be a PEM encoded certificate."
    )
    @PluginProperty(dynamic = true)
    String getSslCert();

    @Schema(
        title = "The SSL private key of the client.",
        description = "Must be a PEM encoded key."
    )
    @PluginProperty(dynamic = true)
    String getSslKey();

    @Schema(
        title = "The password to access the client private key `sslKey`."
    )
    @PluginProperty(dynamic = true)
    String getSslKeyPassword();

    @Schema(
        title = "Specifies the criteria for running a snapshot when the connector starts.",
        description = " Possible settings are:\n" +
            "- `INITIAL`: The connector performs a snapshot only when no offsets have been recorded for the logical server name.\n" +
            "- `ALWAYS`: The connector performs a snapshot each time the connector starts.\n" +
            "- `NEVER`: The connector never performs snapshots. When a connector is configured this way, its behavior when it starts is as follows. If there is a previously stored LSN, the connector continues streaming changes from that position. If no LSN has been stored, the connector starts streaming changes from the point in time when the PostgreSQL logical replication slot was created on the server. The never snapshot mode is useful only when you know all data of interest is still reflected in the WAL.\n" +
            "- `INITIAL_ONLY`: The connector performs an initial snapshot and then stops, without processing any subsequent changes.\n"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    SnapshotMode getSnapshotMode();

    enum SnapshotMode {
        INITIAL,
        ALWAYS,
        NEVER,
        INITIAL_ONLY,
    }

    enum SslMode {
        DISABLE,
        REQUIRE,
        VERIFY_CA,
        VERIFY_FULL
    }

    enum PluginName {
        DECODERBUFS,
        WAL2JSON,
        WAL2JSON_RDS,
        WAL2JSON_STREAMING,
        WAL2JSON_RDS_STREAMING,
        PGOUTPUT
    }
}
