package io.kestra.plugin.debezium.mysql;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface MysqlInterface {
    @Schema(
        title = "Specifies the criteria for running a snapshot when the connector starts.",
        description = "Possible settings are:\n" +
            "- `INITIAL`: The connector runs a snapshot only when no offsets have been recorded for the logical server name.\n" +
            "- `INITIAL_ONLY`: The connector runs a snapshot only when no offsets have been recorded for the logical server name and then stops; i.e. it will not read change events from the binlog.\n" +
            "- `WHEN_NEEDED`: The connector runs a snapshot upon startup whenever it deems it necessary. That is, when no offsets are available, or when a previously recorded offset specifies a binlog location or GTID that is not available in the server.\n" +
            "- `NEVER`: The connector never uses snapshots. Upon first startup with a logical server name, the connector reads from the beginning of the binlog. Configure this behavior with care. It is valid only when the binlog is guaranteed to contain the entire history of the database.\n" +
            "- `SCHEMA_ONLY` (**deprecated**): The connector runs a snapshot of the schemas and not the data. This setting is useful when you do not need the topics to contain a consistent snapshot of the data but need them to have only the changes since the connector was started. Use `NO_DATA` instead.\n" +
            "- `SCHEMA_ONLY_RECOVERY` (**deprecated**): This is a recovery setting for a connector that has already been capturing changes. When you restart the connector, this setting enables recovery of a corrupted or lost database history topic. You might set it periodically to \"clean up\" a database history topic that has been growing unexpectedly. Database history topics require infinite retention. Use `RECOVERY` instead.\n" +
            "- `ALWAYS`: The connector performs a snapshot every time that it starts.\n" +
            "- `NO_DATA`: The connector runs a snapshot that captures only the schema, but not any table data. Use this option if you do not need the topics to contain a consistent snapshot of the data.\n" +
            "- `RECOVERY`: Restores a database schema history topic that is lost or corrupted by rebuilding it from the source tables.\n"
    )
    @NotNull
    Property<SnapshotMode> getSnapshotMode();

    @Schema(
        title = "A numeric ID of this database client.",
        description = "This must be unique across all currently-running database processes in the MySQL cluster. " +
            "This connector joins the MySQL database cluster as another server (with this unique ID) so it can read " +
            "the binlog. By default, a random number between 5400 and 6400 is generated, though the recommendation " +
            "is to explicitly set a value."
    )
    @PluginProperty(dynamic = true)
    Property<String> getServerId();

    public enum SnapshotMode {
        INITIAL,
        INITIAL_ONLY,
        WHEN_NEEDED,
        NEVER,
        SCHEMA_ONLY,
        SCHEMA_ONLY_RECOVERY,
        NO_DATA,
        ALWAYS,
        RECOVERY
    }
}
