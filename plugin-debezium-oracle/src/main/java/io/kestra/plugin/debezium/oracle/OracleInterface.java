package io.kestra.plugin.debezium.oracle;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface OracleInterface {

    @Schema(
        title = "The name of the database to capture changes from."
    )
    @NotNull
    String getSid();

    @Schema(
        title = "The name of the Oracle pluggable database that the connector captures changes from. Used in container database (CDB) installations only.",
        description = "For non-container database (non-CDB) installation, do not specify the pluggableDatabase property."
    )
    String getPluggableDatabase();

    @Schema(
        title = "Specifies the criteria for running a snapshot when the connector starts.",
        description = " Possible settings are:\n" +
            "- `ALWAYS`: The connector runs a snapshot on each connector start.\n" +
            "- `INITIAL`: The connector runs a snapshot only when no offsets have been recorded for the logical server name.\n" +
            "- `INITIAL_ONLY`: The connector runs a snapshot only when no offsets have been recorded for the logical server name and then stops; i.e. it will not read change events from the binlog.\n" +
            "- `WHEN_NEEDED`: The connector runs a snapshot upon startup whenever it deems it necessary. That is, when no offsets are available, or when a previously recorded offset specifies a binlog location or GTID that is not available in the server.\n" +
            "- `NO_DATA`: The connector runs a snapshot of the schemas and not the data. This setting is useful when you do not need the topics to contain a consistent snapshot of the data but need them to have only the changes since the connector was started.\n" +
            "- `RECOVERY`: This is a recovery setting for a connector that has already been capturing changes. When you restart the connector, this setting enables recovery of a corrupted or lost database history topic. You might set it periodically to \"clean up\" a database history topic that has been growing unexpectedly. Database history topics require infinite retention."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    SnapshotMode getSnapshotMode();

    public enum SnapshotMode {
        ALWAYS,
        INITIAL,
        INITIAL_ONLY,
        WHEN_NEEDED,
        NO_DATA,
        RECOVERY
    }
}
