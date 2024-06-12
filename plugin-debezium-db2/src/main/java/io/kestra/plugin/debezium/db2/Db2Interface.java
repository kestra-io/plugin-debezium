package io.kestra.plugin.debezium.db2;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface Db2Interface {
    @Schema(
        title = "The name of the DB2 database from which to stream the changes."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getDatabase();

    @Schema(
        title = "Specifies the criteria for running a snapshot when the connector starts.",
        description = " Possible settings are:\n" +
            "- `ALWAYS`: The connector performs a snapshot every time that it starts.\n" +
            "- `INITIAL`: The connector runs a snapshot only when no offsets have been recorded for the logical server name.\n" +
            "- `INITIAL_ONLY`: The connector runs a snapshot only when no offsets have been recorded for the logical server name and then stops; i.e. it will not read change events from the binlog.\n" +
            "- `WHEN_NEEDED`: After the connector starts, it performs a snapshot only if it detects one of the following circumstances: 1. It cannot detect any topic offsets. 2. A previously recorded offset specifies a log position that is not available on the server.\n" +
            "- `NO_DATA`: The connector captures the structure of all relevant tables, performing all the steps described in the INITIAL, except that it does not create READ events to represent the data set at the point of the connector’s start-up.\n" +
            "- `RECOVERY`: Set this option to restore a database schema history topic that is lost or corrupted. After a restart, the connector runs a snapshot that rebuilds the topic from the source tables."
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
