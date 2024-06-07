package io.kestra.plugin.debezium.cassandra;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface CassandraInterface {
    @Schema(
        title = "Specifies the criteria for running a snapshot when the connector starts.",
        description = " Possible settings are:\n" +
            "- `INITIAL`: The connector runs a snapshot once when initial snapshot will completed, it will no longer perform any additional snapshots.\n" +
            "- `ALWAYS`: The connector runs a snapshot whenever necessary. It check periodically for newly CDC-enabled tables, and snapshot these tables as soon as they are detected.\n" +
            "- `NEVER`: The connector never uses snapshots. Upon first startup with a logical server name, the connector reads from the beginning of the binlog. Configure this behavior with care. It is valid only when the binlog is guaranteed to contain the entire history of the database.\n"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    SnapshotMode getSnapshotMode();

    public enum SnapshotMode {
        INITIAL,
        ALWAYS,
        NEVER
    }
}
