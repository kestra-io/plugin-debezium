package io.kestra.plugin.debezium.sqlserver;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;

import java.io.IOException;
import java.util.Locale;
import java.util.Properties;
import jakarta.validation.constraints.NotNull;

public interface SqlServerInterface {
    @Schema(
        title = "The name of the Microsoft SQL Server database from which to stream the changes."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getDatabase();

    @Schema(
        title = "Specifies the criteria for running a snapshot when the connector starts.",
        description = " Possible settings are:\n" +
            "- `INITIAL`: Takes a snapshot of structure and data of captured tables; useful if topics should be populated with a complete representation of the data from the captured tables.\n" +
            "- `INITIAL_ONLY`: Takes a snapshot of structure and data like initial but instead does not transition into streaming changes once the snapshot has completed.\n" +
            "- `SCHEMA_ONLY`: Takes a snapshot of the structure of captured tables only; useful if only changes happening from now onwards should be propagated to topics.\n"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    SnapshotMode getSnapshotMode();

    static void handleProperties(Properties properties, RunContext runContext, SqlServerInterface sqlServer) throws IllegalVariableEvaluationException, IOException {
        properties.put("database.dbname", runContext.render(sqlServer.getDatabase()));

        if (sqlServer.getSnapshotMode() != null) {
            properties.setProperty("snapshot.mode", sqlServer.getSnapshotMode().name().toLowerCase(Locale.ROOT));
        }
    }

    public enum SnapshotMode {
        INITIAL,
        INITIAL_ONLY,
        SCHEMA_ONLY,
    }
}
