package io.kestra.plugin.debezium.oracle;

import io.debezium.connector.oracle.OracleConnector;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class Capture extends AbstractDebeziumTask {
    @Schema(
        title = "Name of the database to connect to.",
        description = "Must be the CDB name when working with the CDB + PDB model."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String database;

    @Schema(
        title = "Specifies the criteria for running a snapshot when the connector starts.",
        description = " Possible settings are:\n" +
            "- `INITIAL`: The snapshot includes the structure and data of captured tables. Specify this value to populate topics with a complete representation of the data from the captured tables.\n" +
            "- `SCHEMA_ONLY`: The snapshot includes only the structure of captured tables. Specify this value if you want the connector to capture data only for changes that occur after the snapshot.\n" +
            "- `SCHEMA_ONLY_RECOVERY`: This is a recovery setting for a connector that has already been capturing changes. When you restart the connector, this setting enables recovery of a corrupted or lost database history topic. You might set it periodically to \"clean up\" a database history topic that has been growing unexpectedly. Database history topics require infinite retention. Note this mode is only safe to be used when it is guaranteed that no schema changes happened since the point in time the connector was shut down before and the point in time the snapshot is taken.\n"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    @Builder.Default
    private SnapshotMode snapshotMode = SnapshotMode.INITIAL;

    @Override
    protected boolean needDatabaseHistory() {
        return true;
    }

    @Override
    protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) throws Exception {
        Properties props = super.properties(runContext, offsetFile, historyFile);

        props.setProperty("connector.class", OracleConnector.class.getName());

        props.put("database.dbname", runContext.render(database));

        props.put("database.pdb.name", "XEPDB1");

        if (this.snapshotMode != null) {
            props.setProperty("snapshot.mode", this.snapshotMode.name().toLowerCase(Locale.ROOT));
        }

        return props;
    }

    @Introspected
    public enum SnapshotMode {
        INITIAL,
        SCHEMA_ONLY,
        SCHEMA_ONLY_RECOVERY,
    }
}
