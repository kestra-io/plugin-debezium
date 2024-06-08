package io.kestra.plugin.debezium.oracle;

import io.debezium.connector.oracle.OracleConnector;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for change data capture event on Oracle server."
)
@Plugin(
    examples = {
        @Example(
            title = "Non-container database (non-CDB)",
            code = {
                "snapshotMode: INITIAL",
                "hostname: 127.0.0.1",
                "port: \"1521\"",
                "username: c##dbzuser",
                "password: dbz",
                "sid: ORCLCDB",
                "maxRecords: 100",
            }
        ),
        @Example(
            title = "Container database (CDB)",
            code = {
                "snapshotMode: INITIAL",
                "hostname: 127.0.0.1",
                "port: \"1521\"",
                "username: c##dbzuser",
                "password: dbz",
                "sid: ORCLCDB",
                "pluggableDatabase: ORCLPDB1",
                "maxRecords: 100",
            }
        )
    }
)
public class Capture extends AbstractDebeziumTask implements OracleInterface {
    @Builder.Default
    private OracleInterface.SnapshotMode snapshotMode = OracleInterface.SnapshotMode.INITIAL;

    private String sid;

    private String pluggableDatabase;

    @Override
    protected boolean needDatabaseHistory() {
        return true;
    }

    @Override
    protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) throws Exception {
        Properties props = super.properties(runContext, offsetFile, historyFile);

        props.setProperty("connector.class", OracleConnector.class.getName());

        props.setProperty("database.dbname", runContext.render(this.sid.toUpperCase(Locale.ROOT)));

        if (this.pluggableDatabase != null) {
            props.setProperty("database.pdb.name", runContext.render(this.pluggableDatabase.toUpperCase(Locale.ROOT)));
        }
        props.setProperty("include.schema.changes", "false");
        props.setProperty("schema.history.internal.store.only.captured.tables.ddl", "false");

        if (this.snapshotMode != null) {
            props.setProperty("snapshot.mode", this.snapshotMode.name().toLowerCase(Locale.ROOT));
        }

        return props;
    }
}
