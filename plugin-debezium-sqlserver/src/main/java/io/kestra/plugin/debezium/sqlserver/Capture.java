package io.kestra.plugin.debezium.sqlserver;

import io.debezium.connector.sqlserver.SqlServerConnector;
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
    title = "Wait for change data capture event on Microsoft SQL server"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "hostname: 127.0.0.1",
                "port: 1433",
                "username: sa",
                "password: sqlserver_passwd",
                "maxRecords: 100",
            }
        )
    }
)
public class Capture extends AbstractDebeziumTask implements SqlServerInterface {
    protected String database;

    @Builder.Default
    private SqlServerInterface.SnapshotMode snapshotMode = SqlServerInterface.SnapshotMode.INITIAL;

    @Override
    protected boolean needDatabaseHistory() {
        return true;
    }

    @Override
    protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) throws Exception {
        Properties props = super.properties(runContext, offsetFile, historyFile);

        props.setProperty("connector.class", SqlServerConnector.class.getName());

        SqlServerInterface.handleProperties(props, runContext, this);

        return props;
    }
}
