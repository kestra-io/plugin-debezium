package io.kestra.plugin.debezium.sqlserver;

import io.debezium.connector.sqlserver.SqlServerConnector;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
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
    title = "Wait for change data capture event on Microsoft SQL Server."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "snapshotMode: INITIAL",
                "hostname: 127.0.0.1",
                "port: \"1433\"",
                "username: sqlserver_user",
                "password: sqlserver_passwd",
                "maxRecords: 100",
            }
        )
    }
)
public class Capture extends AbstractDebeziumTask implements SqlServerInterface {
    protected Property<String> database;

    @Builder.Default
    private Property<SqlServerInterface.SnapshotMode> snapshotMode = Property.of(SnapshotMode.INITIAL);

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
