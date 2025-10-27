package io.kestra.plugin.debezium.sqlserver;

import io.debezium.connector.sqlserver.SqlServerConnector;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
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
    title = "Wait for change data capture event on Microsoft SQL Server and capture the event as an internal storage file."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for change data capture event on Microsoft SQL Server.",
            full = true,
            code = """
                id: mysql_capture
                namespace: company.team

                tasks:
                  - id: capture
                    type: io.kestra.plugin.debezium.mysql.Capture
                    snapshotMode: NEVER
                    hostname: 127.0.0.1
                    port: "3306"
                    username: "{{ secret('MYSQL_USERNAME') }}"
                    password: "{{ secret('MYSQL_PASSWORD') }}"
                    maxRecords: 100
                """
        )
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            description = "The number of records processed, tagged by source."
        )
    }
)
public class Capture extends AbstractDebeziumTask implements SqlServerInterface {
    protected Property<String> database;

    @Builder.Default
    private Property<SqlServerInterface.SnapshotMode> snapshotMode = Property.ofValue(SnapshotMode.INITIAL);

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
