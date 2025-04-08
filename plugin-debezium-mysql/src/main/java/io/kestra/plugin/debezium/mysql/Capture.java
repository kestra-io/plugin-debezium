package io.kestra.plugin.debezium.mysql;

import io.debezium.connector.mysql.MySqlConnector;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
    title = "Wait for change data capture event on MySQL server."
)
@Plugin(
    examples = {
        @Example(
            title = "Capture data on MySQL server.",
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
    }
)
public class Capture extends AbstractDebeziumTask implements MysqlInterface {
    @Builder.Default
    private Property<MysqlInterface.SnapshotMode> snapshotMode = Property.of(SnapshotMode.INITIAL);

    @NotNull
    private Property<String> serverId;

    @Override
    protected boolean needDatabaseHistory() {
        return true;
    }

    @Override
    protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) throws Exception {
        Properties props = super.properties(runContext, offsetFile, historyFile);

        props.setProperty("connector.class", MySqlConnector.class.getName());
        props.setProperty("database.server.id", runContext.render(this.serverId).as(String.class).orElse(null));
        props.setProperty("include.schema.changes", "false");

        if (this.snapshotMode != null) {
            props.setProperty("snapshot.mode", runContext.render(this.snapshotMode).as(SnapshotMode.class).orElseThrow().name().toLowerCase(Locale.ROOT));
        }

        return props;
    }
}
