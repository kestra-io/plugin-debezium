package io.kestra.plugin.debezium.mysql;

import io.debezium.connector.mysql.MySqlConnector;
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
    title = "Wait for change data capture event on MySQL server."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "snapshotMode: NEVER",
                "hostname: 127.0.0.1",
                "port: \"3306\"",
                "username: mysql_user",
                "password: mysql_passwd",
                "maxRecords: 100",
            }
        )
    }
)
public class Capture extends AbstractDebeziumTask implements MysqlInterface {
    @Builder.Default
    private MysqlInterface.SnapshotMode snapshotMode = MysqlInterface.SnapshotMode.INITIAL;

    private String serverId;

    @Override
    protected boolean needDatabaseHistory() {
        return true;
    }

    @Override
    protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) throws Exception {
        Properties props = super.properties(runContext, offsetFile, historyFile);

        props.setProperty("connector.class", MySqlConnector.class.getName());

        if (this.serverId != null) {
            props.setProperty("database.server.id", runContext.render(this.serverId));
        }

        if (this.snapshotMode != null) {
            props.setProperty("snapshot.mode", this.snapshotMode.name().toLowerCase(Locale.ROOT));
        }

        return props;
    }
}
