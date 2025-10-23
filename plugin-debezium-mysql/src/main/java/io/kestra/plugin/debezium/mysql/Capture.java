package io.kestra.plugin.debezium.mysql;

import io.debezium.connector.mysql.MySqlConnector;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
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
    title = "Wait for a change data capture event on MySQL server and capture the event as an internal storage file."
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
                    offsetsCommitMode: ON_EACH_BATCH
                """
        )
    }
)
public class Capture extends AbstractDebeziumTask implements MysqlInterface {
    @Builder.Default
    private Property<MysqlInterface.SnapshotMode> snapshotMode = Property.ofValue(SnapshotMode.INITIAL);

    @NotNull
    private Property<String> serverId;

    @Schema(
        title = "How to commit the offsets to the KV Store.",
        description = """
            - ON_EACH_BATCH: after each batch of records consumed by this task, the offsets will be stored in the KV Store. This avoids any duplicated records being consumed but can be costly if many events are produced.
            - ON_STOP: when this task is stopped or killed, the offsets will be stored in the KV Store. This avoid any un-necessary writes to the KV Store, but if the task is not stopped gracefully, the KV Store value may not be updated leading to duplicated records consumption."""
    )
    @Builder.Default
    private Property<AbstractDebeziumRealtimeTrigger.OffsetCommitMode> offsetsCommitMode = Property.ofValue(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_EACH_BATCH);

    @Override
    protected boolean needDatabaseHistory() {
        return true;
    }

    @Override
    public AbstractDebeziumTask.Output run(RunContext runContext) throws Exception {
        AbstractDebeziumRealtimeTrigger.OffsetCommitMode mode = null;
        if (this.offsetsCommitMode != null) {
            mode = runContext.render(this.offsetsCommitMode).as(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.class).orElse(null);
        }
        return super.run(runContext, mode);
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
