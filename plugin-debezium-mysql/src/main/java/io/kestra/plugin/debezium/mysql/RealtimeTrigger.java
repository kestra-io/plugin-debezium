package io.kestra.plugin.debezium.mysql;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.plugin.debezium.AbstractDebeziumInterface;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "React for change data capture event on MySQL server and create new execution."
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
    },
    beta = true
)
public class RealtimeTrigger extends AbstractDebeziumRealtimeTrigger implements MysqlInterface, AbstractDebeziumInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Builder.Default
    protected AbstractDebeziumTask.Format format = AbstractDebeziumTask.Format.INLINE;

    @Builder.Default
    protected AbstractDebeziumTask.Deleted deleted = AbstractDebeziumTask.Deleted.ADD_FIELD;

    @Builder.Default
    protected String deletedFieldName = "deleted";

    @Builder.Default
    protected AbstractDebeziumTask.Key key = AbstractDebeziumTask.Key.ADD_FIELD;

    @Builder.Default
    protected AbstractDebeziumTask.Metadata metadata = AbstractDebeziumTask.Metadata.ADD_FIELD;

    @Builder.Default
    protected String metadataFieldName = "metadata";

    @Builder.Default
    protected AbstractDebeziumTask.SplitTable splitTable = AbstractDebeziumTask.SplitTable.TABLE;

    @Builder.Default
    protected Boolean ignoreDdl = true;

    protected String hostname;

    protected String port;

    protected String username;

    protected String password;

    private Object includedDatabases;

    private Object excludedDatabases;

    private Object includedTables;

    private Object excludedTables;

    private Object includedColumns;

    private Object excludedColumns;

    private Map<String, String> properties;

    @Builder.Default
    protected String stateName = "debezium-state";

    private Integer maxRecords;

    private Duration maxDuration;

    @Builder.Default
    private Duration maxWait = Duration.ofSeconds(10);

    @Builder.Default
    private Duration maxSnapshotDuration = Duration.ofHours(1);

    @Builder.Default
    private SnapshotMode snapshotMode = SnapshotMode.INITIAL;

    private String serverId;

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        Capture task = Capture.builder()
            .id(this.id)
            .type(Capture.class.getName())
            .format(this.format)
            .deleted(this.deleted)
            .deletedFieldName(this.deletedFieldName)
            .key(this.key)
            .metadata(this.metadata)
            .metadataFieldName(this.metadataFieldName)
            .splitTable(this.splitTable)
            .ignoreDdl(this.ignoreDdl)
            .hostname(this.hostname)
            .port(this.port)
            .username(this.username)
            .password(this.password)
            .includedDatabases(this.includedDatabases)
            .excludedDatabases(this.excludedDatabases)
            .includedTables(this.includedTables)
            .excludedTables(this.excludedTables)
            .includedColumns(this.includedColumns)
            .excludedColumns(this.excludedColumns)
            .properties(this.properties)
            .stateName(this.stateName)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .maxWait(this.maxWait)
            .maxSnapshotDuration(this.maxSnapshotDuration)
            .snapshotMode(this.snapshotMode)
            .serverId(this.serverId)
            .build();

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map(output -> TriggerService.generateRealtimeExecution(this, context, output));
    }
}
