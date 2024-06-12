package io.kestra.plugin.debezium.oracle;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.debezium.AbstractDebeziumInterface;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages periodically from a Oracle database via change data capture and create one execution per batch.",
    description = "If you would like to consume each message from change data capture in real-time and create one execution per message, you can use the [io.kestra.plugin.debezium.oracle.RealtimeTrigger](https://kestra.io/plugins/plugin-debezium/triggers/io.kestra.plugin.debezium.oracle.realtimetrigger) instead."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "snapshotMode: INITIAL_ONLY",
                "hostname: 127.0.0.1",
                "port: \"1521\"",
                "username: c##dbzuser",
                "password: dbz",
                "sid: ORCLCDB",
                "maxRecords: 100",
            }
        )
    }
)
public class Trigger extends AbstractDebeziumTrigger implements OracleInterface, AbstractDebeziumInterface {
    @Builder.Default
    private OracleInterface.SnapshotMode snapshotMode = OracleInterface.SnapshotMode.INITIAL;

    private String sid;

    private String pluggableDatabase;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

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
            .sid(this.sid)
            .pluggableDatabase(this.pluggableDatabase)
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
            .build();
        AbstractDebeziumTask.Output run = task.run(runContext);

        if (logger.isDebugEnabled()) {
            logger.debug("Found '{}' messages", run.getSize());
        }

        if (run.getSize() == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }
}
