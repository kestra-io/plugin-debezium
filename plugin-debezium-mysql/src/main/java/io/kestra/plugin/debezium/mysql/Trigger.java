package io.kestra.plugin.debezium.mysql;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.debezium.AbstractDebeziumInterface;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumTrigger;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
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
    title = "Trigger a flow via a MySQL change data capture event periodically and create one execution per row.",
    description = "If you would like to consume each message from change data capture in real-time and create one execution per message, you can use the [io.kestra.plugin.debezium.mysql.RealtimeTrigger](https://kestra.io/plugins/plugin-debezium/triggers/io.kestra.plugin.debezium.mysql.realtimetrigger) instead."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume a message from a MySQL database via change data capture periodically.",
            full = true,
            code = """
                id: debezium_mysql
                namespace: company.team

                tasks:
                  - id: send_data
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.uris }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.debezium.mysql.Trigger
                    serverId: "123456789"
                    snapshotMode: NEVER
                    hostname: 127.0.0.1
                    port: "3306"
                    username: "{{ secret('MYSQL_USERNAME') }}"
                    password: "{{ secret('MYSQL_PASSWORD') }}"
                    offsetsCommitMode: ON_EACH_BATCH
                """
        )
    }
)
public class Trigger extends AbstractDebeziumTrigger implements MysqlInterface, AbstractDebeziumInterface {
    @Builder.Default
    private Property<SnapshotMode> snapshotMode = Property.ofValue(SnapshotMode.INITIAL);

    private Property<String> serverId;

    @Schema(
        title = "How to commit the offsets to the KV Store.",
        description = """
            - ON_EACH_BATCH: after each batch of records consumed by this trigger, the offsets will be stored in the KV Store. This avoids any duplicated records being consumed but can be costly if many events are produced.
            - ON_STOP: when this trigger is stopped or killed, the offsets will be stored in the KV Store. This avoid any un-necessary writes to the KV Store, but if the trigger is not stopped gracefully, the KV Store value may not be updated leading to duplicated records consumption."""
    )
    @Builder.Default
    private Property<AbstractDebeziumRealtimeTrigger.OffsetCommitMode> offsetsCommitMode = Property.ofValue(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_EACH_BATCH);

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
            .offsetsCommitMode(this.offsetsCommitMode)
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
