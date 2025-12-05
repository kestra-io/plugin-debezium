package io.kestra.plugin.debezium.mongodb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.debezium.AbstractDebeziumInterface;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow on a change data capture event in MongoDB and create new execution per batch.",
    description = "If you would like to consume each message from change data capture in real-time and create one execution per message, you can use the [io.kestra.plugin.debezium.mongodb.RealtimeTrigger](https://kestra.io/plugins/plugin-debezium/triggers/io.kestra.plugin.debezium.mongodb.realtimetrigger) instead."

)
@Plugin(
    examples = {
        @Example(
            title = "Sharded connection.",
            full = true,
            code = """
                id: debezium_mongodb
                namespace: company.team

                tasks:
                  - id: send_data
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.uris }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.debezium.mongodb.Trigger
                    snapshotMode: INITIAL
                    connectionString: "mongodb://mongo_user:{{secret('MONGO_PASSWORD')}}@mongos0.example.com:27017,mongos1.example.com:27017/"
                """
        ),
        @Example(
            title = "Replica set connection.",
            full = true,
            code = """
                id: debezium_mongodb
                namespace: company.team

                tasks:
                  - id: send_data
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.uris }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.debezium.mongodb.Trigger
                    snapshotMode: INITIAL
                    connectionString: "mongodb://mongo_user:{{secret('MONGO_PASSWORD')}}@mongodb0.example.com:27017/?replicaSet=rs0"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<AbstractDebeziumTask.Output>, MongodbInterface, AbstractDebeziumInterface {

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Builder.Default
    protected Property<AbstractDebeziumTask.Format> format = Property.ofValue(AbstractDebeziumTask.Format.INLINE);

    @Builder.Default
    protected Property<AbstractDebeziumTask.Deleted> deleted = Property.ofValue(AbstractDebeziumTask.Deleted.ADD_FIELD);

    @Builder.Default
    protected Property<String> deletedFieldName = Property.ofValue("deleted");

    @Builder.Default
    protected Property<AbstractDebeziumTask.Key> key = Property.ofValue(AbstractDebeziumTask.Key.ADD_FIELD);

    @Builder.Default
    protected Property<AbstractDebeziumTask.Metadata> metadata = Property.ofValue(AbstractDebeziumTask.Metadata.ADD_FIELD);

    @Builder.Default
    protected Property<String> metadataFieldName = Property.ofValue("metadata");

    @Builder.Default
    protected Property<AbstractDebeziumTask.SplitTable> splitTable = Property.ofValue(AbstractDebeziumTask.SplitTable.TABLE);

    @Builder.Default
    protected Property<Boolean> ignoreDdl = Property.ofValue(true);

    @NotNull
    private Property<String> connectionString;

    @Builder.Default
    protected Property<String> hostname = Property.ofValue("");

    @Builder.Default
    protected Property<String> port = Property.ofValue("");

    protected Property<String> username;

    protected Property<String> password;

    private Object includedDatabases;

    private Object excludedDatabases;

    private Object includedTables;

    private Object excludedTables;

    private Object includedCollections;

    private Object excludedCollections;

    private Object includedColumns;

    private Object excludedColumns;

    private Property<Map<String, String>> properties;

    @Builder.Default
    protected Property<String> stateName = Property.ofValue("debezium-state");

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

    @Builder.Default
    private Property<Duration> maxWait = Property.ofValue(Duration.ofSeconds(10));

    @Builder.Default
    private Property<MongodbInterface.SnapshotMode> snapshotMode = Property.ofValue(SnapshotMode.INITIAL);

    @Schema(
        title = "When to commit the offsets to the KV Store.",
        description = """
            - `ON_EACH_BATCH`: after each batch of records consumed by this trigger, the offsets will be stored in the KV Store. This avoids any duplicated records being consumed but can be costly if many events are produced.
            - `ON_STOP`: when this trigger is stopped or killed, the offsets will be stored in the KV Store. This avoids any un-necessary writes to the KV Store, but if the trigger is not stopped gracefully, the KV Store value may not be updated leading to duplicated records consumption.
            """
    )
    @Builder.Default
    protected Property<AbstractDebeziumRealtimeTrigger.OffsetCommitMode> offsetsCommitMode = Property.ofValue(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_STOP);

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
            .includedDatabases(this.includedDatabases)
            .excludedDatabases(this.excludedDatabases)
            .includedCollections(this.includedCollections)
            .excludedCollections(this.excludedCollections)
            .includedColumns(this.includedColumns)
            .excludedColumns(this.excludedColumns)
            .properties(this.properties)
            .stateName(this.stateName)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .maxWait(this.maxWait)
            .offsetsCommitMode(this.offsetsCommitMode)
            .snapshotMode(this.snapshotMode)
            .connectionString(this.connectionString)
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
