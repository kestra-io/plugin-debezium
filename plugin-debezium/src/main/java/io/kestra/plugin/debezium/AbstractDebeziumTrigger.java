package io.kestra.plugin.debezium;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Map;

import static io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger.OffsetCommitMode;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDebeziumTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<AbstractDebeziumTask.Output> {
    @Builder.Default
    protected final Duration interval = Duration.ofSeconds(60);

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

    protected Property<String> hostname;

    protected Property<String> port;

    protected Property<String> username;

    protected Property<String> password;

    protected Object includedDatabases;

    protected Object excludedDatabases;

    protected Object includedTables;

    protected Object excludedTables;

    protected Object includedColumns;

    protected Object excludedColumns;

    protected Property<Map<String, String>> properties;

    @Builder.Default
    protected Property<String> stateName = Property.ofValue("debezium-state");

    @Schema(
        title = "The maximum number of rows to fetch before stopping.",
        description = "It's not an hard limit and is evaluated every second."
    )
    protected Property<Integer> maxRecords;

    @Schema(
        title = "The maximum duration waiting for new rows.",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    protected Property<Duration> maxDuration;

    @Schema(
        title = "The maximum total processing duration.",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    @Builder.Default
    protected Property<Duration> maxWait = Property.ofValue(Duration.ofSeconds(10));

    @Schema(
        title = "The maximum duration waiting for the snapshot to ends.",
        description = "It's not an hard limit and is evaluated every second.\n The properties 'maxRecord', 'maxDuration' and 'maxWait' are evaluated only after the snapshot is done."
    )
    @Builder.Default
    protected Property<Duration> maxSnapshotDuration = Property.ofValue(Duration.ofHours(1));

    @Schema(
        title = "When to commit the offsets to the KV Store.",
        description = """
            - `ON_EACH_BATCH`: after each batch of records consumed by this trigger, the offsets will be stored in the KV Store. This avoids any duplicated records being consumed but can be costly if many events are produced.
            - `ON_STOP`: when this trigger is stopped or killed, the offsets will be stored in the KV Store. This avoids any un-necessary writes to the KV Store, but if the trigger is not stopped gracefully, the KV Store value may not be updated leading to duplicated records consumption.
            """
    )
    @Builder.Default
    protected Property<OffsetCommitMode> offsetsCommitMode = Property.ofValue(OffsetCommitMode.ON_STOP);
}
