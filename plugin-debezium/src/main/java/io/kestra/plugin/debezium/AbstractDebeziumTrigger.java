package io.kestra.plugin.debezium;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDebeziumTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<AbstractDebeziumTask.Output> {
    @Builder.Default
    protected final Duration interval = Duration.ofSeconds(60);

    @Builder.Default
    protected Property<AbstractDebeziumTask.Format> format = Property.of(AbstractDebeziumTask.Format.INLINE);

    @Builder.Default
    protected Property<AbstractDebeziumTask.Deleted> deleted = Property.of(AbstractDebeziumTask.Deleted.ADD_FIELD);

    @Builder.Default
    protected Property<String> deletedFieldName = Property.of("deleted");

    @Builder.Default
    protected Property<AbstractDebeziumTask.Key> key = Property.of(AbstractDebeziumTask.Key.ADD_FIELD);

    @Builder.Default
    protected Property<AbstractDebeziumTask.Metadata> metadata = Property.of(AbstractDebeziumTask.Metadata.ADD_FIELD);

    @Builder.Default
    protected Property<String> metadataFieldName = Property.of("metadata");

    @Builder.Default
    protected Property<AbstractDebeziumTask.SplitTable> splitTable = Property.of(AbstractDebeziumTask.SplitTable.TABLE);

    @Builder.Default
    protected Property<Boolean> ignoreDdl = Property.of(true);

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
    protected Property<String> stateName = Property.of("debezium-state");

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
    protected Property<Duration> maxWait = Property.of(Duration.ofSeconds(10));

    @Schema(
        title = "The maximum duration waiting for the snapshot to ends.",
        description = "It's not an hard limit and is evaluated every second.\n The properties 'maxRecord', 'maxDuration' and 'maxWait' are evaluated only after the snapshot is done."
    )
    @Builder.Default
    protected Property<Duration> maxSnapshotDuration = Property.of(Duration.ofHours(1));
}
