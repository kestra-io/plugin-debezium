package io.kestra.plugin.debezium;

import io.kestra.core.models.annotations.PluginProperty;
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

    protected Object includedDatabases;

    protected Object excludedDatabases;

    protected Object includedTables;

    protected Object excludedTables;

    protected Object includedColumns;

    protected Object excludedColumns;

    protected Map<String, String> properties;

    @Builder.Default
    protected String stateName = "debezium-state";

    @Schema(
        title = "The maximum number of rows to fetch before stopping.",
        description = "It's not an hard limit and is evaluated every second."
    )
    @PluginProperty
    protected Integer maxRecords;

    @Schema(
        title = "The maximum duration waiting for new rows.",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    @PluginProperty
    protected Duration maxDuration;

    @Schema(
        title = "The maximum total processing duration.",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    @PluginProperty
    @Builder.Default
    protected Duration maxWait = Duration.ofSeconds(10);

    @Schema(
        title = "The maximum duration waiting for the snapshot to ends.",
        description = "It's not an hard limit and is evaluated every second.\n The properties 'maxRecord', 'maxDuration' and 'maxWait' are evaluated only after the snapshot is done."
    )
    @PluginProperty
    @Builder.Default
    protected Duration maxSnapshotDuration = Duration.ofHours(1);
}
