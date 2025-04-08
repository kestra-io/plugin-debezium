package io.kestra.plugin.debezium.postgres;

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
    title = "Consume messages periodically from a PostgreSQL database via change data capture and create one execution per batch.",
    description = "If you would like to consume each message from change data capture in real-time and create one execution per message, you can use the [io.kestra.plugin.debezium.postgres.RealtimeTrigger](https://kestra.io/plugins/plugin-debezium/triggers/io.kestra.plugin.debezium.postgres.realtimetrigger) instead."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Consume a message from a PostgreSQL database via change data capture periodically.",
            code = """"
                id: pg_trigger
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.uris }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.debezium.postgres.Trigger
                    hostname: 127.0.0.1
                    port: "5432"
                    username: "{{ secret('PG_USERNAME') }}"
                    password: "{{ secret('PG_PASSWORD') }}"
                    maxRecords: 100
                    database: my_database
                    pluginName: PGOUTPUT
                    snapshotMode: ALWAYS
            """
        )
    }
)
public class Trigger extends AbstractDebeziumTrigger implements PostgresInterface, AbstractDebeziumInterface {
    protected Property<String> database;

    @Builder.Default
    protected Property<PluginName> pluginName = Property.of(PluginName.PGOUTPUT);

    @Builder.Default
    protected Property<String> slotName = Property.of("kestra");

    @Builder.Default
    protected Property<String> publicationName = Property.of("kestra_publication");

    @Builder.Default
    protected Property<PostgresInterface.SslMode> sslMode = Property.of(SslMode.DISABLE);

    protected Property<String> sslRootCert;

    protected Property<String> sslCert;

    protected Property<String> sslKey;

    protected Property<String> sslKeyPassword;

    @Builder.Default
    private Property<Capture.SnapshotMode> snapshotMode = Property.of(SnapshotMode.INITIAL);

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
            .database(this.database)
            .pluginName(this.pluginName)
            .slotName(this.slotName)
            .publicationName(this.publicationName)
            .sslMode(this.sslMode)
            .sslRootCert(this.sslRootCert)
            .sslCert(this.sslCert)
            .sslKey(this.sslKey)
            .sslKeyPassword(this.sslKeyPassword)
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
