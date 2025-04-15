package io.kestra.plugin.debezium.oracle;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.plugin.debezium.AbstractDebeziumInterface;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow via an Oracle change data capture event in real-time and create one execution per row.",
    description = "If you would like to consume multiple messages processed within a given time frame and process them in batch, you can use the [io.kestra.plugin.debezium.oracle.Trigger](https://kestra.io/plugins/plugin-debezium/triggers/io.kestra.plugin.debezium.oracle.trigger) instead."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume a message from a Oracle database via change data capture in real-time.",
            full = true,
            code = """
                id: debezium_oracle
                namespace: company.team

                tasks:
                  - id: send_data
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.data }}"

                triggers:
                  - id: realtime
                    type: io.kestra.plugin.debezium.oracle.RealtimeTrigger
                    hostname: 127.0.0.1
                    port: "1521"
                    username: "{{ secret('ORACLE_USERNAME') }}"
                    password: "{{ secret('ORACLE_PASSWORD') }}"
                    sid: ORCLCDB
                """
        )
    }
)
public class RealtimeTrigger extends AbstractDebeziumRealtimeTrigger implements OracleInterface, AbstractDebeziumInterface {
    @Builder.Default
    private Property<SnapshotMode> snapshotMode = Property.of(SnapshotMode.INITIAL);

    private Property<String> sid;

    private Property<String> pluggableDatabase;

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
            .snapshotMode(this.snapshotMode)
            .build();

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map(output -> TriggerService.generateRealtimeExecution(this, conditionContext, context, output));
    }
}
