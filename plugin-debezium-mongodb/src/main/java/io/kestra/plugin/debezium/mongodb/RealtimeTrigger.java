package io.kestra.plugin.debezium.mongodb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.plugin.debezium.AbstractDebeziumInterface;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
    title = "Consume a message in real-time from a MongoDB database via change data capture and create one execution per row."
)
@Plugin(
    examples = {
        @Example(
            title = "Sharded connection",
            full = true,
            code = """
                id: debezium-mongodb
                namespace: company.myteam

                tasks:
                  - id: send_data
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.data }}"

                triggers:
                  - id: realtime
                    type: io.kestra.plugin.debezium.mongodb.RealtimeTrigger
                    snapshotMode: INITIAL
                    connectionString: mongodb://mongo_user:mongo_passwd@mongos0.example.com:27017,mongos1.example.com:27017/
                """
        ),
        @Example(
            title = "Replica set connection",
            full = true,
            code = """
                id: debezium-mongodb
                namespace: company.myteam

                tasks:
                  - id: send_data
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.data }}"

                triggers:
                  - id: realtime
                    type: io.kestra.plugin.debezium.mongodb.RealtimeTrigger
                    snapshotMode: INITIAL
                    connectionString: mongodb://mongo_user:mongo_passwd@mongodb0.example.com:27017/?replicaSet=rs0
                """
        )
    }
)
public class RealtimeTrigger extends AbstractDebeziumRealtimeTrigger implements MongodbInterface, AbstractDebeziumInterface {
    @Builder.Default
    private MongodbInterface.SnapshotMode snapshotMode = MongodbInterface.SnapshotMode.INITIAL;

    private Object includedCollections;

    private Object excludedCollections;

    @NotNull
    private String connectionString;

    @Override
    public String getHostname() {
        return "";
    }

    @Override
    public String getPort() {
        return "";
    }

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
            .includedDatabases(this.includedDatabases)
            .excludedDatabases(this.excludedDatabases)
            .includedCollections(this.includedCollections)
            .excludedCollections(this.excludedCollections)
            .includedColumns(this.includedColumns)
            .excludedColumns(this.excludedColumns)
            .properties(this.properties)
            .stateName(this.stateName)
            .snapshotMode(this.snapshotMode)
            .connectionString(this.connectionString)
            .build();

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map(output -> TriggerService.generateRealtimeExecution(this, context, output));
    }

}
