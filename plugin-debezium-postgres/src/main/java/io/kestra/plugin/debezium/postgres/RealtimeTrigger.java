package io.kestra.plugin.debezium.postgres;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.plugin.debezium.AbstractDebeziumInterface;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
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
    title = "React for change data capture event on PostgreSQL server and create new execution."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "hostname: 127.0.0.1",
                "port: \"5432\"",
                "username: posgres",
                "password: psql_passwd",
                "maxRecords: 100",
                "database: my_database",
                "pluginName: PGOUTPUT",
                "snapshotMode: ALWAYS"
            }
        )
    },
    beta = true
)
public class RealtimeTrigger extends AbstractDebeziumRealtimeTrigger implements PostgresInterface, AbstractDebeziumInterface {
    protected String database;

    @Builder.Default
    protected PluginName pluginName = PluginName.PGOUTPUT;

    @Builder.Default
    protected String slotName = "kestra";

    @Builder.Default
    protected String publicationName = "kestra_publication";

    @Builder.Default
    protected SslMode sslMode = SslMode.DISABLE;

    protected String sslRootCert;

    protected String sslCert;

    protected String sslKey;

    protected String sslKeyPassword;

    @Builder.Default
    private SnapshotMode snapshotMode = SnapshotMode.INITIAL;

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

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map(output -> TriggerService.generateRealtimeExecution(this, context, output));
    }
}
