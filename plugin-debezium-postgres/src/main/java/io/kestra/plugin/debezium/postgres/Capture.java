package io.kestra.plugin.debezium.postgres;

import io.debezium.connector.postgresql.PostgresConnector;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for a change data capture event on PostgreSQL server and capture the event as an internal storage file."
)
@Plugin(
    examples = {
        @Example(
            title = "Capture data from PostgreSQL server.",
            full = true,
            code = """
               id: pg_capture
                namespace: company.team

                tasks:
                  - id: capture_data
                    type: io.kestra.plugin.debezium.postgres.Capture
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
public class Capture extends AbstractDebeziumTask implements PostgresInterface {
    protected Property<String> database;

    @Builder.Default
    protected Property<PluginName> pluginName = Property.ofValue(PluginName.PGOUTPUT);

    @Builder.Default
    protected Property<String> slotName = Property.ofValue("kestra");

    @Builder.Default
    protected Property<String> publicationName = Property.ofValue("kestra_publication");

    @Builder.Default
    protected Property<PostgresInterface.SslMode> sslMode = Property.ofValue(SslMode.DISABLE);

    protected Property<String> sslRootCert;

    protected Property<String> sslCert;

    protected Property<String> sslKey;

    protected Property<String> sslKeyPassword;

    @Builder.Default
    private Property<Capture.SnapshotMode> snapshotMode = Property.ofValue(SnapshotMode.INITIAL);

    @Override
    protected boolean needDatabaseHistory() {
        return false;
    }

    @Override
    protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) throws Exception {
        Properties props = super.properties(runContext, offsetFile, historyFile);

        props.setProperty("connector.class", PostgresConnector.class.getName());

        PostgresService.handleProperties(props, runContext, this);

        return props;
    }
}
