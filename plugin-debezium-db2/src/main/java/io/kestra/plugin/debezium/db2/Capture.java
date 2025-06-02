package io.kestra.plugin.debezium.db2;

import io.debezium.connector.db2.Db2Connector;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for change data capture event on Db2 server and capture the event as an internal storage file."
)
@Plugin(
    examples = {
        @Example(
            title = "Capture data from Db2 server.",
            full = true,
            code = """
                id: db2_capture
                namespace: company.team

                tasks:
                  - id: capture
                    type: io.kestra.plugin.debezium.db2.Capture
                    snapshotMode: INITIAL
                    hostname: 127.0.0.1
                    port: "50000"
                    username: "{{ secret('DB2_USERNAME') }}"
                    password: "{{ secret('DB2_PASSWORD') }}"
                    database: my_database
                    maxRecords: 100
                """
        )
    }
)
public class Capture extends AbstractDebeziumTask implements Db2Interface {

    protected Property<String> database;

    @Builder.Default
    private Property<Db2Interface.SnapshotMode> snapshotMode = Property.ofValue(SnapshotMode.INITIAL);

    @Override
    protected boolean needDatabaseHistory() {
        return true;
    }

    @Override
    protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) throws Exception {
        Properties props = super.properties(runContext, offsetFile, historyFile);

        props.setProperty("connector.class", Db2Connector.class.getName());

        props.setProperty("database.dbname", runContext.render(this.database).as(String.class).orElseThrow());

        props.setProperty("include.schema.changes", "false");

        if (this.snapshotMode != null) {
            props.setProperty("snapshot.mode", runContext.render(this.snapshotMode).as(SnapshotMode.class).orElseThrow().name().toLowerCase(Locale.ROOT));
        }

        return props;
    }
}
