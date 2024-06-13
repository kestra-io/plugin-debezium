package io.kestra.plugin.debezium.cassandra;

import io.debezium.connector.cassandra.Cassandra4Connector;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
    title = "Wait for change data capture event on Cassandra server."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "snapshotMode: NEVER",
                "hostname: 127.0.0.1",
                "port: \"9042\"",
                "username: cassandra_user",
                "password: cassandra_passwd",
                "maxRecords: 100",
            }
        )
    }
)
public class Capture extends AbstractDebeziumTask implements CassandraInterface {
    @Builder.Default
    private CassandraInterface.SnapshotMode snapshotMode = CassandraInterface.SnapshotMode.INITIAL;

    @NotNull
    private Path cassandraConfig;

    @Override
    protected boolean needDatabaseHistory() {
        return true;
    }

    @Override
    protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) throws Exception {
        Properties props = super.properties(runContext, offsetFile, historyFile);

        props.setProperty("connector.class", Cassandra4Connector.class.getName());

        props.setProperty("cassandra.config", cassandraConfig.toAbsolutePath().toString()); // TODO: Make as field

        props.setProperty("cassandra.hosts", this.hostname);
        props.setProperty("cassandra.port", this.port);
        props.setProperty("cassandra.username", this.username);
        props.setProperty("cassandra.password", this.password);

        props.setProperty("commit.log.relocation.dir", historyFile.toAbsolutePath().toString());

        props.setProperty("include.schema.changes", "false");

        if (this.snapshotMode != null) {
            props.setProperty("snapshot.mode", this.snapshotMode.name().toLowerCase(Locale.ROOT));
        }

        return props;
    }
}
