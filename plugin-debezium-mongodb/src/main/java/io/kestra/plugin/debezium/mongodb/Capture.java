package io.kestra.plugin.debezium.mongodb;

import io.debezium.connector.mongodb.MongoDbConnector;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
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
    title = "Wait for change data capture event on MongoDB server."
)
@Plugin(
    examples = {
        @Example(
            title = "Replica set connection",
            code = {
                "snapshotMode: INITIAL",
                "connectionString: mongodb://mongo_user:mongo_passwd@mongodb0.example.com:27017/?replicaSet=rs0",
                "maxRecords: 100"
            }
        ),
        @Example(
            title = "Sharded connection",
            code = {
                "snapshotMode: INITIAL",
                "connectionString: mongodb://mongo_user:mongo_passwd@mongos0.example.com:27017,mongos1.example.com:27017/",
                "maxRecords: 100"
            }
        ),
        @Example(
            title = "Replica set SRV connection",
            code = {
                "snapshotMode: INITIAL",
                "connectionString: mongodb+srv://mongo_user:mongo_passwd@mongos0.example.com/?replicaSet=rs0",
                "maxRecords: 100"
            }
        ),
        @Example(
            title = "Sharded SRV connection",
            code = {
                "snapshotMode: INITIAL",
                "connectionString: mongodb+srv://mongo_user:mongo_passwd@mongos0.example.com/",
                "maxRecords: 100"
            }
        )
    }
)
public class Capture extends AbstractDebeziumTask implements MongodbInterface {

    private Object includedCollections;

    private Object excludedCollections;

    @NotNull
    private Property<String> connectionString;

    @Builder.Default
    private Property<MongodbInterface.SnapshotMode> snapshotMode = Property.of(SnapshotMode.INITIAL);

    @Override
    protected boolean needDatabaseHistory() {
        return false;
    }

    @Override
    protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) throws Exception {
        Properties props = super.properties(runContext, offsetFile, historyFile);

        props.setProperty("connector.class", MongoDbConnector.class.getName());

        props.setProperty("mongodb.connection.string", runContext.render(this.connectionString).as(String.class).orElse(null));

        if (this.includedCollections != null) {
            props.setProperty("collection.include.list", joinProperties(runContext, this.includedCollections));
        }

        if (this.excludedCollections != null) {
            props.setProperty("collection.exclude.list", joinProperties(runContext, this.excludedCollections));
        }

        props.setProperty("capture.mode", "change_streams_update_full_with_pre_image");

        if (this.snapshotMode != null) {
            props.setProperty("snapshot.mode", runContext.render(this.snapshotMode).as(SnapshotMode.class).orElseThrow().name().toLowerCase(Locale.ROOT));
        }

        return props;
    }

}
