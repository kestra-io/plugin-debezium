package io.kestra.plugin.debezium.mongodb;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;

import io.debezium.connector.mongodb.MongoDbConnector;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class MongoDbTriggerLifecycleTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void captureConfiguresNoDataSnapshotModeAndConnectorType() throws Exception {
        Capture capture = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://localhost:27017/?replicaSet=rs0"))
            .includedCollections(List.of("test.encounters"))
            .maxWait(Property.ofValue(Duration.ofSeconds(30)))
            .maxDuration(Property.ofValue(Duration.ofMinutes(5)))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, capture, Map.of());
        Path offsetFile = runContext.workingDir().path().resolve("offsets.dat");
        Path historyFile = runContext.workingDir().path().resolve("history.dat");

        Properties properties = capture.properties(runContext, offsetFile, historyFile);

        assertThat(AbstractDebeziumTask.resolveConnectorType(properties), is("mongodb"));
        assertThat(properties.getProperty("connector.class"), is(MongoDbConnector.class.getName()));
        assertThat(properties.getProperty("snapshot.mode"), is("no_data"));
        assertThat(properties.getProperty("capture.mode"), is("change_streams_update_full_with_pre_image"));
        assertThat(properties.getProperty("collection.include.list"), is("test.encounters"));
        assertThat(properties.getProperty("topic.prefix"), notNullValue());
        assertThat(properties.getProperty("name"), notNullValue());
        assertThat(properties.getProperty("database.hostname"), org.hamcrest.Matchers.nullValue());
        assertThat(properties.getProperty("database.port"), org.hamcrest.Matchers.nullValue());
    }

    @Test
    void triggerReturnsEmptyOnZeroMessages() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("test_cdc_trigger")
            .type(Trigger.class.getName())
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://localhost:27017/?replicaSet=rs0"))
            .includedCollections(List.of("test.encounters"))
            .maxWait(Property.ofValue(Duration.ofSeconds(10)))
            .build();

        // Subclass trigger to mock task execution returning 0 messages
        Trigger mockTrigger = new Trigger() {
            @Override
            public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
                AbstractDebeziumTask.Output output = AbstractDebeziumTask.Output.builder()
                    .size(0)
                    .uris(Map.of())
                    .build();

                if (output.getSize() == 0) {
                    return Optional.empty();
                }
                return super.evaluate(conditionContext, context);
            }
        };

        RunContext runContext = runContextFactory.of(Map.of());
        ConditionContext conditionContext = ConditionContext.builder().runContext(runContext).build();
        TriggerContext triggerContext = TriggerContext.builder().build();

        Optional<Execution> result = mockTrigger.evaluate(conditionContext, triggerContext);
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    void mongoExceptionsNotClassifiedAsShutdownArtifacts() {
        com.mongodb.MongoSecurityException securityException = new com.mongodb.MongoSecurityException(
            com.mongodb.MongoCredential.createCredential("user", "admin", "pass".toCharArray()),
            "Exception authenticating"
        );
        assertThat(AbstractDebeziumTask.isShutdownArtifact(securityException), is(false));

        com.mongodb.MongoSocketOpenException socketException = new com.mongodb.MongoSocketOpenException(
            "Exception opening socket",
            new com.mongodb.ServerAddress("localhost", 27017)
        );
        assertThat(AbstractDebeziumTask.isShutdownArtifact(socketException), is(false));

        RuntimeException connectException = new RuntimeException(
            "Could not connect to MongoDB replica set", socketException
        );
        assertThat(AbstractDebeziumTask.isShutdownArtifact(connectException), is(false));
    }
}
