package io.kestra.plugin.debezium.mongodb;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;

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
import static org.hamcrest.Matchers.nullValue;

@KestraTest
class MongoDbTriggerLifecycleTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static boolean isMongoReachable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", 27017), 1000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void captureConfiguresNoDataSnapshotModeAndConnectorType() throws Exception {
        Capture capture = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://localhost:27017/?replicaSet=rs0"))
            .hostname(Property.ofValue("legacy-host"))
            .port(Property.ofValue("27017"))
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
        assertThat(properties.getProperty("database.hostname"), nullValue());
        assertThat(properties.getProperty("database.port"), nullValue());
    }

    @Test
    void triggerMaintainsDeprecatedHostnameAndPortCompatibility() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("test_cdc_trigger")
            .type(Trigger.class.getName())
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://localhost:27017/?replicaSet=rs0"))
            .hostname(Property.ofValue("legacy-host"))
            .port(Property.ofValue("27017"))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        assertThat(runContext.render(trigger.getHostname()).as(String.class).orElse(""), is("legacy-host"));
        assertThat(runContext.render(trigger.getPort()).as(String.class).orElse(""), is("27017"));
        assertThat(trigger.getConnectionString(), notNullValue());
    }

    @Test
    void triggerDefaultsDeprecatedHostnameAndPortToEmptyForCompatibility() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("test_cdc_trigger")
            .type(Trigger.class.getName())
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://localhost:27017/?replicaSet=rs0"))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        assertThat(runContext.render(trigger.getHostname()).as(String.class).orElse(""), is(""));
        assertThat(runContext.render(trigger.getPort()).as(String.class).orElse(""), is(""));
        assertThat(trigger.getConnectionString(), notNullValue());
    }

    @Test
    void triggerReturnsEmptyOnZeroMessages() throws Exception {
        Assumptions.assumeTrue(
            isMongoReachable(),
            "Requires running MongoDB replica set at 127.0.0.1:27017 (docker-compose-ci.yml)"
        );

        Trigger trigger = Trigger.builder()
            .id("test_cdc_trigger")
            .type(Trigger.class.getName())
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://mongo_user:mongo_passwd@127.0.0.1:27017/?replicaSet=rs0"))
            .includedCollections(List.of("test.empty_collection"))
            .maxWait(Property.ofValue(Duration.ofSeconds(2)))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        ConditionContext conditionContext = ConditionContext.builder().runContext(runContext).build();
        TriggerContext triggerContext = TriggerContext.builder().build();

        Optional<Execution> result = trigger.evaluate(conditionContext, triggerContext);
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    void mongoExceptionsNotClassifiedAsShutdownArtifacts() {
        MongoSecurityException securityException = new MongoSecurityException(
            MongoCredential.createCredential("user", "admin", "pass".toCharArray()),
            "Exception authenticating"
        );
        assertThat(AbstractDebeziumTask.isShutdownArtifact(securityException), is(false));

        MongoSocketOpenException socketException = new MongoSocketOpenException(
            "Exception opening socket",
            new ServerAddress("localhost", 27017)
        );
        assertThat(AbstractDebeziumTask.isShutdownArtifact(socketException), is(false));

        RuntimeException connectException = new RuntimeException(
            "Could not connect to MongoDB replica set", socketException
        );
        assertThat(AbstractDebeziumTask.isShutdownArtifact(connectException), is(false));
    }
}
