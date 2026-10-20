package io.kestra.plugin.debezium.mongodb;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;

import io.debezium.connector.mongodb.MongoDbConnector;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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

        assertThat(properties.getProperty("connector.class"), is(MongoDbConnector.class.getName()));
        assertThat(properties.getProperty("snapshot.mode"), is("no_data"));
        assertThat(properties.getProperty("capture.mode"), is("change_streams_update_full_with_pre_image"));
        assertThat(properties.getProperty("collection.include.list"), is("test.encounters"));
        assertThat(properties.getProperty("topic.prefix"), notNullValue());
        assertThat(properties.getProperty("name"), notNullValue());
        assertThat(runContext.render(capture.getHostname()).as(String.class).orElse(""), is("legacy-host"));
        assertThat(runContext.render(capture.getPort()).as(String.class).orElse(""), is("27017"));
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

        RealtimeTrigger realtimeTrigger = RealtimeTrigger.builder()
            .id("test_realtime_trigger")
            .type(RealtimeTrigger.class.getName())
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://localhost:27017/?replicaSet=rs0"))
            .hostname(Property.ofValue("legacy-host"))
            .port(Property.ofValue("27017"))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        assertThat(runContext.render(trigger.getHostname()).as(String.class).orElse(""), is("legacy-host"));
        assertThat(runContext.render(trigger.getPort()).as(String.class).orElse(""), is("27017"));
        assertThat(trigger.getConnectionString(), notNullValue());

        assertThat(runContext.render(realtimeTrigger.getHostname()).as(String.class).orElse(""), is("legacy-host"));
        assertThat(runContext.render(realtimeTrigger.getPort()).as(String.class).orElse(""), is("27017"));
        assertThat(realtimeTrigger.getConnectionString(), notNullValue());
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
        Trigger trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .stateName(Property.ofValue(IdUtils.create()))
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://mongo_user:mongo_passwd@127.0.0.1:27017/admin?replicaSet=rs0"))
            .includedCollections(List.of("test.empty_collection"))
            .maxWait(Property.ofValue(Duration.ofSeconds(2)))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> result = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    void triggerCapturesInsertedRecordInNoDataMode() throws Exception {
        String connectionString = "mongodb://mongo_user:mongo_passwd@127.0.0.1:27017/admin?replicaSet=rs0";
        String uniqueSuffix = IdUtils.create();
        String dbName = "test";
        String collName = "cdc_events_" + uniqueSuffix;
        String fullCollectionName = dbName + "." + collName;

        Trigger trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .stateName(Property.ofValue(IdUtils.create()))
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue(connectionString))
            .includedCollections(List.of(fullCollectionName))
            .maxRecords(Property.ofValue(1))
            .maxDuration(Property.ofValue(Duration.ofMinutes(2)))
            .maxWait(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var running = new AtomicBoolean(true);
        var insertError = new AtomicReference<Throwable>();
        var executor = Executors.newSingleThreadScheduledExecutor();
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            mongoClient.getDatabase(dbName).createCollection(collName);
            var collection = mongoClient.getDatabase(dbName).getCollection(collName);

            executor.scheduleAtFixedRate(() -> {
                if (running.get()) {
                    try {
                        collection.insertOne(new Document("msg", "hello"));
                    } catch (Throwable t) {
                        if (running.get()) {
                            insertError.compareAndSet(null, t);
                        }
                    }
                }
            }, 500, 500, TimeUnit.MILLISECONDS);

            Optional<Execution> result = trigger.evaluate(context.getKey(), context.getValue());
            running.set(false);
            executor.shutdownNow();

            if (result.isEmpty() && insertError.get() != null) {
                throw new AssertionError("MongoDB insertion failed during test execution", insertError.get());
            }

            assertThat(result.isPresent(), is(true));
            Execution execution = result.get();
            assertThat(execution.getTrigger(), notNullValue());
            assertThat((Integer) execution.getTrigger().getVariables().get("size"), greaterThanOrEqualTo(1));
        } finally {
            running.set(false);
            executor.shutdownNow();
            try {
                executor.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
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
