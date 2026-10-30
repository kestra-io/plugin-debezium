package io.kestra.plugin.debezium.mongodb;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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
import com.mongodb.client.MongoClients;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
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
        var capture = Capture.builder()
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

        var runContext = TestsUtils.mockRunContext(runContextFactory, capture, Map.of());
        var offsetFile = runContext.workingDir().path().resolve("offsets.dat");
        var historyFile = runContext.workingDir().path().resolve("history.dat");

        var properties = capture.properties(runContext, offsetFile, historyFile);

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
        var trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://localhost:27017/?replicaSet=rs0"))
            .hostname(Property.ofValue("legacy-host"))
            .port(Property.ofValue("27017"))
            .build();

        var realtimeTrigger = RealtimeTrigger.builder()
            .id(IdUtils.create())
            .type(RealtimeTrigger.class.getName())
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://localhost:27017/?replicaSet=rs0"))
            .hostname(Property.ofValue("legacy-host"))
            .port(Property.ofValue("27017"))
            .build();

        var runContext = runContextFactory.of(Map.of());
        assertThat(runContext.render(trigger.getHostname()).as(String.class).orElse(""), is("legacy-host"));
        assertThat(runContext.render(trigger.getPort()).as(String.class).orElse(""), is("27017"));
        assertThat(trigger.getConnectionString(), notNullValue());

        assertThat(runContext.render(realtimeTrigger.getHostname()).as(String.class).orElse(""), is("legacy-host"));
        assertThat(runContext.render(realtimeTrigger.getPort()).as(String.class).orElse(""), is("27017"));
        assertThat(realtimeTrigger.getConnectionString(), notNullValue());
    }

    @Test
    void triggerDefaultsDeprecatedHostnameAndPortToEmptyForCompatibility() throws Exception {
        var trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://localhost:27017/?replicaSet=rs0"))
            .build();

        var runContext = runContextFactory.of(Map.of());
        assertThat(runContext.render(trigger.getHostname()).as(String.class).orElse(""), is(""));
        assertThat(runContext.render(trigger.getPort()).as(String.class).orElse(""), is(""));
        assertThat(trigger.getConnectionString(), notNullValue());
    }

    @Test
    void triggerReturnsEmptyOnZeroMessages() throws Exception {
        var trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .stateName(Property.ofValue(IdUtils.create()))
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://mongo_user:mongo_passwd@127.0.0.1:27017/admin?replicaSet=rs0"))
            .includedCollections(List.of("test.empty_collection"))
            .maxWait(Property.ofValue(Duration.ofSeconds(2)))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var result = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    void triggerCapturesInsertedRecordInNoDataMode() throws Exception {
        var connectionString = "mongodb://mongo_user:mongo_passwd@127.0.0.1:27017/admin?replicaSet=rs0";
        var uniqueSuffix = IdUtils.create();
        var dbName = "test";
        var collName = "cdc_events_" + uniqueSuffix;
        var fullCollectionName = dbName + "." + collName;

        var trigger = Trigger.builder()
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
        try (var mongoClient = MongoClients.create(connectionString)) {
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

            var result = trigger.evaluate(context.getKey(), context.getValue());
            running.set(false);
            executor.shutdownNow();

            if (result.isEmpty() && insertError.get() != null) {
                throw new AssertionError("MongoDB insertion failed during test execution", insertError.get());
            }

            assertThat(result.isPresent(), is(true));
            var execution = result.get();
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
        var securityException = new MongoSecurityException(
            MongoCredential.createCredential("user", "admin", "pass".toCharArray()),
            "Exception authenticating"
        );
        assertThat(AbstractDebeziumTask.isShutdownArtifact(securityException), is(false));

        var socketException = new MongoSocketOpenException(
            "Exception opening socket",
            new ServerAddress("localhost", 27017)
        );
        assertThat(AbstractDebeziumTask.isShutdownArtifact(socketException), is(false));

        var connectException = new RuntimeException(
            "Could not connect to MongoDB replica set", socketException
        );
        assertThat(AbstractDebeziumTask.isShutdownArtifact(connectException), is(false));
    }
}
