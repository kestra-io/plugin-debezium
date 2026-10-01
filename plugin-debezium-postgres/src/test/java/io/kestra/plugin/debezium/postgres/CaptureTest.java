package io.kestra.plugin.debezium.postgres;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumTest;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class CaptureTest extends AbstractDebeziumTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Override
    protected String getUrl() {
        return "jdbc:postgresql://127.0.0.1:65432/";
    }

    @Override
    protected String getUsername() {
        return TestUtils.username();
    }

    @Override
    protected String getPassword() {
        return TestUtils.password();
    }

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        // init database
        executeSqlScript("scripts/postgres.sql");

        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .hostname(Property.ofValue(TestUtils.hostname()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .port(Property.ofValue("65432"))
            .database(Property.ofValue("postgres"))
            .pluginName(Property.ofValue(PostgresInterface.PluginName.PGOUTPUT))
            .stateName(Property.ofValue("debezium-state-" + IdUtils.create()))
            // SSL is disabled or we cannot test triggers which are very important for Debezium
            //            .sslMode(TestUtils.sslMode())
            //            .sslRootCert(TestUtils.ca())
            //            .sslCert(TestUtils.cert())
            //            .sslKey(TestUtils.key())
            //            .sslKeyPassword(TestUtils.keyPass())
            .snapshotMode(Property.ofValue(Capture.SnapshotMode.INITIAL))
            .maxRecords(Property.ofValue(5))
            .includedTables(List.of("public.events"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        PostgresDebeziumTestHelper.dropReplicationArtifacts(
            this::getConnection,
            runContext.render(task.getSlotName()).as(String.class).orElse("kestra"),
            runContext.render(task.getPublicationName()).as(String.class).orElse("kestra_publication")
        );
        PostgresDebeziumTestHelper.cleanupTaskState(runContext, task);

        AbstractDebeziumTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getSize(), is(5));

        List<Map<String, Object>> events = new ArrayList<>();
        FileSerde.reader(
            new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, runOutput.getUris().get("postgres.events")))),
            r -> events.add((Map<String, Object>) r)
        );

        assertThat(events.size(), is(5));
        assertTrue(events.stream().anyMatch(map -> map.get("event_title").equals("Machine Head")));
        assertTrue(events.stream().anyMatch(map -> map.get("event_title").equals("Dropkick Murphys")));
        assertTrue(events.stream().anyMatch(map -> map.get("event_title").equals("Pink Floyd")));
        assertTrue(events.stream().anyMatch(map -> map.get("event_title").equals("TV show")));
        assertTrue(events.stream().anyMatch(map -> map.get("event_title").equals("Nothing")));

        // rerun state will prevent new records
        runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    void runWithSnapshotModeNever() throws Exception {
        // init database
        executeSqlScript("scripts/postgres.sql");

        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .hostname(Property.ofValue(TestUtils.hostname()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .port(Property.ofValue("65432"))
            .database(Property.ofValue("postgres"))
            .pluginName(Property.ofValue(PostgresInterface.PluginName.PGOUTPUT))
            .stateName(Property.ofValue("debezium-state-" + IdUtils.create()))
            .snapshotMode(Property.ofValue(Capture.SnapshotMode.NEVER))
            .maxWait(Property.ofValue(Duration.ofSeconds(5)))
            .includedTables(List.of("public.events"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        PostgresDebeziumTestHelper.dropReplicationArtifacts(
            this::getConnection,
            runContext.render(task.getSlotName()).as(String.class).orElse("kestra"),
            runContext.render(task.getPublicationName()).as(String.class).orElse("kestra_publication")
        );
        PostgresDebeziumTestHelper.cleanupTaskState(runContext, task);

        // no snapshot: the connector must start (Debezium 3.x rejects "never") and see none of the existing rows
        AbstractDebeziumTask.Output runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(0));

        // rows inserted after the replication slot was created are streamed
        try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO events(events_id, event_title, event_description) VALUES (6, 'Iron Maiden', 'Streamed')");
        }

        runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(1));

        List<Map<String, Object>> events = new ArrayList<>();
        FileSerde.reader(
            new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, runOutput.getUris().get("postgres.events")))),
            r -> events.add((Map<String, Object>) r)
        );

        assertThat(events.size(), is(1));
        assertThat(events.getFirst().get("event_title"), is("Iron Maiden"));
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @EnumSource(AbstractDebeziumTask.Format.class)
    void deletedNull(AbstractDebeziumTask.Format format) throws Exception {
        // init database
        executeSqlScript("scripts/postgres.sql");

        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .hostname(Property.ofValue(TestUtils.hostname()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .port(Property.ofValue("65432"))
            .database(Property.ofValue("postgres"))
            .pluginName(Property.ofValue(PostgresInterface.PluginName.PGOUTPUT))
            .stateName(Property.ofValue("debezium-state-" + IdUtils.create()))
            .snapshotMode(Property.ofValue(Capture.SnapshotMode.NEVER))
            .format(Property.ofValue(format))
            .deleted(Property.ofValue(AbstractDebeziumTask.Deleted.NULL))
            .maxWait(Property.ofValue(Duration.ofSeconds(5)))
            .includedTables(List.of("public.events"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        PostgresDebeziumTestHelper.dropReplicationArtifacts(
            this::getConnection,
            runContext.render(task.getSlotName()).as(String.class).orElse("kestra"),
            runContext.render(task.getPublicationName()).as(String.class).orElse("kestra_publication")
        );
        PostgresDebeziumTestHelper.cleanupTaskState(runContext, task);

        // creates the replication slot, no snapshot
        AbstractDebeziumTask.Output runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(0));

        try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO events(events_id, event_title, event_description) VALUES (6, 'Iron Maiden', 'Inserted')");
            statement.execute("DELETE FROM events WHERE events_id = 1");
        }

        runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(2));

        List<Map<String, Object>> events = new ArrayList<>();
        FileSerde.reader(
            new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, runOutput.getUris().get("postgres.events")))),
            r -> events.add((Map<String, Object>) r)
        );
        assertThat(events.size(), is(2));

        Map<String, Object> inserted = events.get(0);
        Map<String, Object> deleted = events.get(1);

        switch (format) {
            case INLINE -> {
                // non-delete events are untouched
                assertThat(inserted.get("event_title"), is("Iron Maiden"));

                // the deleted row keeps its columns with all values as null
                assertTrue(deleted.containsKey("events_id"));
                assertThat(deleted.get("events_id"), is(nullValue()));
                assertThat(((Map<String, Object>) deleted.get("metadata")).get("operation"), is("DELETE"));
            }
            case WRAP -> {
                assertThat(((Map<String, Object>) inserted.get("record")).get("event_title"), is("Iron Maiden"));

                Map<String, Object> record = (Map<String, Object>) deleted.get("record");
                assertTrue(record.containsKey("events_id"));
                assertThat(record.get("events_id"), is(nullValue()));
                assertThat(((Map<String, Object>) deleted.get("metadata")).get("operation"), is("DELETE"));
            }
            case RAW -> {
                assertThat(inserted.get("value"), is(notNullValue()));

                assertTrue(deleted.containsKey("value"));
                assertThat(deleted.get("value"), is(nullValue()));
            }
        }

        // the deleted field is only added with ADD_FIELD
        assertThat(deleted.containsKey("deleted"), is(false));
    }
}
