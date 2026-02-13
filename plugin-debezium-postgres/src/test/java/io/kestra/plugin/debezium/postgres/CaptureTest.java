package io.kestra.plugin.debezium.postgres;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumTest;
import io.kestra.plugin.debezium.postgres.PostgresDebeziumTestHelper;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, runOutput.getUris().get("postgres.events")))), r -> events.add((Map<String, Object>) r));

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
}
