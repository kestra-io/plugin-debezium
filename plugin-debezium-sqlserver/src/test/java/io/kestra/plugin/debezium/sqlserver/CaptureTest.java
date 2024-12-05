package io.kestra.plugin.debezium.sqlserver;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumTest;
import io.kestra.core.junit.annotations.KestraTest;
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
        return "jdbc:sqlserver://localhost:61433;trustServerCertificate=true;databaseName=deb";
    }

    @Override
    protected String getUsername() {
        return "sa";
    }

    @Override
    protected String getPassword() {
        return "Sqls3rv3r_Pa55word!";
    }

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        // init database
        executeSqlScript("scripts/sqlserver.sql");

        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .snapshotMode(SqlServerInterface.SnapshotMode.INITIAL_ONLY)
            .hostname(Property.of("127.0.0.1"))
            .port(Property.of("61433"))
            .username(Property.of(getUsername()))
            .password(Property.of(getPassword()))
            .database("deb")
            .maxRecords(Property.of(5))
            .properties(Property.of(Map.of("database.encrypt", "false")))
            .includedTables(List.of("dbo.events"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        AbstractDebeziumTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getSize(), is(5));

        List<Map<String, Object>> events = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, null, runOutput.getUris().get("deb.events")))), r -> events.add((Map<String, Object>) r));

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
