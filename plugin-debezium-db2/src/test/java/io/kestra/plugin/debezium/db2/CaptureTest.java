package io.kestra.plugin.debezium.db2;

import com.google.common.base.Charsets;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest
class CaptureTest extends AbstractDebeziumTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Override
    protected String getUrl() {
        return "jdbc:db2://localhost:5023/kestra";
    }

    @Override
    protected String getUsername() {
        return "db2inst1";
    }

    @Override
    protected String getPassword() {
        return "password";
    }

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        // init database
        executeSqlScript("scripts/db2.sql");

        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .snapshotMode(Db2Interface.SnapshotMode.INITIAL)
            .hostname("127.0.0.1")
            .port("5023")
            .username(getUsername())
            .password(getPassword())
            .database("kestra")
            .maxRecords(5)
            .includedTables(List.of("DB2INST1.EVENTS"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        AbstractDebeziumTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getSize(), is(5));

        List<Map<String, Object>> events = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, runOutput.getUris().get("kestra.EVENTS")))), r -> events.add((Map<String, Object>) r));

        IOUtils.toString(storageInterface.get(null, runOutput.getUris().get("kestra.EVENTS")), Charsets.UTF_8);
        assertThat(events.size(), is(5));
        assertTrue(events.stream().anyMatch(map -> map.get("EVENT_TITLE").equals("Machine Head")));
        assertTrue(events.stream().anyMatch(map -> map.get("EVENT_TITLE").equals("Dropkick Murphys")));
        assertTrue(events.stream().anyMatch(map -> map.get("EVENT_TITLE").equals("Pink Floyd")));
        assertTrue(events.stream().anyMatch(map -> map.get("EVENT_TITLE").equals("TV show")));
        assertTrue(events.stream().anyMatch(map -> map.get("EVENT_TITLE").equals("Nothing")));

        // rerun state will prevent new records
        runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(0));
    }
}
