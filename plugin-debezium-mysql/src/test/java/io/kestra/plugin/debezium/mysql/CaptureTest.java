package io.kestra.plugin.debezium.mysql;

import com.google.common.base.Charsets;
import io.kestra.core.models.property.Property;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
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
        return "jdbc:mysql://127.0.0.1:63306/kestra";
    }

    @Override
    protected String getUsername() {
        return "root";
    }

    @Override
    protected String getPassword() {
        return "mysql_passwd";
    }

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        // init database and wipe binlog state so we only consume freshly inserted rows
        resetMaster();
        executeSqlScript("scripts/mysql.sql");

        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .serverId(Property.ofValue("123456789"))
            .snapshotMode(Property.ofValue(MysqlInterface.SnapshotMode.NEVER))
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("63306"))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .maxRecords(Property.ofValue(5))
            .maxWait(Property.ofValue(Duration.ofSeconds(30)))
            .includedTables(List.of("kestra.capture_events"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Remove any persisted Debezium offsets/history from previous runs so we only consume the freshly seeded data.
        cleanupState(runContext, task);

        AbstractDebeziumTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getSize(), is(5));

        List<Map<String, Object>> events = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, runOutput.getUris().get("kestra.capture_events")))), r -> events.add((Map<String, Object>) r));

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

    private void resetMaster() throws Exception {
        try (var connection = getConnection(); var statement = connection.createStatement()) {
            statement.execute("RESET MASTER");
        }
    }

    private static void cleanupState(RunContext runContext, Capture task) throws Exception {
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        String taskRunValue = runContext.storage().getTaskStorageContext()
            .map(io.kestra.core.storages.StorageContext.Task::getTaskRunValue)
            .orElse(null);
        String stateName = runContext.render(task.getStateName()).as(String.class).orElse("debezium-state");

        var offsetKey = computeKvStoreKey(runContext, stateName, "offsets.dat", taskRunValue);
        kvStore.delete(offsetKey);

        if (task.needDatabaseHistory()) {
            var historyKey = computeKvStoreKey(runContext, stateName, "dbhistory.dat", taskRunValue);
            kvStore.delete(historyKey);
        }
    }

    private static String computeKvStoreKey(RunContext runContext, String stateName, String filename, String taskRunValue) throws Exception {
        String separator = "_";
        boolean hashTaskRunValue = taskRunValue != null;

        String flowId = runContext.flowInfo().id();
        String flowIdPrefix = (flowId == null) ? "" : (io.kestra.core.utils.Slugify.of(flowId) + separator);
        String prefix = flowIdPrefix + "states" + separator + stateName;

        if (taskRunValue != null) {
            String taskRunSuffix = hashTaskRunValue ? io.kestra.core.utils.Hashing.hashToString(taskRunValue) : taskRunValue;
            prefix = prefix + separator + taskRunSuffix;
        }

        return prefix + separator + filename;
    }
}
