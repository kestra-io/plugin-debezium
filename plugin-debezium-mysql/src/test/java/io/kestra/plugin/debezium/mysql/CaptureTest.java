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
        // init database
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
}
