package io.kestra.plugin.debezium.mysql;

import com.google.common.base.Charsets;
import io.debezium.data.Envelope;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
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

@MicronautTest
class CaptureTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .serverId("123456789")
            .snapshotMode(MysqlInterface.SnapshotMode.NEVER)
            .hostname("127.0.0.1")
            .port("63306")
            .username("root")
            .password("mysql_passwd")
            .maxRecords(19)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        AbstractDebeziumTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getSize(), is(19));

        List<Map<String, Object>> employee = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, runOutput.getUris().get("second.EmployeeTerritory")))), r -> employee.add((Map<String, Object>) r));

        List<Map<String, Object>> types = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, runOutput.getUris().get("kestra.mysql_types")))), r -> types.add((Map<String, Object>) r));

        IOUtils.toString(storageInterface.get(null, runOutput.getUris().get("kestra.mysql_types")), Charsets.UTF_8);
        assertThat(employee.size(), is(14));
        assertThat(employee.stream().filter(o -> !((Boolean) o.get("deleted"))).count(), is(7L));

        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, runOutput.getUris().get("second.EmployeeTerritory")))), r -> employee.add((Map<String, Object>) r));
        assertThat(types.size(), is(5));
        assertThat(types.stream().filter(o -> !((Boolean) o.get("deleted"))).count(), is(4L));

        // rerun state will prevent new records
        runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(0));
    }
}
