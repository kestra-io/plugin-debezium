package io.kestra.plugin.debezium.sqlserver;

import com.google.common.base.Charsets;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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
            .snapshotMode(SqlServerInterface.SnapshotMode.INITIAL_ONLY)
            .hostname("127.0.0.1")
            .port("61433")
            .username("sa")
            .password("Sqls3rv3r_Pa55word!")
            .database("deb")
            .maxRecords(2)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        AbstractDebeziumTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getSize(), is(2));

        List<Map<String, Object>> data = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(runOutput.getUris().get("deb.sqlserver_types")))), r -> data.add((Map<String, Object>) r));

        assertThat(data.size(), is(2));
        assertThat(data.stream().filter(o -> !((Boolean) o.get("deleted"))).count(), is(2L));

        // rerun state will prevent new records
        runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(0));
    }
}
