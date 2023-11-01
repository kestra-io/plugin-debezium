package io.kestra.plugin.debezium.postgres;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
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
            .hostname(TestUtils.hostname())
            .username(TestUtils.username())
            .password(TestUtils.password())
            .port("65432")
            .database("postgres")
            .pluginName(PostgresInterface.PluginName.PGOUTPUT)
            .sslMode(TestUtils.sslMode())
            .sslRootCert(TestUtils.ca())
            .sslCert(TestUtils.cert())
            .sslKey(TestUtils.key())
            .sslKeyPassword(TestUtils.keyPass())
            .snapshotMode(Capture.SnapshotMode.INITIAL)
            .maxRecords(2)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        AbstractDebeziumTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getSize(), is(2));

        List<Map<String, Object>> types = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, runOutput.getUris().get("postgres.pgsql_types")))), r -> types.add((Map<String, Object>) r));

        assertThat(types.size(), is(2));
        assertThat(types.stream().filter(o -> o.get("concert_id").equals(3)).count(), is(1L));
        assertThat(types.stream().filter(o -> o.get("concert_id").equals(2)).count(), is(1L));

        // rerun state will prevent new records
        runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(0));
    }
}
