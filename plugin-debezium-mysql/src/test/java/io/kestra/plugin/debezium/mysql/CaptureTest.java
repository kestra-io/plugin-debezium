package io.kestra.plugin.debezium.mysql;

import com.google.common.base.Charsets;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueException;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunnerUtils;
import io.kestra.core.runners.StandAloneRunner;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class CaptureTest extends AbstractDebeziumTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Inject
    protected StandAloneRunner runner;

    @Inject
    protected RunnerUtils runnerUtils;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @BeforeEach
    protected void init() throws IOException, URISyntaxException {
        repositoryLoader.load(Objects.requireNonNull(CaptureTest.class.getClassLoader().getResource("flows")));
        this.runner.run();
    }

    @Test
    void flow() throws TimeoutException, QueueException, SQLException, IOException, URISyntaxException {
        // init database
        executeSqlScript("scripts/mysql.sql");

        Execution execution = runnerUtils.runOne(
            null,
            "io.kestra.tests",
            "capture-mysql",
            null,
            (f, e) -> Map.of(
                "username", getUsername(),
                "password", getPassword()
            ),
            Duration.ofMinutes(10)
        );

        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));


        Map<String, Object> output = execution.getTaskRunList().getFirst().getOutputs();

        assertThat(output.get("size"), is(5));

        List<Map<String, Object>> events = new ArrayList<>();
        Map<String, URI> uris = (Map<String, URI>) output.get("uris");

        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, null, uris.get("kestra.events")))), r -> events.add((Map<String, Object>) r));

        IOUtils.toString(storageInterface.get(null, null, uris.get("kestra.events")), StandardCharsets.UTF_8);
        assertThat(events.size(), is(5));
        assertTrue(events.stream().anyMatch(map -> map.get("event_title").equals("Machine Head")));
        assertTrue(events.stream().anyMatch(map -> map.get("event_title").equals("Dropkick Murphys")));
        assertTrue(events.stream().anyMatch(map -> map.get("event_title").equals("Pink Floyd")));
        assertTrue(events.stream().anyMatch(map -> map.get("event_title").equals("TV show")));
        assertTrue(events.stream().anyMatch(map -> map.get("event_title").equals("Nothing")));

        // rerun state will prevent new records
        runnerUtils.runOne(
            null,
            "io.kestra.tests",
            "capture-mysql",
            null,
            (f, e) -> Map.of(
                "username", getUsername(),
                "password", getPassword()
            ),
            Duration.ofMinutes(10)
        );
        assertThat(output.get("size"), is(5));
    }

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
}
