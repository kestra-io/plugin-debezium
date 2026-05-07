package io.kestra.plugin.debezium.postgres;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.services.KVStoreService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTest;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest(startRunner = true, startScheduler = true)
class RealtimeTriggerTest extends AbstractDebeziumTest {
    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    private KVStoreService kvStoreService;

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

    @BeforeEach
    void cleanup() throws Exception {
        PostgresDebeziumTestHelper.dropReplicationArtifacts(this::getConnection, "kestra", "kestra_publication");
        PostgresDebeziumTestHelper.cleanupFlowState(kvStoreService, "io.kestra.tests", "trigger", "debezium-state");
        executeSqlScript("scripts/postgres.sql");
    }

    @Test
    void flow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();
        executionQueue.addListener(execution -> {
            if (execution.getFlowId().equals("trigger")) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        repositoryLoader.load(Objects.requireNonNull(RealtimeTriggerTest.class.getClassLoader().getResource("flows/realtime.yaml")));

        boolean await = queueCount.await(15, TimeUnit.SECONDS);
        assertThat(await, is(true));

        Map<String, Object> data = (Map<String, Object>) last.get().getTrigger().getVariables().get("data");

        assertThat(data, notNullValue());
        assertThat(data.size(), greaterThanOrEqualTo(5));
    }
}
