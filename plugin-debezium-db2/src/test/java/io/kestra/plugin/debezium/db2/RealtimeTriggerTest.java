package io.kestra.plugin.debezium.db2;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
@Disabled("The tests are disabled for CI, as db2 container have long time initialization")
class RealtimeTriggerTest extends AbstractDebeziumTest {
    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    private KVStoreService kvStoreService;

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

    @BeforeEach
    void cleanup() throws Exception {
        Db2DebeziumTestHelper.cleanupFlowState(kvStoreService, "io.kestra.tests", "trigger", "debezium-state");
        executeSqlScript("scripts/db2.sql");
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
