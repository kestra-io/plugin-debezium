package io.kestra.plugin.debezium.sqlserver;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTest;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest(startRunner = true, startScheduler = true)
@Disabled("This test works but when we added it, the CI will fail on the Trigger test. It may be caused by a database setup ...")
class RealtimeTriggerTest extends AbstractDebeziumTest {
    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

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

    @Test
    void flow() throws Exception {
        // init database
        executeSqlScript("scripts/sqlserver.sql");

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
