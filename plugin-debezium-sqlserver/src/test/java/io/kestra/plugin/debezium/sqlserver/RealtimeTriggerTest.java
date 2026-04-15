package io.kestra.plugin.debezium.sqlserver;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTest;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest(startRunner = true, startScheduler = true)
@Disabled("This test works but when we added it, the CI will fail on the Trigger test. It may be caused by a database setup ...")
class RealtimeTriggerTest extends AbstractDebeziumTest {
    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

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
        Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
            queueCount.countDown();
            assertThat(execution.getLeft().getFlowId(), is("trigger"));
        });

        repositoryLoader.load(Objects.requireNonNull(RealtimeTriggerTest.class.getClassLoader().getResource("flows/realtime.yaml")));

        boolean await = queueCount.await(15, TimeUnit.SECONDS);
        assertThat(await, is(true));

        Map<String, Object> data = (Map<String, Object>) receive.blockLast().getTrigger().getVariables().get("data");

        assertThat(data, notNullValue());
        assertThat(data.size(), greaterThanOrEqualTo(5));
    }
}
