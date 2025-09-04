package io.kestra.plugin.debezium.mysql;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.Worker;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTest;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class RealtimeTriggerTest extends AbstractDebeziumTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

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

    @Test
    void flow() throws Exception {
        // init database
        executeSqlScript("scripts/mysql.sql");

        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
            DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, IdUtils.create(), 8, null);
        ) {
            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("trigger"));
            });

            worker.run();
            scheduler.run();

            repositoryLoader.load(Objects.requireNonNull(RealtimeTriggerTest.class.getClassLoader().getResource("flows/realtime.yaml")));

            boolean await = queueCount.await(15, TimeUnit.SECONDS);
            assertThat(await, is(true));

            Map<String, Object> data = (Map<String, Object>) receive.blockLast().getTrigger().getVariables().get("data");

            assertThat(data, notNullValue());

            assertThat(data.size(), greaterThanOrEqualTo(5));
        }
    }
}
