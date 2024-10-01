package io.kestra.plugin.debezium.oracle;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTest;
import io.micronaut.context.ApplicationContext;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.io.FileNotFoundException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@KestraTest
@Disabled("Works locally by flaky on CI")
class TriggerTest extends AbstractDebeziumTest {
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
        return "jdbc:oracle:thin:@localhost:1521:XE";
    }

    @Override
    protected String getUsername() {
        return "kestra";
    }

    @Override
    protected String getPassword() {
        return "passwd";
    }

    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        try {
            RunScript.execute(getConnection(), new StringReader("DROP TABLE trigger_events;"));
        } catch (Exception ignored) {

        }

        executeSqlScript("scripts/oracle-trigger.sql");
    }

    @Test
    void flow() throws Exception {
        // init database
        initDatabase();

        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
            Worker worker = applicationContext.createBean(Worker.class, IdUtils.create(), 8, null);
        ) {
            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("trigger"));
            });

            worker.run();
            scheduler.run();

            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/trigger.yaml")));

            boolean await = queueCount.await(1, TimeUnit.MINUTES);
            assertThat(await, is(true));

            Integer trigger = (Integer) receive.blockLast().getTrigger().getVariables().get("size");

            assertThat(trigger, greaterThanOrEqualTo(5));
        }
    }
}
