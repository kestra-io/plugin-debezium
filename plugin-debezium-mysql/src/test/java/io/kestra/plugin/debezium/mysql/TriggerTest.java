package io.kestra.plugin.debezium.mysql;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.services.KVStoreService;
import io.kestra.core.storages.kv.KVEntry;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVStoreException;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.Slugify;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import io.kestra.plugin.debezium.AbstractDebeziumTest;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.ExecutionMode;
import reactor.core.publisher.Flux;

import java.net.URL;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@org.junit.jupiter.api.parallel.Execution(ExecutionMode.SAME_THREAD)
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

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private KVStoreService kvStoreService;

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

    private AbstractScheduler scheduler;
    private DefaultWorker worker;

    @BeforeAll
    void startWorkerAndScheduler() {
        scheduler = new JdbcScheduler(applicationContext, flowListenersService);
        worker = applicationContext.createBean(DefaultWorker.class, IdUtils.create(), 8, null);

        worker.run();
        scheduler.run();
    }

    @AfterAll
    void stopWorkerAndScheduler() {
        if (scheduler != null) {
            scheduler.close();
        }
        if (worker != null) {
            worker.close();
        }
    }

    @BeforeEach
    void seedDbEachTest() throws Exception {
        resetMaster();
        cleanupDebeziumState();
        executeSqlScript("scripts/mysql.sql");
    }

    @Test
    void flow() throws Exception {
        runFlowAndAssertOffsetMode(
            "flows/trigger.yaml",
            "trigger-default",
            AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_STOP
        );
    }

    @Test
    void flow_offsetsCommitMode_onStop() throws Exception {
        runFlowAndAssertOffsetMode(
            "flows/trigger-on-stop.yaml",
            "trigger-on-stop",
            AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_STOP
        );
    }

    @Test
    void flow_offsetsCommitMode_onEachBatch() throws Exception {
        runFlowAndAssertOffsetMode(
            "flows/trigger-on-each-batch.yaml",
            "trigger-on-each-batch",
            AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_EACH_BATCH
        );
    }

    private void runFlowAndAssertOffsetMode(
        String flowResource,
        String expectedFlowId,
        AbstractDebeziumRealtimeTrigger.OffsetCommitMode expectedMode
    ) throws Exception {
        URL resource = Objects.requireNonNull(
            TriggerTest.class.getClassLoader().getResource(flowResource),
            "Missing flow resource: " + flowResource
        );

        Flow parsed = JacksonMapper.ofYaml().readValue(resource, Flow.class);
        assertThat(parsed.getTriggers(), notNullValue());
        assertThat(parsed.getTriggers().size(), greaterThan(0));
        assertThat(parsed.getTriggers().getFirst(), instanceOf(Trigger.class));

        Trigger parsedTrigger = (Trigger) parsed.getTriggers().getFirst();
        var renderedOffsetsCommitMode = runContextFactory.of()
            .render(parsedTrigger.getOffsetsCommitMode())
            .as(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.class)
            .orElse(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_STOP);

        assertThat(renderedOffsetsCommitMode, is(expectedMode));

        CountDownLatch queueCount = new CountDownLatch(1);

        Flux<Execution> receive = TestsUtils.receive(executionQueue)
            .filter(execution -> {
                boolean matches = expectedFlowId.equals(execution.getFlowId());
                if (matches) {
                    queueCount.countDown();
                }
                return matches;
            })
            .take(1);

        repositoryLoader.load(resource);

        boolean await = queueCount.await(15, TimeUnit.SECONDS);
        assertThat(await, is(false));

        Integer size = (Integer) Objects.requireNonNull(receive.blockLast())
            .getTrigger()
            .getVariables()
            .get("size");

        assertThat(size, greaterThanOrEqualTo(5));
    }

    private void resetMaster() throws Exception {
        try (var connection = getConnection(); var statement = connection.createStatement()) {
            statement.execute("RESET MASTER");
        }
    }

    private void cleanupDebeziumState() throws Exception {
        cleanupState("io.kestra.tests", "trigger-default", "debezium-state-default-v2");
        cleanupState("io.kestra.plugin.debezium.mysql", "trigger-on-stop", "debezium-state-on-stop-v2");
        cleanupState("io.kestra.plugin.debezium.mysql", "trigger-on-each-batch", "debezium-state-on-each-batch-v2");
    }

    private void cleanupState(String namespace, String flowId, String... stateNames) throws Exception {
        KVStore kvStore;
        try {
            kvStore = kvStoreService.get(TenantService.MAIN_TENANT, namespace, namespace);
        } catch (KVStoreException e) {
            return;
        }

        String flowPrefix = Slugify.of(flowId) + "_states_";

        for (KVEntry kvEntry : kvStore.listAll()) {
            for (String stateName : stateNames) {
                if (kvEntry.key().startsWith(flowPrefix + stateName)) {
                    kvStore.delete(kvEntry.key());
                }
            }
        }
    }
}
