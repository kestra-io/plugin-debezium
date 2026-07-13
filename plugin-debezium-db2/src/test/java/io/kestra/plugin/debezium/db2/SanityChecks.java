package io.kestra.plugin.debezium.db2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.services.KVStoreService;
import io.kestra.plugin.debezium.AbstractDebeziumTest;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
@Disabled("The tests are disabled for CI, as db2 container have long time initialization")
class SanityChecks extends AbstractDebeziumTest {

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
    void setup() throws Exception {
        cleanupFlowState(kvStoreService, "sanitychecks.plugin-debezium-db2", "db2-capture", "debezium-state");
        executeSqlScript("scripts/db2.sql");
    }

    @Test
    @ExecuteFlow("sanity-checks/db2-capture.yaml")
    void db2Capture(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
