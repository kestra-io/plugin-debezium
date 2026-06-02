package io.kestra.plugin.debezium.postgres;

import org.junit.jupiter.api.BeforeEach;
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
class SanityChecks extends AbstractDebeziumTest {

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
    void setup() throws Exception {
        PostgresDebeziumTestHelper.dropReplicationArtifacts(this::getConnection, "kestra", "kestra_publication");
        cleanupFlowState(kvStoreService, "sanitychecks.plugin-debezium-postgres", "postgres-capture", "debezium-state");
        executeSqlScript("scripts/postgres.sql");
    }

    @Test
    @ExecuteFlow("sanity-checks/postgres-capture-ci.yaml")
    void postgresCapture(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
