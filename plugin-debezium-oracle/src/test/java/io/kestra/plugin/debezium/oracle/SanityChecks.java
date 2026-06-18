package io.kestra.plugin.debezium.oracle;

import java.io.StringReader;

import org.h2.tools.RunScript;
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

    @BeforeEach
    void setup() throws Exception {
        cleanupFlowState(kvStoreService, "sanitychecks.plugin-debezium-oracle", "oracle-capture", "debezium-state");
        try {
            RunScript.execute(getConnection(), new StringReader("DROP TABLE events"));
        } catch (Exception ignored) {
        }
        executeSqlScript("scripts/oracle.sql");
    }

    @Test
    @ExecuteFlow("sanity-checks/oracle-capture.yaml")
    void oracleCapture(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
