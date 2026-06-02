package io.kestra.plugin.debezium.mongodb;

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
        return "";
    }

    @Override
    protected String getUsername() {
        return "";
    }

    @Override
    protected String getPassword() {
        return "";
    }

    @BeforeEach
    void setup() throws Exception {
        cleanupFlowState(kvStoreService, "sanitychecks.plugin-debezium-mongodb", "mongodb-capture", "debezium-state");
    }

    @Test
    @ExecuteFlow("sanity-checks/mongodb-capture.yaml")
    void mongodbCapture(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
