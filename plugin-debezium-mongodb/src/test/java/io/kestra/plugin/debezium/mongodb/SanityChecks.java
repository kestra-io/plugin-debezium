package io.kestra.plugin.debezium.mongodb;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
@Disabled("Until there will be automatic way to execute mongo.js scripts")
class SanityChecks {

    @Test
    @ExecuteFlow("sanity-checks/mongodb-capture.yaml")
    void mongodbCapture(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
