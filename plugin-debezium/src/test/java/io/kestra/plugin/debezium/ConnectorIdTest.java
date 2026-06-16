package io.kestra.plugin.debezium;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ConnectorIdTest {

    @Test
    void derivedIdHasExpectedFormat() {
        var id = AbstractDebeziumTask.connectorIdFromParts("my.namespace", "my-flow", "my-task", "");

        assertThat(id, startsWith("kestra_"));
        // "kestra_" (7 chars) + 8 hex chars = 15
        assertThat(id.length(), is(15));
        assertThat(id, matchesPattern("kestra_[0-9a-f]{8}"));
    }

    @Test
    void sameInputsProduceSameId() {
        var id1 = AbstractDebeziumTask.connectorIdFromParts("ns", "flow", "task", "");
        var id2 = AbstractDebeziumTask.connectorIdFromParts("ns", "flow", "task", "");

        assertThat(id1, is(id2));
    }

    @Test
    void differentTaskIdsProduceDifferentIds() {
        var id1 = AbstractDebeziumTask.connectorIdFromParts("ns", "flow", "task-a", "");
        var id2 = AbstractDebeziumTask.connectorIdFromParts("ns", "flow", "task-b", "");

        assertThat(id1, is(not(id2)));
    }

    @Test
    void differentIterationValuesProduceDifferentIds() {
        var id1 = AbstractDebeziumTask.connectorIdFromParts("ns", "flow", "task", "iteration-1");
        var id2 = AbstractDebeziumTask.connectorIdFromParts("ns", "flow", "task", "iteration-2");

        assertThat(id1, is(not(id2)));
    }

    @Test
    void differentNamespacesProduceDifferentIds() {
        var id1 = AbstractDebeziumTask.connectorIdFromParts("company.team-a", "flow", "task", "");
        var id2 = AbstractDebeziumTask.connectorIdFromParts("company.team-b", "flow", "task", "");

        assertThat(id1, is(not(id2)));
    }
}
