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

    @Test
    void resolveTaskIdPrefersOwnId() {
        // During trigger evaluation taskRunInfo is empty (no execution yet), so the
        // task/trigger's own id is what keeps two triggers in the same flow distinct.
        assertThat(AbstractDebeziumTask.resolveTaskId("my-trigger", null), is("my-trigger"));
        // Own id always wins, even when taskRunInfo would also provide one.
        assertThat(AbstractDebeziumTask.resolveTaskId("my-task", "taskrun-task-id"), is("my-task"));
    }

    @Test
    void resolveTaskIdFallsBackWhenOwnIdMissing() {
        assertThat(AbstractDebeziumTask.resolveTaskId(null, "taskrun-task-id"), is("taskrun-task-id"));
        assertThat(AbstractDebeziumTask.resolveTaskId(null, null), is(""));
    }

    @Test
    void distinctTriggersInSameFlowGetDistinctIds() {
        // Two Debezium triggers in the same namespace/flow, differentiated only by their own
        // id — this is the trigger-evaluation case where taskRunInfo is empty. They must not
        // collide, otherwise their JMX MBean names would clash on the same JVM.
        var triggerA = AbstractDebeziumTask.connectorIdFromParts(
            "ns", "flow", AbstractDebeziumTask.resolveTaskId("trigger-a", null), "");
        var triggerB = AbstractDebeziumTask.connectorIdFromParts(
            "ns", "flow", AbstractDebeziumTask.resolveTaskId("trigger-b", null), "");

        assertThat(triggerA, is(not(triggerB)));
    }
}
