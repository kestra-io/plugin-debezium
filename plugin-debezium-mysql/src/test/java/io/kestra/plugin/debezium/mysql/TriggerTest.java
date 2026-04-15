package io.kestra.plugin.debezium.mysql;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.plugin.debezium.AbstractDebeziumTest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerTest extends AbstractDebeziumTest {

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

    @BeforeEach
    void setUp() throws Exception {
        executeSqlScript("scripts/mysql.sql");
    }

    @Test
    @EvaluateTrigger(flow = "flows/trigger.yaml", triggerId = "watch")
    void flow(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Integer size = (Integer) optionalExecution.get().getTrigger().getVariables().get("size");
        assertThat(size, greaterThanOrEqualTo(5));
    }
}
