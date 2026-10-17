package io.kestra.plugin.debezium.oracle;

import java.io.FileNotFoundException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Optional;

import org.h2.tools.RunScript;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.services.KVStoreService;
import io.kestra.plugin.debezium.AbstractDebeziumTest;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerTest extends AbstractDebeziumTest {

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

    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        try {
            RunScript.execute(getConnection(), new StringReader("DROP TABLE trigger_events;"));
        } catch (Exception ignored) {
        }
        executeSqlScript("scripts/oracle-trigger.sql");
    }

    @BeforeEach
    void setUp() throws Exception {
        // Start each run from a clean offset instead of resuming a stale redo-log position.
        cleanupFlowState(kvStoreService, "io.kestra.tests", "trigger", "someStateNameForTrigger");
        initDatabase();
    }

    @Test
    @EvaluateTrigger(flow = "flows/trigger.yaml", triggerId = "watch")
    void flow(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        Integer size = (Integer) optionalExecution.get().getTrigger().getVariables().get("size");
        assertThat(size, greaterThanOrEqualTo(5));
    }
}
