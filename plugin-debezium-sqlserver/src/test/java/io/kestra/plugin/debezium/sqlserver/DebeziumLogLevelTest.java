package io.kestra.plugin.debezium.sqlserver;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.event.Level;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTest;
import io.kestra.plugin.debezium.LogCapture;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class DebeziumLogLevelTest extends AbstractDebeziumTest {
    // a camel-case secret passed through `properties`: Debezium's task configuration dump at INFO does not
    // recognise it as a password, so it is only kept out of the log by the level cap
    private static final String TRUST_STORE_PASSWORD = "leak-check-truststore";

    @Inject
    private RunContextFactory runContextFactory;

    @Override
    protected String getUrl() {
        return "jdbc:sqlserver://localhost:61433;trustServerCertificate=true;databaseName=deb";
    }

    @Override
    protected String getUsername() {
        return "sa";
    }

    @Override
    protected String getPassword() {
        return "Sqls3rv3r_Pa55word!";
    }

    @ParameterizedTest
    @EnumSource(value = Level.class, names = {"INFO", "DEBUG", "TRACE"})
    void verboseLevelsDoNotPrintSecrets(Level level) throws Exception {
        var task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .snapshotMode(Property.ofValue(SqlServerInterface.SnapshotMode.INITIAL_ONLY))
            .stateName(Property.ofValue(IdUtils.create()))
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("61433"))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .database(Property.ofValue("deb"))
            .maxWait(Property.ofValue(Duration.ofSeconds(10)))
            .includedTables(List.of("dbo.events"))
            .debeziumLogLevel(Property.ofValue(level))
            .properties(Property.ofValue(Map.of(
                "database.encrypt", "false",
                "database.trustStorePassword", TRUST_STORE_PASSWORD
            )))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        // record everything logged while the capture runs
        var logs = LogCapture.record();
        try (logs) {
            task.run(runContext);
        }

        // the requested level was applied while the capture ran
        assertTrue(logs.hasDebeziumOutputAtOrBelow(level), "expected Debezium " + level + " output while the capture ran");

        // no level may print a secret given to the connector
        assertThat(logs.loggersPrinting(List.of(getPassword(), TRUST_STORE_PASSWORD)), is(List.of()));
    }
}
