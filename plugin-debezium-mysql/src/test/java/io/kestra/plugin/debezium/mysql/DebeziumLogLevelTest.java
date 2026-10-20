package io.kestra.plugin.debezium.mysql;

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
    // passed through `properties`, the way any other connector secret would reach Debezium
    private static final String KEYSTORE_PASSWORD = "leak-check-keystore";

    @Inject
    private RunContextFactory runContextFactory;

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

    @ParameterizedTest
    @EnumSource(value = Level.class, names = {"INFO", "DEBUG", "TRACE"})
    void verboseLevelsDoNotPrintSecrets(Level level) throws Exception {
        executeSqlScript("scripts/mysql.sql");

        var task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .serverId(Property.ofValue("123456790"))
            .snapshotMode(Property.ofValue(MysqlInterface.SnapshotMode.NEVER))
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("63306"))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .stateName(Property.ofValue("debezium-state-" + IdUtils.create()))
            .maxWait(Property.ofValue(Duration.ofSeconds(5)))
            .includedTables(List.of("kestra.capture_events"))
            .debeziumLogLevel(Property.ofValue(level))
            .properties(Property.ofValue(Map.of("database.ssl.keystore.password", KEYSTORE_PASSWORD)))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var logs = LogCapture.record();
        try (logs) {
            task.run(runContext);
        }

        assertTrue(logs.hasDebeziumOutputAtOrBelow(level), "expected Debezium " + level + " output while the capture ran");

        assertThat(logs.loggersPrinting(List.of(getPassword(), KEYSTORE_PASSWORD)), is(List.of()));
    }
}
