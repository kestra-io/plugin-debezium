package io.kestra.plugin.debezium.oracle;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.event.Level;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.LogCapture;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class DebeziumLogLevelTest {
    private static final String USERNAME = "c##dbzuser";
    private static final String PASSWORD = "dbz";
    // the password is too short to search for on its own, so look for the credentials part of the URL
    private static final String URL_CREDENTIALS = USERNAME + "/" + PASSWORD + "@";

    @Inject
    private RunContextFactory runContextFactory;

    @ParameterizedTest
    @EnumSource(value = Level.class, names = {"INFO", "DEBUG", "TRACE"})
    void verboseLevelsDoNotPrintSecrets(Level level) throws Exception {
        var task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .snapshotMode(Property.ofValue(OracleInterface.SnapshotMode.INITIAL_ONLY))
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("1521"))
            .username(Property.ofValue(USERNAME))
            .sid(Property.ofValue("XE"))
            .password(Property.ofValue(PASSWORD))
            .stateName(Property.ofValue(UUID.randomUUID().toString()))
            .maxWait(Property.ofValue(Duration.ofSeconds(10)))
            .includedTables("KESTRA.EVENTS")
            .debeziumLogLevel(Property.ofValue(level))
            // an Oracle JDBC URL can hold the user and password
            .properties(Property.ofValue(Map.of("database.url", "jdbc:oracle:thin:" + URL_CREDENTIALS + "127.0.0.1:1521:XE")))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var logs = LogCapture.record();
        try (logs) {
            task.run(runContext);
        }

        assertTrue(logs.hasDebeziumOutputAtOrBelow(level), "expected Debezium " + level + " output while the capture ran");

        assertThat(logs.loggersPrinting(List.of(URL_CREDENTIALS)), is(List.of()));
    }
}
