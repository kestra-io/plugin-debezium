package io.kestra.plugin.debezium.mongodb;

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
import io.kestra.plugin.debezium.LogCapture;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

// unlike the other MongoDB tests, this one needs no data, only the database
@KestraTest
class DebeziumLogLevelTest {
    private static final String PASSWORD = "mongo_passwd";
    // passed through `properties`, the way any other connector secret would reach Debezium
    private static final String KEYSTORE_PASSWORD = "leak-check-keystore";

    @Inject
    private RunContextFactory runContextFactory;

    @ParameterizedTest
    @EnumSource(value = Level.class, names = {"INFO", "DEBUG", "TRACE"})
    void verboseLevelsDoNotPrintSecrets(Level level) throws Exception {
        var task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .snapshotMode(Property.ofValue(MongodbInterface.SnapshotMode.NO_DATA))
            .connectionString(Property.ofValue("mongodb://mongo_user:" + PASSWORD + "@127.0.0.1:27017/?replicaSet=rs0"))
            // required by the shared interface, the connection string is what the connector uses
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("27017"))
            .stateName(Property.ofValue("debezium-state-" + IdUtils.create()))
            .maxWait(Property.ofValue(Duration.ofSeconds(5)))
            .debeziumLogLevel(Property.ofValue(level))
            .properties(Property.ofValue(Map.of("mongodb.ssl.keystore.password", KEYSTORE_PASSWORD)))
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
        assertThat(logs.loggersPrinting(List.of(PASSWORD, KEYSTORE_PASSWORD)), is(List.of()));
    }
}
