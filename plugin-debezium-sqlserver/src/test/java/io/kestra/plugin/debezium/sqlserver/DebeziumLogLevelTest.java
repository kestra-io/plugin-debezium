package io.kestra.plugin.debezium.sqlserver;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTest;

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

    private static String fullText(ILoggingEvent event) {
        return event.getFormattedMessage() + (event.getThrowableProxy() == null ? "" : ThrowableProxyUtil.asString(event.getThrowableProxy()));
    }

    @ParameterizedTest
    @EnumSource(value = Level.class, names = {"INFO", "DEBUG", "TRACE"})
    void verboseLevelsDoNotPrintSecrets(Level level) throws Exception {
        // record everything logged while the capture runs
        var logs = new ListAppender<ILoggingEvent>();
        logs.start();
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.addAppender(logs);

        Capture task = Capture.builder()
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

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        try {
            task.run(runContext);
        } finally {
            root.detachAppender(logs);
        }

        // the requested level was applied while the capture ran
        assertTrue(
            logs.list.stream().anyMatch(event -> event.getLoggerName().startsWith("io.debezium.")
                && event.getLevel().toInt() <= ch.qos.logback.classic.Level.toLevel(level.name()).toInt()),
            "expected Debezium " + level + " output while the capture ran"
        );

        // no level may print a secret given to the connector, in the message or in an exception
        var secrets = List.of(getPassword(), TRUST_STORE_PASSWORD);
        var leaks = logs.list.stream()
            .filter(event -> secrets.stream().anyMatch(fullText(event)::contains))
            .map(ILoggingEvent::getLoggerName)
            .distinct()
            .toList();
        assertThat(leaks, is(List.of()));
    }
}
