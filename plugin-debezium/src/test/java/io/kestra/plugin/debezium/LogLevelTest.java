package io.kestra.plugin.debezium;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class LogLevelTest {

    private static final List<String> NAMES = List.of(
        "io.debezium",
        "org.apache.kafka.connect",
        "io.debezium.test.child",
        "org.apache.kafka.connect.test.child",
        "io.kestra.plugin.debezium.test.unrelated"
    );

    // The levels are global to the JVM, so put back whatever other tests had.
    private final Map<String, Level> previous = new HashMap<>();

    @BeforeEach
    void rememberLevels() {
        NAMES.forEach(name -> previous.put(name, logger(name).getLevel()));
    }

    @AfterEach
    void restoreLevels() {
        NAMES.forEach(name -> logger(name).setLevel(previous.get(name)));
    }

    private static Logger logger(String name) {
        return (Logger) LoggerFactory.getLogger(name);
    }

    @Test
    void defaultIsWarn() {
        // Per the issue: ERROR hides too much, WARN is the out-of-the-box level.
        assertThat(AbstractDebeziumTask.DEFAULT_LOG_LEVEL, is(org.slf4j.event.Level.WARN));
    }

    @ParameterizedTest
    @EnumSource(org.slf4j.event.Level.class)
    void everyLevelSetsTheLogbackLevelOfTheSameName(org.slf4j.event.Level level) {
        AbstractDebeziumTask.applyLogLevel(level);

        assertThat(logger("io.debezium").getLevel().toString(), is(level.name()));
    }

    @Test
    void appliesTheLevelToDebeziumAndKafkaConnectLoggers() {
        AbstractDebeziumTask.applyLogLevel(org.slf4j.event.Level.DEBUG);

        assertThat(logger("io.debezium").getLevel(), is(Level.DEBUG));
        assertThat(logger("org.apache.kafka.connect").getLevel(), is(Level.DEBUG));
    }

    @Test
    void childLoggersThatAlreadyExistFollowTheNewLevel() {
        // A child that got an explicit level on a previous run would not inherit the parent's new level.
        AbstractDebeziumTask.applyLogLevel(org.slf4j.event.Level.ERROR);
        assertThat(logger("io.debezium.test.child").getLevel(), is(Level.ERROR));
        assertThat(logger("org.apache.kafka.connect.test.child").getLevel(), is(Level.ERROR));

        AbstractDebeziumTask.applyLogLevel(org.slf4j.event.Level.INFO);
        assertThat(logger("io.debezium.test.child").getLevel(), is(Level.INFO));
        assertThat(logger("org.apache.kafka.connect.test.child").getLevel(), is(Level.INFO));
    }

    @Test
    void lastAppliedLevelWins() {
        AbstractDebeziumTask.applyLogLevel(org.slf4j.event.Level.DEBUG);
        AbstractDebeziumTask.applyLogLevel(org.slf4j.event.Level.WARN);

        assertThat(logger("io.debezium").getLevel(), is(Level.WARN));
    }

    @Test
    void doesNotTouchUnrelatedLoggers() {
        logger("io.kestra.plugin.debezium.test.unrelated").setLevel(null);

        AbstractDebeziumTask.applyLogLevel(org.slf4j.event.Level.TRACE);

        // `io.kestra.plugin.debezium...` is not under `io.debezium`, so it keeps inheriting.
        assertThat(logger("io.kestra.plugin.debezium.test.unrelated").getLevel(), is(nullValue()));
    }
}
