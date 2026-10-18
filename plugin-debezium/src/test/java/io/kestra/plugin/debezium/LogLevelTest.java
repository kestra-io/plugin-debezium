package io.kestra.plugin.debezium;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import ch.qos.logback.classic.Logger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class LogLevelTest {

    @BeforeEach
    @AfterEach
    void noRunActive() {
        // the levels are global to the JVM, so every test starts and ends with no lease held, which
        // means the loggers are at the baseline
        DebeziumLogLevels.applyBaseline();
        assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.ERROR));
    }

    private static ch.qos.logback.classic.Level level(String loggerName) {
        return ((Logger) LoggerFactory.getLogger(loggerName)).getLevel();
    }

    // the level a logger actually logs at, inherited from its parents when it has none of its own
    private static ch.qos.logback.classic.Level effectiveLevel(String loggerName) {
        return ((Logger) LoggerFactory.getLogger(loggerName)).getEffectiveLevel();
    }

    private static ch.qos.logback.classic.Level logback(Level level) {
        return ch.qos.logback.classic.Level.toLevel(level.name());
    }

    @Test
    void defaultIsError() {
        // existing flows keep the ERROR level they always had; users opt in to more
        assertThat(AbstractDebeziumTask.DEFAULT_LOG_LEVEL, is(Level.ERROR));
    }

    @Test
    void baselineIsErrorWhenNoRunIsActive() {
        assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.ERROR));
        assertThat(level("org.apache.kafka.connect"), is(ch.qos.logback.classic.Level.ERROR));
    }

    @ParameterizedTest
    @EnumSource(Level.class)
    void everyLevelIsAppliedWhileTheRunIsActive(Level requested) {
        try (var ignored = DebeziumLogLevels.acquire(requested)) {
            assertThat(level("io.debezium"), is(logback(requested)));
            assertThat(level("org.apache.kafka.connect"), is(logback(requested)));
        }
    }

    @ParameterizedTest
    @EnumSource(value = Level.class, names = {"DEBUG", "TRACE"})
    void loggersThatPrintTheConnectorConfigNeverGoBelowInfo(Level requested) {
        try (var ignored = DebeziumLogLevels.acquire(requested)) {
            assertThat(level("io.debezium"), is(logback(requested)));
            assertThat(level("io.debezium.embedded"), is(ch.qos.logback.classic.Level.INFO));
            assertThat(level("io.debezium.jdbc"), is(ch.qos.logback.classic.Level.INFO));
            assertThat(effectiveLevel("io.debezium.jdbc.JdbcConnection"), is(ch.qos.logback.classic.Level.INFO));
            assertThat(effectiveLevel("io.debezium.embedded.async.AsyncEmbeddedEngine"), is(ch.qos.logback.classic.Level.INFO));
            assertThat(level("io.debezium.openlineage"), is(ch.qos.logback.classic.Level.INFO));
            assertThat(effectiveLevel("io.debezium.openlineage.DebeziumOpenLineageEmitter"), is(ch.qos.logback.classic.Level.INFO));
        }
    }

    @ParameterizedTest
    @EnumSource(value = Level.class, names = {"ERROR", "WARN", "INFO"})
    void levelsAtOrAboveInfoAreNotCapped(Level requested) {
        try (var ignored = DebeziumLogLevels.acquire(requested)) {
            assertThat(level("io.debezium.embedded"), is(logback(requested)));
            assertThat(level("io.debezium.jdbc"), is(logback(requested)));
            assertThat(level("io.debezium.openlineage"), is(logback(requested)));
        }
    }

    @ParameterizedTest
    @EnumSource(value = Level.class, names = {"INFO", "DEBUG", "TRACE"})
    void theTaskConfigurationDumpNeverGoesBelowWarn(Level requested) {
        // BaseSourceTask logs every connector setting at INFO, and only masks the keys it knows as passwords
        try (var ignored = DebeziumLogLevels.acquire(requested)) {
            assertThat(level("io.debezium"), is(logback(requested)));
            assertThat(effectiveLevel("io.debezium.connector.common.BaseSourceTask"), is(ch.qos.logback.classic.Level.WARN));
        }
    }

    @ParameterizedTest
    @EnumSource(value = Level.class, names = {"ERROR", "WARN"})
    void theTaskConfigurationDumpFollowsLessVerboseLevels(Level requested) {
        try (var ignored = DebeziumLogLevels.acquire(requested)) {
            assertThat(effectiveLevel("io.debezium.connector.common.BaseSourceTask"), is(logback(requested)));
        }
    }

    @Test
    void theOracleJdbcUrlIsNeverLoggedAtTrace() {
        // OracleConnection logs the raw `database.url` at TRACE, and it can hold the user and password
        try (var ignored = DebeziumLogLevels.acquire(Level.TRACE)) {
            assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.TRACE));
            assertThat(effectiveLevel("io.debezium.connector.oracle.OracleConnection"), is(ch.qos.logback.classic.Level.DEBUG));
        }
        try (var ignored = DebeziumLogLevels.acquire(Level.DEBUG)) {
            assertThat(effectiveLevel("io.debezium.connector.oracle.OracleConnection"), is(ch.qos.logback.classic.Level.DEBUG));
        }
    }

    @Test
    void theMostVerboseActiveRunWins() {
        var debugRun = DebeziumLogLevels.acquire(Level.DEBUG);
        var warnRun = DebeziumLogLevels.acquire(Level.WARN);
        try {
            // the WARN run starting after the DEBUG one does not silence it
            assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.DEBUG));

            debugRun.close();
            assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.WARN));
        } finally {
            debugRun.close();
            warnRun.close();
        }
    }

    @Test
    void theBaselineIsRestoredWhenTheLastRunEnds() {
        var first = DebeziumLogLevels.acquire(Level.INFO);
        var second = DebeziumLogLevels.acquire(Level.DEBUG);

        second.close();
        assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.INFO));

        first.close();
        assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.ERROR));
        assertThat(level("io.debezium.embedded"), is(ch.qos.logback.classic.Level.ERROR));
    }

    @Test
    void closingALeaseTwiceIsANoOp() {
        var debugRun = DebeziumLogLevels.acquire(Level.DEBUG);
        var infoRun = DebeziumLogLevels.acquire(Level.INFO);
        try {
            debugRun.close();
            debugRun.close();

            // the second close must not release the INFO run
            assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.INFO));
        } finally {
            infoRun.close();
        }
    }

    @Test
    void childLoggersThatAlreadyExistFollowTheLevel() {
        LoggerFactory.getLogger("io.debezium.test.child");
        LoggerFactory.getLogger("org.apache.kafka.connect.test.child");

        try (var ignored = DebeziumLogLevels.acquire(Level.DEBUG)) {
            assertThat(level("io.debezium.test.child"), is(ch.qos.logback.classic.Level.DEBUG));
            assertThat(level("org.apache.kafka.connect.test.child"), is(ch.qos.logback.classic.Level.DEBUG));
        }

        assertThat(level("io.debezium.test.child"), is(ch.qos.logback.classic.Level.ERROR));
    }

    @Test
    void doesNotTouchUnrelatedLoggers() {
        // `io.kestra.plugin.debezium` contains "debezium" and `io.debeziumx` shares the prefix, but neither
        // is under `io.debezium`
        ((Logger) LoggerFactory.getLogger("io.kestra.plugin.debezium.test.unrelated")).setLevel(null);
        ((Logger) LoggerFactory.getLogger("io.debeziumx.test")).setLevel(null);

        try (var ignored = DebeziumLogLevels.acquire(Level.TRACE)) {
            assertThat(level("io.kestra.plugin.debezium.test.unrelated"), is(nullValue()));
            assertThat(level("io.debeziumx.test"), is(nullValue()));
        }
    }

    /**
     * Every connector JAR ships its own copy of this plugin's classes, each in its own class loader, while
     * logback is shared. Loads a second, independent copy of {@link DebeziumLogLevels} the same way.
     */
    private static Class<?> copyFromAnotherConnector() throws ClassNotFoundException {
        var name = DebeziumLogLevels.class.getName();
        var isolated = new ClassLoader(LogLevelTest.class.getClassLoader()) {
            @Override
            protected Class<?> loadClass(String className, boolean resolve) throws ClassNotFoundException {
                if (!className.equals(name) && !className.startsWith(name + "$")) {
                    return super.loadClass(className, resolve);
                }
                synchronized (getClassLoadingLock(className)) {
                    Class<?> loaded = findLoadedClass(className);
                    if (loaded == null) {
                        try (var in = getParent().getResourceAsStream(className.replace('.', '/') + ".class")) {
                            var bytes = in.readAllBytes();
                            loaded = defineClass(className, bytes, 0, bytes.length);
                        } catch (IOException e) {
                            throw new ClassNotFoundException(className, e);
                        }
                    }
                    return loaded;
                }
            }
        };
        return isolated.loadClass(name);
    }

    @Test
    void runsOfOtherConnectorsShareTheSameLeases() throws Exception {
        var otherConnector = copyFromAnotherConnector();
        var acquire = otherConnector.getDeclaredMethod("acquire", Level.class);
        acquire.setAccessible(true);
        var applyBaseline = otherConnector.getDeclaredMethod("applyBaseline");
        applyBaseline.setAccessible(true);

        // e.g. a PostgreSQL realtime trigger at DEBUG ...
        try (var postgresTrigger = DebeziumLogLevels.acquire(Level.DEBUG)) {
            // ... while a MySQL capture runs at WARN and finishes
            try (var mysqlCapture = (AutoCloseable) acquire.invoke(null, Level.WARN)) {
                assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.DEBUG));
            }
            assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.DEBUG));

            // ... and the MySQL task class initialising must not reset it either
            applyBaseline.invoke(null);
            assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.DEBUG));
        }

        assertThat(level("io.debezium"), is(ch.qos.logback.classic.Level.ERROR));
    }
}
