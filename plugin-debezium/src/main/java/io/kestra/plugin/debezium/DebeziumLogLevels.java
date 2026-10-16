package io.kestra.plugin.debezium;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import ch.qos.logback.classic.LoggerContext;

/**
 * Keeps the level of the Debezium and Kafka Connect loggers in line with the runs in progress.
 *
 * Logback levels are global to the JVM, so every Debezium task and trigger on a worker shares them.
 * Each run takes a {@link Lease} for the level it asked for. The effective level is the most verbose
 * one among the active leases, so a run can never silence another one, and when the last lease is
 * released the loggers go back to the {@link #BASELINE}.
 */
final class DebeziumLogLevels {
    // Loggers controlled by the `debeziumLogLevel` property.
    static final List<String> LOGGER_PREFIXES = List.of("org.apache.kafka.connect", "io.debezium");

    // Below INFO these loggers print the raw connector configuration, database password included:
    // AsyncEmbeddedEngine logs "Config #0: {...}" and DebeziumOpenLineageEmitter logs the config it is
    // initialised with. They never go below INFO, whatever level is requested.
    static final List<String> CAPPED_LOGGER_PREFIXES = List.of("io.debezium.embedded", "io.debezium.openlineage");

    // Level of the loggers while no Debezium run is active, the same as the static block it replaces.
    static final Level BASELINE = Level.ERROR;

    private static final Object LOCK = new Object();
    private static final Map<Lease, Level> ACTIVE = new HashMap<>();

    private DebeziumLogLevels() {
    }

    /**
     * Registers a run that wants the given level and applies the new effective level.
     */
    static Lease acquire(Level level) {
        var lease = new Lease();
        synchronized (LOCK) {
            ACTIVE.put(lease, level);
            apply(effectiveLevel());
        }
        return lease;
    }

    /**
     * Applies the baseline, unless a run currently holds a lease.
     */
    static void applyBaseline() {
        synchronized (LOCK) {
            if (ACTIVE.isEmpty()) {
                apply(BASELINE);
            }
        }
    }

    static int activeLeases() {
        synchronized (LOCK) {
            return ACTIVE.size();
        }
    }

    /**
     * The level a given logger gets when {@code requested} is the effective level.
     */
    static Level levelFor(String loggerName, Level requested) {
        if (matches(loggerName, CAPPED_LOGGER_PREFIXES) && isMoreVerbose(requested, Level.INFO)) {
            return Level.INFO;
        }
        return requested;
    }

    // must be called while holding LOCK
    private static Level effectiveLevel() {
        return ACTIVE.values().stream()
            .min(Comparator.comparingInt(Level::toInt))
            .orElse(BASELINE);
    }

    // must be called while holding LOCK
    private static void apply(Level level) {
        // make sure the parent loggers exist, so loggers created later inherit the right level
        Stream.concat(LOGGER_PREFIXES.stream(), CAPPED_LOGGER_PREFIXES.stream()).forEach(LoggerFactory::getLogger);

        // set it on every existing logger, not only on the parents: a child that got an explicit level
        // earlier would otherwise keep it
        ((LoggerContext) LoggerFactory.getILoggerFactory())
            .getLoggerList()
            .stream()
            .filter(logger -> matches(logger.getName(), LOGGER_PREFIXES))
            .forEach(logger -> logger.setLevel(ch.qos.logback.classic.Level.toLevel(levelFor(logger.getName(), level).name())));
    }

    private static boolean matches(String loggerName, List<String> prefixes) {
        return prefixes.stream().anyMatch(prefix -> loggerName.equals(prefix) || loggerName.startsWith(prefix + "."));
    }

    // a lower toInt() is more verbose: TRACE < DEBUG < INFO < WARN < ERROR
    private static boolean isMoreVerbose(Level level, Level than) {
        return level.toInt() < than.toInt();
    }

    /**
     * A run's claim on a log level. Closing it more than once is a no-op.
     */
    static final class Lease implements AutoCloseable {
        private boolean released;

        private Lease() {
        }

        @Override
        public void close() {
            synchronized (LOCK) {
                if (released) {
                    return;
                }
                released = true;
                ACTIVE.remove(this);
                apply(effectiveLevel());
            }
        }
    }
}
