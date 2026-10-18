package io.kestra.plugin.debezium;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import ch.qos.logback.classic.LoggerContext;

/**
 * Sets the Debezium and Kafka Connect loggers to the most verbose level among the active runs' leases,
 * and back to {@link #BASELINE} when the last lease is released.
 */
final class DebeziumLogLevels {
    static final List<String> LOGGER_PREFIXES = List.of("org.apache.kafka.connect", "io.debezium");

    // These loggers print connection settings, secrets included, so they never go below the level given
    // here, whatever level is requested:
    // - AsyncEmbeddedEngine logs "Config #0: {...}" with the database password (DEBUG),
    // - JdbcConnection logs "Connected to ... with {...}" (DEBUG) and "Config: {...}" (TRACE) with the
    //   SSL key password and any secret passed through `properties`,
    // - DebeziumOpenLineageEmitter logs the configuration it is initialised with (DEBUG),
    // - BaseSourceTask logs every connector setting when a task starts (INFO); it masks the keys it knows
    //   as passwords but not others, such as SQL Server's `database.trustStorePassword`,
    // - OracleConnection logs the raw `database.url`, which can hold the user and password (TRACE).
    static final Map<String, Level> CAPPED_LOGGERS = Map.of(
        "io.debezium.embedded", Level.INFO,
        "io.debezium.jdbc", Level.INFO,
        "io.debezium.openlineage", Level.INFO,
        "io.debezium.connector.common.BaseSourceTask", Level.WARN,
        "io.debezium.connector.oracle.OracleConnection", Level.DEBUG
    );

    static final Level BASELINE = Level.ERROR;

    // every connector plugin has its own copy of this class, so the leases live in the shared logback context
    static final String REGISTRY_KEY = "io.kestra.plugin.debezium.logLevelLeases";

    private DebeziumLogLevels() {
    }

    static Lease acquire(Level level) {
        var registry = registry();
        var lease = new Lease(registry);
        synchronized (registry) {
            var before = effectiveLevel(registry);
            registry.put(lease, level.toInt());
            applyIfChanged(before, registry);
        }
        return lease;
    }

    static void applyBaseline() {
        var registry = registry();
        synchronized (registry) {
            if (registry.isEmpty()) {
                apply(BASELINE);
            }
        }
    }

    static Level levelFor(String loggerName, Level requested) {
        return CAPPED_LOGGERS.entrySet().stream()
            .filter(cap -> matches(loggerName, cap.getKey()))
            .map(Map.Entry::getValue)
            .filter(cap -> isMoreVerbose(requested, cap))
            .findFirst()
            .orElse(requested);
    }

    private static LoggerContext loggerContext() {
        return (LoggerContext) LoggerFactory.getILoggerFactory();
    }

    @SuppressWarnings("unchecked")
    private static Map<Object, Integer> registry() {
        var context = loggerContext();
        synchronized (context) {
            var registry = (Map<Object, Integer>) context.getObject(REGISTRY_KEY);
            if (registry == null) {
                registry = new HashMap<>();
                context.putObject(REGISTRY_KEY, registry);
            }
            return registry;
        }
    }

    private static Level effectiveLevel(Map<Object, Integer> registry) {
        // a lower toInt() is more verbose: TRACE < DEBUG < INFO < WARN < ERROR
        return registry.values().stream()
            .min(Integer::compare)
            .map(Level::intToLevel)
            .orElse(BASELINE);
    }

    private static void applyIfChanged(Level before, Map<Object, Integer> registry) {
        var after = effectiveLevel(registry);
        if (after != before) {
            apply(after);
        }
    }

    private static void apply(Level level) {
        // make sure the parent loggers exist, so loggers created later inherit the right level
        Stream.concat(LOGGER_PREFIXES.stream(), CAPPED_LOGGERS.keySet().stream()).forEach(LoggerFactory::getLogger);

        // also children: one that got an explicit level earlier would otherwise keep it
        loggerContext()
            .getLoggerList()
            .stream()
            .filter(logger -> LOGGER_PREFIXES.stream().anyMatch(prefix -> matches(logger.getName(), prefix)))
            .forEach(logger -> logger.setLevel(ch.qos.logback.classic.Level.toLevel(levelFor(logger.getName(), level).name())));
    }

    private static boolean matches(String loggerName, String prefix) {
        return loggerName.equals(prefix) || loggerName.startsWith(prefix + ".");
    }

    private static boolean isMoreVerbose(Level level, Level than) {
        return level.toInt() < than.toInt();
    }

    // closing a lease more than once is a no-op
    static final class Lease implements AutoCloseable {
        private final Map<Object, Integer> registry;

        private Lease(Map<Object, Integer> registry) {
            this.registry = registry;
        }

        @Override
        public void close() {
            synchronized (registry) {
                var before = effectiveLevel(registry);
                if (registry.remove(this) != null) {
                    applyIfChanged(before, registry);
                }
            }
        }
    }
}
