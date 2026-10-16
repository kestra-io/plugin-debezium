package io.kestra.plugin.debezium;

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
 *
 * Every connector plugin (PostgreSQL, MySQL, ...) ships its own copy of this class in its own class
 * loader, but they all log through the same logback {@link LoggerContext}. So the leases are not kept in
 * a field of this class: they live in that shared context, under {@link #REGISTRY_KEY}, as a map that only
 * holds JDK types, and every copy reads and updates the same map.
 */
final class DebeziumLogLevels {
    // Loggers controlled by the `debeziumLogLevel` property.
    static final List<String> LOGGER_PREFIXES = List.of("org.apache.kafka.connect", "io.debezium");

    // These loggers print connection settings, secrets included, so they never go below the level given
    // here, whatever level is requested:
    // - AsyncEmbeddedEngine logs "Config #0: {...}" with the database password (DEBUG),
    // - JdbcConnection logs "Connected to ... with {...}" (DEBUG) and "Config: {...}" (TRACE) with the
    //   SSL key password and any secret passed through `properties`,
    // - DebeziumOpenLineageEmitter logs the configuration it is initialised with (DEBUG),
    // - BaseSourceTask logs every connector setting when a task starts (INFO); it masks the keys it knows
    //   as passwords but not others, such as SQL Server's `database.trustStorePassword`.
    static final Map<String, Level> CAPPED_LOGGERS = Map.of(
        "io.debezium.embedded", Level.INFO,
        "io.debezium.jdbc", Level.INFO,
        "io.debezium.openlineage", Level.INFO,
        "io.debezium.connector.common.BaseSourceTask", Level.WARN
    );

    // Level of the loggers while no Debezium run is active, the same as the static block it replaces.
    static final Level BASELINE = Level.ERROR;

    // Key of the shared lease registry in the logback context: lease -> requested level as Level#toInt().
    static final String REGISTRY_KEY = "io.kestra.plugin.debezium.logLevelLeases";

    private DebeziumLogLevels() {
    }

    /**
     * Registers a run that wants the given level and applies the new effective level.
     */
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

    /**
     * Applies the baseline, unless a run of any connector currently holds a lease.
     */
    static void applyBaseline() {
        var registry = registry();
        synchronized (registry) {
            if (registry.isEmpty()) {
                apply(BASELINE);
            }
        }
    }

    /**
     * The level a given logger gets when {@code requested} is the effective level.
     */
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
        // the context is the one object every copy of this class shares, so it guards the creation
        synchronized (context) {
            var registry = (Map<Object, Integer>) context.getObject(REGISTRY_KEY);
            if (registry == null) {
                registry = new HashMap<>();
                context.putObject(REGISTRY_KEY, registry);
            }
            return registry;
        }
    }

    // must be called while holding the registry's lock
    private static Level effectiveLevel(Map<Object, Integer> registry) {
        // a lower toInt() is more verbose: TRACE < DEBUG < INFO < WARN < ERROR
        return registry.values().stream()
            .min(Integer::compare)
            .map(Level::intToLevel)
            .orElse(BASELINE);
    }

    // must be called while holding the registry's lock; walking every logger is only worth it when the
    // effective level changed, which most acquires and releases at the default level don't do
    private static void applyIfChanged(Level before, Map<Object, Integer> registry) {
        var after = effectiveLevel(registry);
        if (after != before) {
            apply(after);
        }
    }

    // must be called while holding the registry's lock
    private static void apply(Level level) {
        // make sure the parent loggers exist, so loggers created later inherit the right level
        Stream.concat(LOGGER_PREFIXES.stream(), CAPPED_LOGGERS.keySet().stream()).forEach(LoggerFactory::getLogger);

        // set it on every existing logger, not only on the parents: a child that got an explicit level
        // earlier would otherwise keep it
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

    /**
     * A run's claim on a log level. Closing it more than once is a no-op.
     */
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
