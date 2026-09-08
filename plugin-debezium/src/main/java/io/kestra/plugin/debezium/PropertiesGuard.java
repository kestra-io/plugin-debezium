package io.kestra.plugin.debezium;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Rejects `properties` entries that would load arbitrary classes into the worker JVM or move
 * Debezium's internal storage somewhere the plugin does not control.
 *
 * Debezium resolves a number of options by fully-qualified class name and instantiates them by
 * reflection, the embedded engine's Kafka Connect worker instantiates `config.providers` classes in
 * its constructor, and `database.*` / `driver.*` entries pass straight through to the JDBC driver,
 * which has its own class-name and local-file options. `properties` is flow-author input, so those
 * keys are a code-execution path into the worker and are refused rather than silently dropped.
 */
final class PropertiesGuard {
    /** Debezium / Kafka Connect options whose value is a fully-qualified class name. */
    private static final Set<String> DENIED_KEYS = Set.of(
        "config.providers",
        "connector.class",
        "converters",
        "offset.commit.policy",
        "offset.storage",
        "post.processors",
        "predicates",
        "schema.history.internal",
        "sourceinfo.struct.maker",
        "topic.naming.strategy",
        "transaction.metadata.factory",
        "transforms"
    );

    /**
     * Subtrees denied wholesale: the converter chain, the `<chain>.<name>.type` transform, predicate
     * and post-processor chains, Kafka Connect's `config.providers.<name>.class` provider chain, and
     * the offset and schema-history stores the plugin owns. Only the schema-history backend and its
     * Kafka client config are blocked, the DDL tuning flags such as
     * `schema.history.internal.skip.unparseable.ddl` stay usable.
     */
    private static final List<String> DENIED_PREFIXES = List.of(
        "config.providers.",
        "header.converter",
        "internal.key.converter",
        "internal.value.converter",
        "key.converter",
        "offset.storage.",
        "post.processors.",
        "predicates.",
        "schema.history.internal.consumer.",
        "schema.history.internal.file.",
        "schema.history.internal.kafka.",
        "schema.history.internal.producer.",
        "transforms.",
        "value.converter"
    );

    /** Prefixes Debezium strips before handing the remainder to the JDBC driver. */
    private static final List<String> JDBC_PASSTHROUGH_PREFIXES = List.of("database.", "driver.");

    /**
     * JDBC parameters that load a class, deserialize server-controlled bytes, or read local files.
     * Lower-cased because Connector/J and pgjdbc both match parameter names case-insensitively.
     */
    private static final Set<String> DENIED_JDBC_PARAMETERS = Set.of(
        // Debezium JdbcConfiguration, cross-connector: FQCN of a JdbcConnection.ConnectionFactory
        "connection.factory.class",
        // MySQL Connector/J
        "allowloadlocalinfile",
        "allowloadlocalinfileinpath",
        "allowurlinlocalinfile",
        "authenticationplugins",
        // removed in Connector/J 9.x, kept so an older driver on the classpath is still covered
        "autodeserialize",
        "clientinfoprovider",
        "connectionlifecycleinterceptors",
        "defaultauthenticationplugin",
        "disabledauthenticationplugins",
        "exceptioninterceptors",
        "keymanagerfactoryprovider",
        "keystoreprovider",
        "parseinfocachefactory",
        "propertiestransform",
        "queryinfocachefactory",
        "queryinterceptors",
        "serverconfigcachefactory",
        "serverrsapublickeyfile",
        "sslcontextprovider",
        "trustmanagerfactoryprovider",
        // pgjdbc, including the CVE-2022-21724 socket and ssl factory pair
        "authenticationpluginclassname",
        "loggerfile",
        "loggerlevel",
        "socketfactory",
        "socketfactoryarg",
        "sslfactory",
        "sslfactoryarg",
        "sslhostnameverifier",
        "sslpasswordcallback",
        "xmlfactoryfactory",
        // SQL Server
        "accesstokencallbackclass",
        "socketfactoryclass",
        "socketfactoryconstructorarg",
        "trustmanagerclass"
    );

    /**
     * Catch-all for the shape these driver parameters keep taking. An explicit list goes stale on
     * every driver bump -- Connector/J 9.6 alone added five `*Provider` / `*CacheFactory` options --
     * so any pass-through parameter naming a class is refused whether or not it is listed above.
     * Compared against the parameter with dots removed, so dotted class-shaped keys such as
     * `connection.factory.class` still match. JCA algorithm names such as Oracle's
     * `ssl.keyManagerFactory.algorithm` collapse to `sslkeymanagerfactoryalgorithm` and do not match.
     */
    private static final List<String> DENIED_JDBC_PARAMETER_SUFFIXES = List.of(
        "callbackclass",
        "classname",
        "factory",
        "factoryarg",
        "factoryclass",
        "factoryconstructorarg",
        "hostnameverifier",
        "interceptor",
        "interceptors",
        "passwordcallback",
        "plugin",
        "plugins",
        "provider",
        "transform"
    );

    private PropertiesGuard() {
    }

    static void ensureAllowed(String key) {
        if (key == null) {
            return;
        }

        String normalized = key.trim().toLowerCase(Locale.ROOT);

        if (DENIED_KEYS.contains(normalized)) {
            throw reject(key);
        }

        for (String prefix : DENIED_PREFIXES) {
            if (normalized.startsWith(prefix)) {
                throw reject(key);
            }
        }

        for (String prefix : JDBC_PASSTHROUGH_PREFIXES) {
            if (!normalized.startsWith(prefix)) {
                continue;
            }

            String parameter = normalized.substring(prefix.length());
            if (DENIED_JDBC_PARAMETERS.contains(parameter)) {
                throw reject(key);
            }

            // Collapse dots so a dotted class-shaped key (e.g. `connection.factory.class`) is matched
            // by the undotted suffixes below, closing the raw-endsWith gap.
            String collapsed = parameter.replace(".", "");
            for (String suffix : DENIED_JDBC_PARAMETER_SUFFIXES) {
                if (collapsed.endsWith(suffix)) {
                    throw reject(key);
                }
            }
        }
    }

    private static IllegalArgumentException reject(String key) {
        return new IllegalArgumentException(
            "The property '" + key + "' is not allowed: it can load arbitrary classes into the worker " +
                "or redirect Debezium internal storage. Remove it from `properties`."
        );
    }
}
