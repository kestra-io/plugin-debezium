package io.kestra.plugin.debezium;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Rejects `properties` entries that would load arbitrary classes into the worker JVM, read or write
 * local files, redirect the connection, or move Debezium's internal storage somewhere the plugin
 * does not control.
 *
 * Debezium resolves a number of options by fully-qualified class name and instantiates them by
 * reflection, the embedded engine's Kafka Connect worker instantiates `config.providers` classes in
 * its constructor, and `database.*` / `driver.*` entries pass straight through to the JDBC driver,
 * which has its own class-name, URL and local-file options. `properties` is flow-author input, so
 * those keys are a code-execution / file-access path into the worker and are refused rather than
 * silently dropped.
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
     * the offset store the plugin owns.
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
        "transforms.",
        "value.converter"
    );

    /**
     * The whole `schema.history.internal.` subtree is denied (any backend and its client config),
     * except the DDL-handling flags below. Enumerating the allowed half keeps new storage backends
     * (`.jdbc.*`, `.redis.*`, ...) blocked without another list to maintain.
     */
    private static final String SCHEMA_HISTORY_PREFIX = "schema.history.internal.";

    private static final Set<String> SCHEMA_HISTORY_ALLOWED = Set.of(
        "skip.unparseable.ddl",
        "store.only.captured.tables.ddl",
        "store.only.captured.databases.ddl",
        "ddl.filter"
    );

    /** Prefixes Debezium strips before handing the remainder to the JDBC driver. */
    private static final List<String> JDBC_PASSTHROUGH_PREFIXES = List.of("database.", "driver.");

    /**
     * JDBC parameters that load a class, deserialize server-controlled bytes, open a URL, or read /
     * write local files. Lower-cased because Connector/J and pgjdbc both match parameter names
     * case-insensitively.
     */
    private static final Set<String> DENIED_JDBC_PARAMETERS = Set.of(
        // Debezium JdbcConfiguration, cross-connector: FQCN of a JdbcConnection.ConnectionFactory
        "connection.factory.class",
        // MySQL Connector/J - class loading
        "allowloadlocalinfile",
        "allowloadlocalinfileinpath",
        "allowurlinlocalinfile",
        "authenticationplugins",
        "authenticationopenidconnectcallbackhandler",
        "authenticationwebauthncallbackhandler",
        // removed in Connector/J 9.x, kept so an older driver on the classpath is still covered
        "autodeserialize",
        "clientinfoprovider",
        "connectionlifecycleinterceptors",
        "defaultauthenticationplugin",
        "disabledauthenticationplugins",
        "exceptioninterceptors",
        "keymanagerfactoryprovider",
        "keystoreprovider",
        "logger",
        "parseinfocachefactory",
        "profilereventhandler",
        "propertiestransform",
        "queryinfocachefactory",
        "queryinterceptors",
        "serverconfigcachefactory",
        "sslcontextprovider",
        "trustmanagerfactoryprovider",
        // MySQL Connector/J - URL open / local file read
        "clientcertificatekeystoreurl",
        "idtokenfile",
        "loggerfile",
        "ociconfigfile",
        "serverrsapublickeyfile",
        "trustcertificatekeystoreurl",
        // pgjdbc, including the CVE-2022-21724 socket and ssl factory pair
        "authenticationpluginclassname",
        "connectexecutor",
        "connectexecutorarg",
        "classloaderstrategy",
        "socketfactory",
        "socketfactoryarg",
        "sslcert",
        "sslfactory",
        "sslfactoryarg",
        "sslhostnameverifier",
        "sslkey",
        "sslpasswordcallback",
        "sslrootcert",
        "xmlfactoryfactory",
        // SQL Server
        "accesstokencallbackclass",
        "socketfactoryclass",
        "socketfactoryconstructorarg",
        "trustmanagerclass",
        // DB2 JCC - local file write
        "tracefile",
        "tracedirectory"
    );

    /**
     * Catch-all for the shape these driver parameters keep taking. An explicit list goes stale on
     * every driver bump, so any pass-through parameter naming a class or handler is refused whether
     * or not it is listed above. Compared against the parameter with dots removed, so dotted
     * class-shaped keys such as `connection.factory.class` still match. JCA algorithm names such as
     * Oracle's `ssl.keyManagerFactory.algorithm` collapse to `sslkeymanagerfactoryalgorithm` and do
     * not match.
     */
    private static final List<String> DENIED_JDBC_PARAMETER_SUFFIXES = List.of(
        "callbackclass",
        "callbackhandler",
        "class",
        "classname",
        "executor",
        "factory",
        "factoryarg",
        "factoryclass",
        "factoryconstructorarg",
        "handler",
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

        // schema.history.internal.*: deny the subtree, allow only the DDL-handling flags.
        if (normalized.startsWith(SCHEMA_HISTORY_PREFIX)) {
            if (SCHEMA_HISTORY_ALLOWED.contains(normalized.substring(SCHEMA_HISTORY_PREFIX.length()))) {
                return;
            }
            throw reject(key);
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
            "The property '" + key + "' is not allowed: it can load a class into the worker, read or " +
                "write local files, redirect the connection, or move Debezium internal storage. Remove it " +
                "from `properties`. There is no override; if a safe parameter is blocked in error, please " +
                "open an issue."
        );
    }
}
