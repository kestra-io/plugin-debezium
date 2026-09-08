package io.kestra.plugin.debezium.mongodb;

import java.util.Locale;
import java.util.Set;

/**
 * Rejects MongoDB connection-string URI options that redirect the connection or disable transport
 * security.
 *
 * `connectionString` is a flow-author field that reaches the driver outside the guarded `properties`
 * map, so these options would otherwise bypass PropertiesGuard entirely. Only options honored by the
 * MongoDB Java driver are relevant; file-path TLS options (`tlsCAFile` etc.) are libmongoc-only and
 * not read here.
 */
final class MongoConnectionGuard {
    // Lower-cased because MongoDB connection-string option names are case-insensitive.
    private static final Set<String> DENIED_OPTIONS = Set.of(
        // route the connection through an attacker-chosen proxy
        "proxyhost",
        "proxyport",
        // disable TLS certificate / hostname validation
        "tlsinsecure",
        "sslinsecure",
        "tlsallowinvalidhostnames",
        "sslinvalidhostnameallowed",
        "tlsallowinvalidcertificates",
        "sslallowinvalidcertificates"
    );

    private MongoConnectionGuard() {
    }

    static void ensureAllowed(String connectionString) {
        if (connectionString == null) {
            return;
        }

        int queryStart = connectionString.indexOf('?');
        if (queryStart < 0 || queryStart == connectionString.length() - 1) {
            return;
        }

        for (String pair : connectionString.substring(queryStart + 1).split("&")) {
            if (pair.isEmpty()) {
                continue;
            }

            int eq = pair.indexOf('=');
            String name = (eq < 0 ? pair : pair.substring(0, eq)).trim().toLowerCase(Locale.ROOT);

            if (DENIED_OPTIONS.contains(name)) {
                throw new IllegalArgumentException(
                    "The MongoDB connection-string option '" + name + "' is not allowed: it can redirect " +
                        "the connection or disable TLS validation. Remove it from `connectionString`."
                );
            }
        }
    }
}
