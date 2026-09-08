package io.kestra.plugin.debezium.mongodb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MongoConnectionGuardTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "mongodb://user:pass@host:27017/?proxyHost=attacker.example.com",
        "mongodb://user:pass@host:27017/?replicaSet=rs0&proxyPort=1080",
        "mongodb://host/?tlsInsecure=true",
        "mongodb://host/?sslInsecure=true",
        "mongodb://host/?tlsAllowInvalidCertificates=true",
        "mongodb://host/?tlsAllowInvalidHostnames=true",
        "mongodb://host/?sslInvalidHostnameAllowed=true",
        "mongodb+srv://host/?PROXYHOST=evil",
        // option name casing must not bypass the check
        "mongodb://host/?TlsInsecure=true"
    })
    void deniesRedirectAndTlsDowngradeOptions(String connectionString) {
        var exception = assertThrows(IllegalArgumentException.class, () -> MongoConnectionGuard.ensureAllowed(connectionString));

        assertThat(exception.getMessage(), containsString("connectionString"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "mongodb://user:pass@host:27017/?replicaSet=rs0",
        "mongodb://user:pass@host:27017/?authSource=admin&retryWrites=true&w=majority",
        "mongodb+srv://user:pass@cluster.example.com/?tls=true",
        // no query string at all
        "mongodb://user:pass@host:27017/",
        "mongodb://host0.example.com:27017,host1.example.com:27017/"
    })
    void allowsRegularConnectionStrings(String connectionString) {
        assertDoesNotThrow(() -> MongoConnectionGuard.ensureAllowed(connectionString));
    }

    @Test
    void allowsNull() {
        assertDoesNotThrow(() -> MongoConnectionGuard.ensureAllowed(null));
    }
}
