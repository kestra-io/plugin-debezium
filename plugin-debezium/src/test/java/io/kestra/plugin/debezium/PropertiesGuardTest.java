package io.kestra.plugin.debezium;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PropertiesGuardTest {

    @ParameterizedTest
    @ValueSource(strings = {
        // Debezium / Kafka Connect options resolved to a class by reflection
        "connector.class",
        "converters",
        "offset.commit.policy",
        "post.processors",
        "post.processors.evil.type",
        "predicates",
        "predicates.evil.type",
        "sourceinfo.struct.maker",
        "topic.naming.strategy",
        "transaction.metadata.factory",
        "transforms",
        "transforms.evil.type",
        "key.converter",
        "value.converter",
        "header.converter",
        "value.converter.schemas.enable",
        "internal.key.converter",
        // storage the plugin owns
        "offset.storage",
        "offset.storage.file.filename",
        "schema.history.internal",
        "schema.history.internal.file.filename",
        "schema.history.internal.kafka.bootstrap.servers",
        "schema.history.internal.producer.sasl.jaas.config",
        "schema.history.internal.consumer.sasl.jaas.config",
        // Kafka Connect config providers: the embedded worker instantiates these classes in its constructor
        "config.providers",
        "config.providers.evil.class",
        // JDBC pass-through, both prefixes Debezium strips
        "database.allowLoadLocalInfile",
        "database.allowUrlInLocalInfile",
        "database.autoDeserialize",
        "database.queryInterceptors",
        "database.propertiesTransform",
        "database.socketFactory",
        "database.socketFactoryArg",
        "database.sslfactory",
        "database.sslfactoryarg",
        "database.authenticationPluginClassName",
        "database.socketFactoryClass",
        "driver.autoDeserialize",
        "driver.queryInterceptors",
        // class-valued Connector/J 9.6 options
        "database.queryInfoCacheFactory",
        "database.serverConfigCacheFactory",
        "database.parseInfoCacheFactory",
        "database.sslContextProvider",
        "database.keyStoreProvider",
        "database.keyManagerFactoryProvider",
        "database.trustManagerFactoryProvider",
        "database.clientInfoProvider",
        // class-valued mssql-jdbc options
        "database.accessTokenCallbackClass",
        "database.trustManagerClass",
        "database.socketFactoryConstructorArg",
        // pgjdbc
        "database.xmlFactoryFactory",
        "database.sslhostnameverifier",
        "database.sslpasswordcallback",
        // Debezium JdbcConfiguration connection factory (dotted class key, both pass-through prefixes)
        "database.connection.factory.class",
        "driver.connection.factory.class"
    })
    void deniesUnsafeKeys(String key) {
        var exception = assertThrows(IllegalArgumentException.class, () -> PropertiesGuard.ensureAllowed(key));

        assertThat(exception.getMessage(), containsString(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        // Connector/J and pgjdbc match parameter names case-insensitively, so casing must not bypass the check
        "DATABASE.AUTODESERIALIZE",
        "database.QUERYINTERCEPTORS",
        "Transforms.evil.type",
        // padding must not bypass it either
        "  transforms  "
    })
    void deniesRegardlessOfCasingAndPadding(String key) {
        assertThrows(IllegalArgumentException.class, () -> PropertiesGuard.ensureAllowed(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "snapshot.mode",
        "max.batch.size",
        "poll.interval.ms",
        "decimal.handling.mode",
        "topic.prefix",
        "name",
        "database.dbname",
        "database.sslmode",
        "database.serverTimezone",
        "database.initial.statements",
        "driver.connectTimeout",
        // JCA algorithm names, not class names - the suffix rule must not catch these
        "driver.oracle.net.ssl.keyManagerFactory.algorithm",
        "database.ssl.trustManagerFactory.algorithm",
        // schema history DDL tuning stays usable, only the backend and its Kafka client are blocked
        "schema.history.internal.skip.unparseable.ddl",
        "schema.history.internal.store.only.captured.tables.ddl"
    })
    void allowsRegularProperties(String key) {
        assertDoesNotThrow(() -> PropertiesGuard.ensureAllowed(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        // an explicit list goes stale on every driver bump, so the class-naming shape is refused too
        "database.someNewSocketFactory",
        "database.someNewProvider",
        "database.someNewInterceptors",
        "database.someNewPluginClassName",
        "driver.someNewFactory",
        // dotted class-shaped keys must not slip past the suffix check
        "database.some.new.connection.factory.class",
        "driver.custom.socket.factory"
    })
    void deniesUnlistedParametersThatNameAClass(String key) {
        assertThrows(IllegalArgumentException.class, () -> PropertiesGuard.ensureAllowed(key));
    }

    @Test
    void allowsNullKey() {
        assertDoesNotThrow(() -> PropertiesGuard.ensureAllowed(null));
    }
}
