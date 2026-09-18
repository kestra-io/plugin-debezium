package io.kestra.plugin.debezium.mysql;

import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import jakarta.inject.Inject;
import jakarta.validation.Validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class ServerIdTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private Validator validator;

    @ParameterizedTest
    @MethodSource("serverIds")
    void serverIdIsOptionalAndSupportsExpressions(Property<String> serverId) throws Exception {
        Capture task = Capture.builder()
            .id("capture")
            .type(Capture.class.getName())
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("63306"))
            .username(Property.ofValue("root"))
            .password(Property.ofValue("mysql_passwd"))
            .serverId(serverId)
            .build();

        assertTrue(validator.validateProperty(task, "serverId").isEmpty());

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("serverId", "123456789"));
        var properties = task.properties(
            runContext,
            runContext.workingDir().path().resolve("offsets.dat"),
            runContext.workingDir().path().resolve("dbhistory.dat")
        );
        var connectorConfig = new MySqlConnectorConfig(Configuration.from(properties));

        if (serverId == null) {
            assertTrue(connectorConfig.getServerId() >= 5400 && connectorConfig.getServerId() <= 6400);
        } else {
            assertEquals("123456789", properties.getProperty("database.server.id"));
            assertEquals(123456789L, connectorConfig.getServerId());
        }
    }

    private static Stream<Property<String>> serverIds() {
        return Stream.of(
            null,
            Property.ofValue("123456789"),
            Property.ofExpression("{{ inputs.serverId }}")
        );
    }
}
