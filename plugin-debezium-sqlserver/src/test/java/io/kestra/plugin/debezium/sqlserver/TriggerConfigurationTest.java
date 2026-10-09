package io.kestra.plugin.debezium.sqlserver;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.serializers.YamlParser;

import jakarta.validation.ConstraintViolationException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class TriggerConfigurationTest {

    @ParameterizedTest
    @ValueSource(classes = {Trigger.class, RealtimeTrigger.class})
    void serverIdIsRejected(Class<?> triggerType) {
        var yaml = """
            id: sqlserver_serverid
            namespace: company.team
            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "{{ trigger }}"
            triggers:
              - id: trigger
                type: %s
                hostname: 127.0.0.1
                port: "1433"
                username: sa
                password: secret
                database: deb
                serverId: "123456789"
            """.formatted(triggerType.getName());

        var exception = assertThrows(ConstraintViolationException.class, () -> YamlParser.parse(yaml, Flow.class));
        assertThat(exception.getMessage(), containsString("Unrecognized field \"serverId\""));
    }
}
