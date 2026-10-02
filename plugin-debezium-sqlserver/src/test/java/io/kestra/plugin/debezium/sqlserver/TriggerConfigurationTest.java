package io.kestra.plugin.debezium.sqlserver;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;
import jakarta.validation.Validator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class TriggerConfigurationTest {
    @Inject
    private Validator validator;

    @Test
    void triggersDoNotDeclareServerId() {
        assertFalse(declaresField(Trigger.class, "serverId"));
        assertFalse(declaresField(RealtimeTrigger.class, "serverId"));
    }

    @Test
    void triggerWithoutServerIdPassesBeanValidation() {
        Trigger trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("1433"))
            .username(Property.ofValue("sa"))
            .password(Property.ofValue("secret"))
            .database(Property.ofValue("deb"))
            .snapshotMode(Property.ofValue(SqlServerInterface.SnapshotMode.INITIAL))
            .maxRecords(Property.ofValue(100))
            .includedTables(List.of("dbo.events"))
            .properties(Property.ofValue(Map.of("database.encrypt", "false")))
            .build();

        assertTrue(validator.validate(trigger).isEmpty());
    }

    private static boolean declaresField(Class<?> type, String name) {
        return Arrays.stream(type.getDeclaredFields()).anyMatch(field -> field.getName().equals(name));
    }
}
