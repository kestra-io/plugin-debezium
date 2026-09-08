package io.kestra.plugin.debezium.mysql;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

// End-to-end wiring: a denied `properties` key is rejected through the real
// Capture.properties() path, not only in the isolated PropertiesGuard unit test.
// The guard throws before any database connection, so this needs no running MySQL.
@KestraTest
class PropertiesGuardWiringTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void deniedPropertyIsRejectedThroughTheTaskWiring() throws Exception {
        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .serverId(Property.ofValue("123456789"))
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("63306"))
            .username(Property.ofValue("root"))
            .password(Property.ofValue("mysql_passwd"))
            .properties(Property.ofValue(Map.of("database.queryInterceptors", "com.attacker.Gadget")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Path offsetFile = runContext.workingDir().path().resolve("offsets.dat");
        Path historyFile = runContext.workingDir().path().resolve("dbhistory.dat");

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.properties(runContext, offsetFile, historyFile)
        );
        assertThat(exception.getMessage(), containsString("database.queryInterceptors"));
    }
}
