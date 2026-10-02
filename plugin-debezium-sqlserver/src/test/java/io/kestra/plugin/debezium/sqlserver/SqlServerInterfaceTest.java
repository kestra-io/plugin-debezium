package io.kestra.plugin.debezium.sqlserver;

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@KestraTest
class SqlServerInterfaceTest {
    @Inject
    private RunContextFactory runContextFactory;

    @ParameterizedTest
    @CsvSource({
        "INITIAL, initial",
        "INITIAL_ONLY, initial_only",
        "SCHEMA_ONLY, no_data"
    })
    void mapsSnapshotModesCorrectly(SqlServerInterface.SnapshotMode snapshotMode, String expectedDebeziumMode) throws Exception {
        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("61433"))
            .username(Property.ofValue("sa"))
            .password(Property.ofValue("Sqls3rv3r_Pa55word!"))
            .database(Property.ofValue("deb"))
            .snapshotMode(Property.ofValue(snapshotMode))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Properties properties = task.properties(
            runContext,
            runContext.workingDir().path().resolve("offsets.dat"),
            runContext.workingDir().path().resolve("dbhistory.dat")
        );

        assertThat(properties.getProperty("snapshot.mode"), is(expectedDebeziumMode));

        var connectorConfig = new SqlServerConnectorConfig(Configuration.from(properties));
        assertDoesNotThrow(connectorConfig::getSnapshotMode);
    }
}
