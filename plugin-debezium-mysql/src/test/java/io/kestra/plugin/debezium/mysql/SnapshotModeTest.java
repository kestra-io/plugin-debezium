package io.kestra.plugin.debezium.mysql;

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@KestraTest
class SnapshotModeTest {
    @Inject
    private RunContextFactory runContextFactory;

    @ParameterizedTest
    @EnumSource(MysqlInterface.SnapshotMode.class)
    void everySnapshotModeIsAcceptedByDebezium(MysqlInterface.SnapshotMode snapshotMode) throws Exception {
        Properties properties = properties(snapshotMode);

        // An unknown value makes the connector fail at startup, so check it against Debezium itself.
        String value = properties.getProperty("snapshot.mode");
        assertNotNull(BinlogConnectorConfig.SnapshotMode.parse(value), "Debezium rejects snapshot.mode=" + value);
        assertNotNull(new MySqlConnectorConfig(Configuration.from(properties)).getSnapshotMode());
    }

    @Test
    void neverKeepsStreamOnlyBehavior() throws Exception {
        Properties properties = properties(MysqlInterface.SnapshotMode.NEVER);

        assertEquals("configuration_based", properties.getProperty("snapshot.mode"));
        assertEquals("false", properties.getProperty("snapshot.mode.configuration.based.snapshot.schema"));
        assertEquals("false", properties.getProperty("snapshot.mode.configuration.based.snapshot.data"));
        assertEquals("true", properties.getProperty("snapshot.mode.configuration.based.start.stream"));
    }

    @Test
    void deprecatedModesUseTheirReplacement() throws Exception {
        assertEquals("no_data", properties(MysqlInterface.SnapshotMode.SCHEMA_ONLY).getProperty("snapshot.mode"));
        assertEquals("recovery", properties(MysqlInterface.SnapshotMode.SCHEMA_ONLY_RECOVERY).getProperty("snapshot.mode"));
    }

    private Properties properties(MysqlInterface.SnapshotMode snapshotMode) throws Exception {
        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("63306"))
            .username(Property.ofValue("root"))
            .password(Property.ofValue("mysql_passwd"))
            .snapshotMode(Property.ofValue(snapshotMode))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        return task.properties(
            runContext,
            runContext.workingDir().path().resolve("offsets.dat"),
            runContext.workingDir().path().resolve("dbhistory.dat")
        );
    }
}
