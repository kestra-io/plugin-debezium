package io.kestra.plugin.debezium.mysql;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

// Include/exclude filters are rendered before they reach the connector config (#256).
// properties() builds the config before any database connection, so this needs no running MySQL.
@KestraTest
class IncludedFiltersRenderingTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void givenPebbleStrings_whenProperties_thenFiltersAreRendered() throws Exception {
        var props = properties(
            baseTask()
                .includedDatabases("{{ inputs.db }}")
                .excludedDatabases("{{ inputs.db }}_archive")
                .includedTables("{{ inputs.db }}.{{ inputs.table }}")
                .excludedTables("{{ inputs.db }}.tmp")
                .includedColumns("{{ inputs.db }}.{{ inputs.table }}.id")
                .excludedColumns("{{ inputs.db }}.{{ inputs.table }}.secret")
                .build()
        );

        assertThat(props.getProperty("database.include.list"), is("cdc_demo"));
        assertThat(props.getProperty("database.exclude.list"), is("cdc_demo_archive"));
        assertThat(props.getProperty("table.include.list"), is("cdc_demo.cdc_canary"));
        assertThat(props.getProperty("table.exclude.list"), is("cdc_demo.tmp"));
        assertThat(props.getProperty("column.include.list"), is("cdc_demo.cdc_canary.id"));
        assertThat(props.getProperty("column.exclude.list"), is("cdc_demo.cdc_canary.secret"));
    }

    @Test
    void givenPebbleList_whenProperties_thenEachEntryIsRendered() throws Exception {
        var props = properties(
            baseTask()
                .includedTables(List.of("{{ inputs.db }}.{{ inputs.table }}", "{{ inputs.db }}.orders", "plain.table"))
                .build()
        );

        assertThat(props.getProperty("table.include.list"), is("cdc_demo.cdc_canary,cdc_demo.orders,plain.table"));
    }

    private static Capture.CaptureBuilder<?, ?> baseTask() {
        return Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .serverId(Property.ofValue("123456789"))
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("63306"))
            .username(Property.ofValue("root"))
            .password(Property.ofValue("mysql_passwd"));
    }

    private Properties properties(Capture task) throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("db", "cdc_demo", "table", "cdc_canary"));
        var workingDir = runContext.workingDir().path();
        return task.properties(runContext, workingDir.resolve("offsets.dat"), workingDir.resolve("dbhistory.dat"));
    }
}
