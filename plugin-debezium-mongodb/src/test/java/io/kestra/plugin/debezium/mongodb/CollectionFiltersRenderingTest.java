package io.kestra.plugin.debezium.mongodb;

import java.util.List;
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
import static org.hamcrest.Matchers.is;

// Collection filters go through the same rendering as the shared include/exclude filters (#256).
// properties() builds the config before any connection, so this needs no running MongoDB.
@KestraTest
class CollectionFiltersRenderingTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void givenPebbleExpressions_whenProperties_thenCollectionFiltersAreRendered() throws Exception {
        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .hostname(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("27017"))
            .connectionString(Property.ofValue("mongodb://127.0.0.1:27017/?replicaSet=rs0"))
            .includedDatabases("{{ inputs.db }}")
            .includedCollections(List.of("{{ inputs.db }}.orders", "{{ inputs.db }}.customers"))
            .excludedCollections("{{ inputs.db }}.tmp")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("db", "shop"));
        var workingDir = runContext.workingDir().path();
        var props = task.properties(runContext, workingDir.resolve("offsets.dat"), workingDir.resolve("dbhistory.dat"));

        assertThat(props.getProperty("database.include.list"), is("shop"));
        assertThat(props.getProperty("collection.include.list"), is("shop.orders,shop.customers"));
        assertThat(props.getProperty("collection.exclude.list"), is("shop.tmp"));
    }
}
