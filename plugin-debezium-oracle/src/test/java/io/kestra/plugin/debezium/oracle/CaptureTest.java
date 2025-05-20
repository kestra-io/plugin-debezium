package io.kestra.plugin.debezium.oracle;

import com.google.common.base.Charsets;
import io.kestra.core.models.property.Property;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.apache.commons.io.IOUtils;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class CaptureTest extends AbstractDebeziumTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Override
    protected String getUrl() {
        return "jdbc:oracle:thin:@localhost:1521:XE";
    }

    @Override
    protected String getUsername() {
        return "kestra";
    }

    @Override
    protected String getPassword() {
        return "passwd";
    }

    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        try {
            RunScript.execute(getConnection(), new StringReader("DROP TABLE events;"));
        } catch (Exception ignored) {

        }

        executeSqlScript("scripts/oracle.sql");
    }

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        // init database
        initDatabase();

        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .snapshotMode(Property.of(OracleInterface.SnapshotMode.INITIAL_ONLY))
            .hostname(Property.of("127.0.0.1"))
            .port(Property.of("1521"))
            .username(Property.of("c##dbzuser"))
            .sid(Property.of("XE"))
            .password(Property.of("dbz"))
            .stateName(Property.of(UUID.randomUUID().toString()))
            .maxRecords(Property.of(5))
            .includedTables("KESTRA.EVENTS")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        AbstractDebeziumTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getSize(), is(5));

        List<Map<String, Object>> events = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, runOutput.getUris().get("XE.EVENTS")))), r -> events.add((Map<String, Object>) r));

        IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, runOutput.getUris().get("XE.EVENTS")), Charsets.UTF_8);
        assertThat(events.size(), is(5));
        assertTrue(events.stream().anyMatch(map -> map.get("EVENT_TITLE").equals("Machine Head")));
        assertTrue(events.stream().anyMatch(map -> map.get("EVENT_TITLE").equals("Dropkick Murphys")));
        assertTrue(events.stream().anyMatch(map -> map.get("EVENT_TITLE").equals("Pink Floyd")));
        assertTrue(events.stream().anyMatch(map -> map.get("EVENT_TITLE").equals("TV show")));
        assertTrue(events.stream().anyMatch(map -> map.get("EVENT_TITLE").equals("Nothing")));
//
//        // rerun state will prevent new records
//        runOutput = task.run(runContext);
//        assertThat(runOutput.getSize(), is(0));
    }
}
