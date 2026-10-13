package io.kestra.plugin.debezium.postgres;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.postgresql.PGConnection;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger.OffsetCommitMode;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumTest;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for debezium/dbz#1554 (Postgres COPY shares an LSN across rows).
 * This exercises the same Capture engine that Postgres Trigger starts on every poll.
 * PostgreSQL with logical replication must be available at port 65432.
 */
@KestraTest
class CopyBatchRestartRegressionTest extends AbstractDebeziumTest {
    private static final int EXPECTED_ROWS = 40127;
    private static final int MAX_POLLS = 100;
    private static final String TABLE = "copy_restart_events";
    private static final String SLOT = "kestra_copy_restart_check";
    private static final String PUBLICATION = "kestra_copy_restart_check_pub";
    private static final String OUTPUT_KEY = "postgres." + TABLE;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Override
    protected String getUrl() {
        return "jdbc:postgresql://127.0.0.1:65432/";
    }

    @Override
    protected String getUsername() {
        return TestUtils.username();
    }

    @Override
    protected String getPassword() {
        return TestUtils.password();
    }

    @ParameterizedTest(name = "{0} run {1}")
    @CsvSource({ "COPY,1", "COPY,2", "COPY,3", "INSERT,1" })
    @Timeout(600)
    @SuppressWarnings("unchecked")
    void copyEventsSurviveMultipleBatchRestarts(String loadMode, int repetition) throws Exception {
        PostgresDebeziumTestHelper.dropReplicationArtifacts(this::getConnection, SLOT, PUBLICATION);
        try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
            statement.execute("CREATE TABLE " + TABLE + " (event_id INT PRIMARY KEY, payload TEXT NOT NULL)");
        }

        Capture task = Capture.builder()
            .id("copy_restart_capture_" + IdUtils.create())
            .type(Capture.class.getName())
            .hostname(Property.ofValue(TestUtils.hostname()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .port(Property.ofValue(TestUtils.port()))
            .database(Property.ofValue("postgres"))
            .pluginName(Property.ofValue(PostgresInterface.PluginName.PGOUTPUT))
            .snapshotMode(Property.ofValue(Capture.SnapshotMode.NEVER))
            .stateName(Property.ofValue("debezium-copy-restart-" + IdUtils.create()))
            .slotName(Property.ofValue(SLOT))
            .publicationName(Property.ofValue(PUBLICATION))
            .includedTables(List.of("public." + TABLE))
            .offsetsCommitMode(Property.ofValue(OffsetCommitMode.ON_EACH_BATCH))
            .maxRecords(Property.ofValue(1000))
            .maxWait(Property.ofValue(Duration.ofSeconds(20)))
            .build();

        // Use the same task identity and KV state on every run, as Trigger does.
        RunContext context = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        PostgresDebeziumTestHelper.cleanupTaskState(context, task);

        Path evidenceDirectory = Path.of("build", "copy-regression");
        Files.createDirectories(evidenceDirectory);
        Path receivedCsv = evidenceDirectory.resolve(loadMode.toLowerCase() + "-" + repetition + ".csv");
        try (BufferedWriter evidence = Files.newBufferedWriter(receivedCsv, StandardCharsets.UTF_8)) {
            evidence.write("id,batch\n");
            // Establish the slot and publication before COPY. No-data mode must not emit the table rows.
            assertEquals(0, task.run(context).getSize());
            try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet slot = statement.executeQuery("SELECT plugin FROM pg_replication_slots WHERE slot_name = '" + SLOT + "'")
            ) {
                assertTrue(slot.next(), "The replication slot must exist before loading rows");
                assertEquals("pgoutput", slot.getString(1));
            }

            // One large COPY transaction reproduces pgoutput multi-insert WAL records.
            // Several rows share one LSN, so the connector must preserve the partial LSN offset.
            try (Connection connection = getConnection()) {
                if (loadMode.equals("INSERT")) {
                    try (Statement statement = connection.createStatement()) {
                        assertEquals(
                            EXPECTED_ROWS, statement.executeUpdate(
                                "INSERT INTO " + TABLE + " SELECT id, 'bulk-' || id FROM generate_series(1, " + EXPECTED_ROWS + ") AS id"
                            )
                        );
                    }
                } else {
                    StringBuilder csv = new StringBuilder(EXPECTED_ROWS * 16);
                    for (int id = 1; id <= EXPECTED_ROWS; id++) {
                        csv.append(id).append(",bulk-").append(id).append('\n');
                    }
                    long copied = connection.unwrap(PGConnection.class).getCopyAPI().copyIn(
                        "COPY " + TABLE + " (event_id, payload) FROM STDIN WITH (FORMAT csv)",
                        new StringReader(csv.toString())
                    );
                    assertEquals(EXPECTED_ROWS, copied);
                }
            }
            // Check the source independently. A complete received count is not enough
            // if the COPY operation did not write the intended number of rows.
            try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SELECT COUNT(*), COUNT(DISTINCT event_id) FROM " + TABLE)
            ) {
                assertTrue(rows.next());
                assertEquals(EXPECTED_ROWS, rows.getLong(1), "COPY must create the full source set");
                assertEquals(EXPECTED_ROWS, rows.getLong(2), "COPY source IDs must be unique");
                System.out.printf(
                    "CDC source: load=%s run=%d rows=%d distinct=%d debezium=%s postgres=%s%n", loadMode, repetition, rows.getLong(1), rows.getLong(2),
                    io.debezium.connector.postgresql.Module.version(), connection.getMetaData().getDatabaseProductVersion()
                );
            }

            Map<Integer, Integer> counts = new HashMap<>();
            int nonemptyPolls = 0;
            int receivedRows = 0;
            int consecutiveEmptyPolls = 0;

            for (int poll = 0; poll < MAX_POLLS && receivedRows < EXPECTED_ROWS; poll++) {
                AbstractDebeziumTask.Output output = task.run(context);
                if (output.getSize() == 0) {
                    // A poll can end during connector startup. Require sustained inactivity;
                    // the exact ID assertions below still reject an incomplete result.
                    if (++consecutiveEmptyPolls >= 3) {
                        break;
                    }
                    continue;
                }

                consecutiveEmptyPolls = 0;
                nonemptyPolls++;
                int recordsInFile = exportOutput(output, counts, evidence, nonemptyPolls);
                receivedRows += recordsInFile;
                evidence.flush();
                System.out.printf("CDC received: load=%s run=%d batch=%d rows=%d total=%d distinct=%d%n", loadMode, repetition, nonemptyPolls, recordsInFile, receivedRows, counts.size());
            }

            // A single uninterrupted engine run cannot detect the reported restart bug.
            assertTrue(nonemptyPolls > 1, "COPY was consumed in one poll; the restart path was not tested");
            assertEquals(EXPECTED_ROWS, receivedRows, "Missing or duplicated events across COPY batch boundaries");
            assertEquals(EXPECTED_ROWS, counts.size(), "Not all expected row IDs were received");
            for (int id = 1; id <= EXPECTED_ROWS; id++) {
                assertEquals(1, counts.getOrDefault(id, 0), "Row ID " + id + " must be captured exactly once");
            }
            // Export even unexpected final-restart rows, so the CSV never hides duplicates.
            int finalRows = exportOutput(task.run(context), counts, evidence, nonemptyPolls + 1);
            evidence.flush();
            assertEquals(0, finalRows, "A final restart emitted duplicate events");
        } finally {
            PostgresDebeziumTestHelper.cleanupTaskState(context, task);
            PostgresDebeziumTestHelper.dropReplicationArtifacts(this::getConnection, SLOT, PUBLICATION);
            try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + TABLE);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private int exportOutput(AbstractDebeziumTask.Output output, Map<Integer, Integer> counts, BufferedWriter evidence, int batch) throws Exception {
        if (output.getSize() == 0) {
            return 0;
        }
        var uri = output.getUris().get(OUTPUT_KEY);
        assertTrue(uri != null, "Missing data file for " + OUTPUT_KEY);
        int[] recordsInFile = { 0 };
        try (var reader = new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, uri), StandardCharsets.UTF_8))) {
            FileSerde.reader(reader, row ->
            {
                Map<String, Object> event = (Map<String, Object>) row;
                int id = ((Number) event.get("event_id")).intValue();
                counts.merge(id, 1, Integer::sum);
                try {
                    evidence.write(id + "," + batch + "\n");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                recordsInFile[0]++;
            });
        }
        assertEquals(output.getSize(), recordsInFile[0], "Record count differs from saved output file");
        return recordsInFile[0];
    }
}
