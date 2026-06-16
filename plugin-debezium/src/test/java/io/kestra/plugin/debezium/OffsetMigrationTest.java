package io.kestra.plugin.debezium;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Pure unit tests for the legacy-offset migration logic.
 * No Micronaut DI required — migration methods are static and only need a Logger.
 */
class OffsetMigrationTest {

    private static final Logger log = LoggerFactory.getLogger(OffsetMigrationTest.class);

    // ---------------------------------------------------------------------------
    // offsetKey helper
    // ---------------------------------------------------------------------------

    @Test
    void offsetKeyFormat() {
        assertThat(
            AbstractDebeziumTask.offsetKey("engine", "kestra_"),
            is("[\"engine\",{\"server\":\"kestra_\"}]")
        );
        assertThat(
            AbstractDebeziumTask.offsetKey("kestra_b64777fd", "kestra_b64777fd"),
            is("[\"kestra_b64777fd\",{\"server\":\"kestra_b64777fd\"}]")
        );
    }

    // ---------------------------------------------------------------------------
    // Offset file migration — happy path
    // ---------------------------------------------------------------------------

    @Test
    void migratesLegacyOffsetKeyToNewConnectorId(@TempDir Path tmp) throws Exception {
        var offsetFile = tmp.resolve("offsets.dat");
        var newId = AbstractDebeziumTask.connectorIdFromParts("ns", "flow", IdUtils.create(), "");
        var legacyValue = "{\"lsn\":12345,\"snapshot\":\"true\"}".getBytes(StandardCharsets.UTF_8);

        writeOffsets(offsetFile, Map.of(
            AbstractDebeziumTask.offsetKey(
                AbstractDebeziumTask.LEGACY_CONNECTOR_NAME,
                AbstractDebeziumTask.LEGACY_TOPIC_PREFIX
            ),
            legacyValue
        ));

        AbstractDebeziumTask.migrateOffsetFile(log, offsetFile, newId, newId);

        var result = readOffsets(offsetFile);
        var expectedKey = AbstractDebeziumTask.offsetKey(newId, newId).getBytes(StandardCharsets.UTF_8);

        assertThat("new key present", result.keySet().stream().anyMatch(k -> Arrays.equals(k, expectedKey)));
        assertThat("legacy key removed", result.keySet().stream().noneMatch(k -> Arrays.equals(
            k,
            AbstractDebeziumTask.offsetKey(
                AbstractDebeziumTask.LEGACY_CONNECTOR_NAME,
                AbstractDebeziumTask.LEGACY_TOPIC_PREFIX
            ).getBytes(StandardCharsets.UTF_8)
        )));

        byte[] migratedValue = result.entrySet().stream()
            .filter(e -> Arrays.equals(e.getKey(), expectedKey))
            .findFirst()
            .map(Map.Entry::getValue)
            .orElse(null);
        assertThat("value is preserved unchanged", migratedValue, is(legacyValue));
    }

    // ---------------------------------------------------------------------------
    // Offset file migration — idempotency
    // ---------------------------------------------------------------------------

    @Test
    void idempotentWhenNewKeyAlreadyPresent(@TempDir Path tmp) throws Exception {
        var offsetFile = tmp.resolve("offsets.dat");
        var newId = AbstractDebeziumTask.connectorIdFromParts("ns", "flow", IdUtils.create(), "");
        var newKey = AbstractDebeziumTask.offsetKey(newId, newId);
        var value = "{\"lsn\":99}".getBytes(StandardCharsets.UTF_8);

        // File already has the new key — as if written by a post-fix run.
        writeOffsets(offsetFile, Map.of(newKey, value));

        var sizeBefore = Files.size(offsetFile);

        AbstractDebeziumTask.migrateOffsetFile(log, offsetFile, newId, newId);

        // File must not have changed at all.
        assertThat(Files.size(offsetFile), is(sizeBefore));
        var result = readOffsets(offsetFile);
        assertThat(result.size(), is(1));
    }

    @Test
    void idempotentWhenRunTwice(@TempDir Path tmp) throws Exception {
        var offsetFile = tmp.resolve("offsets.dat");
        var newId = AbstractDebeziumTask.connectorIdFromParts("ns", "flow", IdUtils.create(), "");
        var legacyValue = "{\"lsn\":55}".getBytes(StandardCharsets.UTF_8);

        writeOffsets(offsetFile, Map.of(
            AbstractDebeziumTask.offsetKey(
                AbstractDebeziumTask.LEGACY_CONNECTOR_NAME,
                AbstractDebeziumTask.LEGACY_TOPIC_PREFIX
            ),
            legacyValue
        ));

        AbstractDebeziumTask.migrateOffsetFile(log, offsetFile, newId, newId);
        AbstractDebeziumTask.migrateOffsetFile(log, offsetFile, newId, newId); // second call must be a no-op

        var result = readOffsets(offsetFile);
        assertThat(result.size(), is(1));

        var expectedKey = AbstractDebeziumTask.offsetKey(newId, newId).getBytes(StandardCharsets.UTF_8);
        assertThat("new key still present after second call",
            result.keySet().stream().anyMatch(k -> Arrays.equals(k, expectedKey)));
    }

    // ---------------------------------------------------------------------------
    // Offset file migration — edge cases
    // ---------------------------------------------------------------------------

    @Test
    void noOpOnAbsentFile(@TempDir Path tmp) {
        var offsetFile = tmp.resolve("absent.dat");

        // Must not throw.
        AbstractDebeziumTask.migrateOffsetFile(log, offsetFile, "kestra_aabbccdd", "kestra_aabbccdd");

        assertThat(offsetFile.toFile().exists(), is(false));
    }

    @Test
    void noOpOnEmptyFile(@TempDir Path tmp) throws Exception {
        var offsetFile = tmp.resolve("empty.dat");
        Files.createFile(offsetFile);

        // Must not throw.
        AbstractDebeziumTask.migrateOffsetFile(log, offsetFile, "kestra_aabbccdd", "kestra_aabbccdd");

        assertThat(Files.size(offsetFile), is(0L));
    }

    @Test
    void noThrowOnCorruptFile(@TempDir Path tmp) throws Exception {
        var offsetFile = tmp.resolve("corrupt.dat");
        Files.write(offsetFile, new byte[]{0x00, 0x01, 0x02, (byte) 0xFF});

        // Must not throw — only log a warning.
        AbstractDebeziumTask.migrateOffsetFile(log, offsetFile, "kestra_aabbccdd", "kestra_aabbccdd");

        // File left untouched.
        assertThat(Files.readAllBytes(offsetFile), is(new byte[]{0x00, 0x01, 0x02, (byte) 0xFF}));
    }

    @Test
    void noOpWhenNoLegacyKeyPresent(@TempDir Path tmp) throws Exception {
        // A file with an unknown/different key — simulates a MongoDB-style offset
        // whose partition shape doesn't match the legacy JDBC key format.
        var offsetFile = tmp.resolve("mongo.dat");
        var mongoKey = "[\"kestra_cafe1234\",{\"rs\":\"rs0\",\"server_id\":\"kestra_cafe1234\"}]";
        var mongoValue = "{\"sec\":1700000000,\"ord\":1}".getBytes(StandardCharsets.UTF_8);

        writeOffsets(offsetFile, Map.of(mongoKey, mongoValue));
        var sizeBefore = Files.size(offsetFile);

        // The mongo-style key doesn't match the new key format, so the idempotency guard
        // doesn't fire. The legacy-key scan also finds nothing and exits cleanly.
        AbstractDebeziumTask.migrateOffsetFile(log, offsetFile, "kestra_cafe1234", "kestra_cafe1234");

        assertThat(Files.size(offsetFile), is(sizeBefore));
    }

    // ---------------------------------------------------------------------------
    // Offset file migration — user override of topic.prefix / name (Issue 1)
    // ---------------------------------------------------------------------------

    @Test
    void migratesLegacyOffsetKeyToOverriddenTopicPrefix(@TempDir Path tmp) throws Exception {
        var offsetFile = tmp.resolve("offsets.dat");
        var effectiveName = "kestra_aabbccdd";
        var overriddenPrefix = "my_custom_prefix";
        var legacyValue = "{\"lsn\":99}".getBytes(StandardCharsets.UTF_8);

        writeOffsets(offsetFile, Map.of(
            AbstractDebeziumTask.offsetKey(
                AbstractDebeziumTask.LEGACY_CONNECTOR_NAME,
                AbstractDebeziumTask.LEGACY_TOPIC_PREFIX
            ),
            legacyValue
        ));

        AbstractDebeziumTask.migrateOffsetFile(log, offsetFile, effectiveName, overriddenPrefix);

        var result = readOffsets(offsetFile);
        var expectedKey = AbstractDebeziumTask.offsetKey(effectiveName, overriddenPrefix).getBytes(StandardCharsets.UTF_8);

        assertThat("overridden key present", result.keySet().stream().anyMatch(k -> Arrays.equals(k, expectedKey)));
        assertThat("legacy key removed", result.keySet().stream().noneMatch(k -> Arrays.equals(
            k,
            AbstractDebeziumTask.offsetKey(
                AbstractDebeziumTask.LEGACY_CONNECTOR_NAME,
                AbstractDebeziumTask.LEGACY_TOPIC_PREFIX
            ).getBytes(StandardCharsets.UTF_8)
        )));
    }

    @Test
    void noOpWhenEffectiveIdentityEqualsLegacy(@TempDir Path tmp) throws Exception {
        // When user pins name=engine and topic.prefix=kestra_, effective == legacy.
        // The idempotency guard (new key == legacy key) makes this a correct no-op.
        var offsetFile = tmp.resolve("offsets.dat");
        var legacyValue = "{\"lsn\":1}".getBytes(StandardCharsets.UTF_8);

        writeOffsets(offsetFile, Map.of(
            AbstractDebeziumTask.offsetKey(
                AbstractDebeziumTask.LEGACY_CONNECTOR_NAME,
                AbstractDebeziumTask.LEGACY_TOPIC_PREFIX
            ),
            legacyValue
        ));

        // effective == legacy, so the target key is already present → idempotency guard fires.
        AbstractDebeziumTask.migrateOffsetFile(
            log, offsetFile,
            AbstractDebeziumTask.LEGACY_CONNECTOR_NAME,
            AbstractDebeziumTask.LEGACY_TOPIC_PREFIX
        );

        // File unchanged: one entry with the legacy key still intact.
        var result = readOffsets(offsetFile);
        assertThat(result.size(), is(1));
        var legacyKeyBytes = AbstractDebeziumTask.offsetKey(
            AbstractDebeziumTask.LEGACY_CONNECTOR_NAME,
            AbstractDebeziumTask.LEGACY_TOPIC_PREFIX
        ).getBytes(StandardCharsets.UTF_8);
        assertThat("legacy key still present",
            result.keySet().stream().anyMatch(k -> Arrays.equals(k, legacyKeyBytes)));
    }

    // ---------------------------------------------------------------------------
    // Schema-history file migration — happy path
    // ---------------------------------------------------------------------------

    @Test
    void migratesLegacyServerNameInHistoryFile(@TempDir Path tmp) throws Exception {
        var historyFile = tmp.resolve("dbhistory.dat");
        var newId = AbstractDebeziumTask.connectorIdFromParts("ns", "flow", IdUtils.create(), "");

        var lines = List.of(
            "{\"source\":{\"server\":\"kestra_\"},\"position\":{\"ts_sec\":1000},\"databaseName\":\"mydb\",\"ddl\":\"CREATE TABLE t1 (id INT);\"}",
            "{\"source\":{\"server\":\"kestra_\"},\"position\":{\"ts_sec\":1001},\"databaseName\":\"mydb\",\"ddl\":\"CREATE TABLE t2 (id INT);\"}"
        );
        Files.write(historyFile, lines, StandardCharsets.UTF_8);

        AbstractDebeziumTask.migrateHistoryFile(log, historyFile, newId);

        var result = Files.readAllLines(historyFile, StandardCharsets.UTF_8);
        assertThat(result.size(), is(2));
        for (var line : result) {
            assertThat("new id in line", line.contains("\"" + newId + "\""));
            assertThat("legacy token removed", !line.contains("\"kestra_\""));
        }
    }

    // ---------------------------------------------------------------------------
    // Schema-history file migration — idempotency
    // ---------------------------------------------------------------------------

    @Test
    void historyMigrationIdempotentWhenNewIdAlreadyPresent(@TempDir Path tmp) throws Exception {
        var historyFile = tmp.resolve("dbhistory.dat");
        var newId = AbstractDebeziumTask.connectorIdFromParts("ns", "flow", IdUtils.create(), "");

        var lines = List.of(
            "{\"source\":{\"server\":\"" + newId + "\"},\"ddl\":\"CREATE TABLE t1 (id INT);\"}"
        );
        Files.write(historyFile, lines, StandardCharsets.UTF_8);
        var contentBefore = Files.readString(historyFile, StandardCharsets.UTF_8);

        AbstractDebeziumTask.migrateHistoryFile(log, historyFile, newId);

        assertThat(Files.readString(historyFile, StandardCharsets.UTF_8), is(contentBefore));
    }

    @Test
    void historyMigrationNoOpOnAbsentFile(@TempDir Path tmp) {
        var historyFile = tmp.resolve("absent.dat");

        AbstractDebeziumTask.migrateHistoryFile(log, historyFile, "kestra_aabbccdd");

        assertThat(historyFile.toFile().exists(), is(false));
    }

    @Test
    void historyMigrationNoOpOnEmptyFile(@TempDir Path tmp) throws Exception {
        var historyFile = tmp.resolve("empty.dat");
        Files.createFile(historyFile);

        AbstractDebeziumTask.migrateHistoryFile(log, historyFile, "kestra_aabbccdd");

        assertThat(Files.size(historyFile), is(0L));
    }

    @Test
    void historyMigrationNoOpWhenNoLegacyToken(@TempDir Path tmp) throws Exception {
        var historyFile = tmp.resolve("dbhistory.dat");
        var line = "{\"source\":{\"server\":\"other_server\"},\"ddl\":\"CREATE TABLE t (id INT);\"}";
        Files.write(historyFile, List.of(line), StandardCharsets.UTF_8);
        var contentBefore = Files.readString(historyFile, StandardCharsets.UTF_8);

        AbstractDebeziumTask.migrateHistoryFile(log, historyFile, "kestra_aabbccdd");

        assertThat(Files.readString(historyFile, StandardCharsets.UTF_8), is(contentBefore));
    }

    // ---------------------------------------------------------------------------
    // Schema-history file migration — DDL content safety (Issue 2)
    // ---------------------------------------------------------------------------

    @Test
    void historyMigrationDoesNotCorruptDdlContainingLegacyToken(@TempDir Path tmp) throws Exception {
        // A DDL string or column default that happens to contain "kestra_" must not be touched.
        // Only source.server should be updated.
        var historyFile = tmp.resolve("dbhistory.dat");
        var newId = "kestra_aabbccdd";
        var line = "{\"source\":{\"server\":\"kestra_\"},\"position\":{\"ts_sec\":1},\"databaseName\":\"mydb\"," +
            "\"ddl\":\"CREATE TABLE kestra_events (kestra_ VARCHAR(64) DEFAULT 'kestra_' NOT NULL);\"}";
        Files.write(historyFile, List.of(line), StandardCharsets.UTF_8);

        AbstractDebeziumTask.migrateHistoryFile(log, historyFile, newId);

        var result = Files.readAllLines(historyFile, StandardCharsets.UTF_8);
        assertThat(result.size(), is(1));
        var rewritten = result.getFirst();

        // source.server must be updated.
        assertThat("source.server migrated", rewritten.contains("\"server\":\"" + newId + "\""));
        // The DDL body with "kestra_" table/column/default values must be byte-for-byte intact.
        assertThat("DDL table name preserved", rewritten.contains("kestra_events"));
        assertThat("DDL column name preserved", rewritten.contains("kestra_ VARCHAR"));
        assertThat("DDL default value preserved", rewritten.contains("'kestra_'"));
    }

    @Test
    void historyMigrationToOverriddenTopicPrefix(@TempDir Path tmp) throws Exception {
        var historyFile = tmp.resolve("dbhistory.dat");
        var overriddenPrefix = "my_custom_prefix";
        var lines = List.of(
            "{\"source\":{\"server\":\"kestra_\"},\"databaseName\":\"db\",\"ddl\":\"CREATE TABLE t (id INT);\"}"
        );
        Files.write(historyFile, lines, StandardCharsets.UTF_8);

        AbstractDebeziumTask.migrateHistoryFile(log, historyFile, overriddenPrefix);

        var result = Files.readAllLines(historyFile, StandardCharsets.UTF_8);
        assertThat(result.size(), is(1));
        assertThat("overridden prefix in source.server", result.getFirst().contains("\"server\":\"" + overriddenPrefix + "\""));
        assertThat("legacy server token gone", !result.getFirst().contains("\"server\":\"kestra_\""));
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** Writes a HashMap<byte[],byte[]> in the same binary format as FileOffsetBackingStore. */
    private static void writeOffsets(Path file, Map<String, byte[]> entries) throws IOException {
        var map = new HashMap<byte[], byte[]>();
        for (var e : entries.entrySet()) {
            map.put(e.getKey().getBytes(StandardCharsets.UTF_8), e.getValue());
        }
        try (var oos = new ObjectOutputStream(new FileOutputStream(file.toFile()))) {
            oos.writeObject(map);
        }
    }

    @SuppressWarnings("unchecked")
    private static HashMap<byte[], byte[]> readOffsets(Path file) throws IOException, ClassNotFoundException {
        try (var ois = new ObjectInputStream(new FileInputStream(file.toFile()))) {
            return (HashMap<byte[], byte[]>) ois.readObject();
        }
    }
}
