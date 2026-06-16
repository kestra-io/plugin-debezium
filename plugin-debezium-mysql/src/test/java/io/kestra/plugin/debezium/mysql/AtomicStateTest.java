package io.kestra.plugin.debezium.mysql;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import io.kestra.plugin.debezium.AbstractDebeziumTask;

import jakarta.inject.Inject;
import lombok.experimental.SuperBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests the atomic KV state persistence introduced in fix/175.
 * Uses MySQL's Capture (needsDatabaseHistory=true) so both offset and history code paths are exercised.
 */
@KestraTest
class AtomicStateTest {

    @Inject
    private RunContextFactory runContextFactory;

    // Minimal no-history task (like MongoDB) to test the offsets-only path.
    @SuperBuilder
    private static class OffsetOnlyTask extends AbstractDebeziumTask {
        @Override
        protected boolean needDatabaseHistory() {
            return false;
        }

        @Override
        protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) {
            return new Properties();
        }
    }

    // Full task with history (like MySQL) to test both streams together.
    @SuperBuilder
    private static class HistoryTask extends AbstractDebeziumTask {
        @Override
        protected boolean needDatabaseHistory() {
            return true;
        }

        @Override
        protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) {
            return new Properties();
        }
    }

    @Test
    void atomicSaveWritesSingleKey(@TempDir Path tempDir) throws Exception {
        var stateName = "debezium-state-" + IdUtils.create();
        var task = HistoryTask.builder()
            .id(IdUtils.create())
            .type(HistoryTask.class.getName())
            .stateName(Property.ofValue(stateName))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var offsetFile = tempDir.resolve("offsets.dat");
        var historyFile = tempDir.resolve("dbhistory.dat");
        Files.write(offsetFile, "offsets-content".getBytes());
        Files.write(historyFile, "history-content".getBytes());

        var combinedKey = task.saveStateAtomically(runContext, offsetFile, historyFile);

        // Key must embed the state name and the combined filename, not the legacy filenames.
        assertThat(combinedKey, containsString(stateName));
        assertThat(combinedKey, containsString(AbstractDebeziumTask.COMBINED_STATE_FILE));
        assertThat(combinedKey, not(containsString(AbstractDebeziumTask.OFFSETS_DATA_FILE)));
        assertThat(combinedKey, not(containsString(AbstractDebeziumTask.DBHISTORY_DATA_FILE)));

        // The stored value must be a map with both entries.
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        var stored = kvStore.getValue(combinedKey);
        assertThat(stored.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        var stateMap = (Map<String, Object>) stored.get().value();
        assertThat(new String((byte[]) stateMap.get(AbstractDebeziumTask.STATE_KEY_OFFSETS)), is("offsets-content"));
        assertThat(new String((byte[]) stateMap.get(AbstractDebeziumTask.STATE_KEY_HISTORY)), is("history-content"));
    }

    @Test
    void atomicSaveAndRestore(@TempDir Path tempDir) throws Exception {
        var stateName = "debezium-state-" + IdUtils.create();
        var task = HistoryTask.builder()
            .id(IdUtils.create())
            .type(HistoryTask.class.getName())
            .stateName(Property.ofValue(stateName))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var offsetFile = tempDir.resolve("offsets.dat");
        var historyFile = tempDir.resolve("dbhistory.dat");
        Files.write(offsetFile, "offsets-content".getBytes());
        Files.write(historyFile, "history-content".getBytes());

        task.saveStateAtomically(runContext, offsetFile, historyFile);

        var restoredOffsets = tempDir.resolve("restored-offsets.dat");
        var restoredHistory = tempDir.resolve("restored-history.dat");
        task.restoreState(runContext, restoredOffsets, restoredHistory);

        assertThat(new String(Files.readAllBytes(restoredOffsets)), is("offsets-content"));
        assertThat(new String(Files.readAllBytes(restoredHistory)), is("history-content"));
    }

    @Test
    void atomicSaveWithoutHistoryFile(@TempDir Path tempDir) throws Exception {
        var stateName = "debezium-state-" + IdUtils.create();
        var task = OffsetOnlyTask.builder()
            .id(IdUtils.create())
            .type(OffsetOnlyTask.class.getName())
            .stateName(Property.ofValue(stateName))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var offsetFile = tempDir.resolve("offsets.dat");
        var historyFile = tempDir.resolve("dbhistory.dat"); // does not exist
        Files.write(offsetFile, "offsets-only".getBytes());

        task.saveStateAtomically(runContext, offsetFile, historyFile);

        var restoredOffsets = tempDir.resolve("restored-offsets.dat");
        var restoredHistory = tempDir.resolve("restored-history.dat");
        task.restoreState(runContext, restoredOffsets, restoredHistory);

        assertThat(new String(Files.readAllBytes(restoredOffsets)), is("offsets-only"));
        // history was not saved, so the restore file should not have been created
        assertThat(restoredHistory.toFile().exists(), is(false));
    }

    @Test
    void restoreFallsBackToLegacyKeys(@TempDir Path tempDir) throws Exception {
        var stateName = "debezium-state-" + IdUtils.create();
        var task = HistoryTask.builder()
            .id(IdUtils.create())
            .type(HistoryTask.class.getName())
            .stateName(Property.ofValue(stateName))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Write legacy keys directly, simulating state written by an older plugin version.
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        var legacyOffsetKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.OFFSETS_DATA_FILE, null);
        var legacyHistoryKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.DBHISTORY_DATA_FILE, null);
        kvStore.put(legacyOffsetKey, new KVValueAndMetadata(null, "legacy-offsets".getBytes()));
        kvStore.put(legacyHistoryKey, new KVValueAndMetadata(null, "legacy-history".getBytes()));

        var restoredOffsets = tempDir.resolve("offsets.dat");
        var restoredHistory = tempDir.resolve("dbhistory.dat");
        task.restoreState(runContext, restoredOffsets, restoredHistory);

        assertThat(new String(Files.readAllBytes(restoredOffsets)), is("legacy-offsets"));
        assertThat(new String(Files.readAllBytes(restoredHistory)), is("legacy-history"));
    }

    @Test
    void historyTaskDoesNotWriteIncompleteEntryWhenHistoryFileMissing(@TempDir Path tempDir) throws Exception {
        var stateName = "debezium-state-" + IdUtils.create();
        var task = HistoryTask.builder()
            .id(IdUtils.create())
            .type(HistoryTask.class.getName())
            .stateName(Property.ofValue(stateName))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var offsetFile = tempDir.resolve("offsets.dat");
        var historyFile = tempDir.resolve("dbhistory.dat"); // deliberately absent
        Files.write(offsetFile, "offsets-content".getBytes());

        var result = task.saveStateAtomically(runContext, offsetFile, historyFile);

        // Must return null — no write should have occurred.
        assertThat(result, is(nullValue()));

        // The combined key must not exist in the KV store.
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        var combinedKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(
            runContext, stateName, AbstractDebeziumTask.COMBINED_STATE_FILE, null);
        var stored = kvStore.getValue(combinedKey);
        assertThat("combined key must not have been written when history file is absent",
            stored.isPresent(), is(false));
    }

    @Test
    void combinedKeyTakesPrecedenceOverLegacy(@TempDir Path tempDir) throws Exception {
        var stateName = "debezium-state-" + IdUtils.create();
        var task = HistoryTask.builder()
            .id(IdUtils.create())
            .type(HistoryTask.class.getName())
            .stateName(Property.ofValue(stateName))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Write stale legacy keys that should be ignored when the combined key exists.
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        var legacyOffsetKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.OFFSETS_DATA_FILE, null);
        var legacyHistoryKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.DBHISTORY_DATA_FILE, null);
        kvStore.put(legacyOffsetKey, new KVValueAndMetadata(null, "stale-offsets".getBytes()));
        kvStore.put(legacyHistoryKey, new KVValueAndMetadata(null, "stale-history".getBytes()));

        var offsetFile = tempDir.resolve("offsets.dat");
        var historyFile = tempDir.resolve("dbhistory.dat");
        Files.write(offsetFile, "fresh-offsets".getBytes());
        Files.write(historyFile, "fresh-history".getBytes());
        task.saveStateAtomically(runContext, offsetFile, historyFile);

        var restoredOffsets = tempDir.resolve("restored-offsets.dat");
        var restoredHistory = tempDir.resolve("restored-history.dat");
        task.restoreState(runContext, restoredOffsets, restoredHistory);

        assertThat(new String(Files.readAllBytes(restoredOffsets)), is("fresh-offsets"));
        assertThat(new String(Files.readAllBytes(restoredHistory)), is("fresh-history"));
    }
}
