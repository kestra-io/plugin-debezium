package io.kestra.plugin.debezium.postgres;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class PostgresSlotResolutionTest {

    private static final Pattern PG_SLOT_PATTERN = Pattern.compile("^[a-z0-9_]{1,63}$");

    @Inject
    private RunContextFactory runContextFactory;

    private Capture.CaptureBuilder<?, ?> createCaptureBuilder(String id, String stateName) {
        return Capture.builder()
            .id(id)
            .type(Capture.class.getName())
            .hostname(Property.ofValue("localhost"))
            .port(Property.ofValue("5432"))
            .database(Property.ofValue("postgres"))
            .stateName(Property.ofValue(stateName));
    }

    private static final String LEGACY_CONNECTOR_NAME = "engine";
    private static final String LEGACY_TOPIC_PREFIX = "kestra_";

    private static String offsetKey(String connectorName, String topicPrefix) {
        return "[\"" + connectorName + "\",{\"server\":\"" + topicPrefix + "\"}]";
    }

    private static byte[] serializeOffsets(Map<String, byte[]> entries) throws IOException {
        var map = new HashMap<byte[], byte[]>();
        for (var e : entries.entrySet()) {
            map.put(e.getKey().getBytes(StandardCharsets.UTF_8), e.getValue());
        }
        var baos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(map);
        }
        return baos.toByteArray();
    }

    @Test
    void freshCapture_firstRun_derivesSlotAndPersistsMetadata() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_fresh", stateName).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        String slotName = PostgresService.resolveSlotName(runContext, task);

        assertThat(slotName, startsWith("kestra_"));
        assertThat(slotName.length(), is(15));
        assertTrue(PG_SLOT_PATTERN.matcher(slotName).matches(), "Slot name must comply with PostgreSQL naming rules");

        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        String slotMetaKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, "pg-slot-" + slotName + ".txt", null);

        var stored = kvStore.getValue(slotMetaKey);
        assertTrue(stored.isPresent(), "Slot metadata must be recorded in KV store upon first run");
        String recordedVal = stored.get().value() instanceof byte[] bytes
            ? new String(bytes, StandardCharsets.UTF_8)
            : stored.get().value().toString();
        assertThat(recordedVal, is(slotName));
    }

    @Test
    void sameCapture_secondRun_usesPersistedDerivedSlot() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_stable", stateName).build();
        RunContext runContext1 = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // First run: derives slot and records metadata in KV store
        String slot1 = PostgresService.resolveSlotName(runContext1, task);

        // Simulate subsequent run in a new RunContext with existing KV store state
        RunContext runContext2 = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String slot2 = PostgresService.resolveSlotName(runContext2, task);

        assertThat(
            "Subsequent run must return the exact same derived slot and NEVER revert to 'kestra'",
            slot2, is(slot1)
        );
        assertThat(slot2, startsWith("kestra_"));
    }

    /**
     * Regression test for Maintainer Review Comment 1 / Issue #235 Scenario A:
     * Task A runs, resolves its slot, and writes state under the shared flow-level KV key.
     * Task B resolves afterwards.
     * Task B must derive its own distinct slot and MUST NOT fall back to 'kestra'.
     */
    @Test
    void taskA_writesFlowLevelState_taskB_stillDerivesItsOwnSlot() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture taskA = createCaptureBuilder("task_a", stateName).build();
        Capture taskB = createCaptureBuilder("task_b", stateName).build();

        RunContext runContextA = TestsUtils.mockRunContext(runContextFactory, taskA, Map.of());
        RunContext runContextB = TestsUtils.mockRunContext(runContextFactory, taskB, Map.of());

        // 1. Task A resolves to a derived slot
        String slotA = PostgresService.resolveSlotName(runContextA, taskA);
        assertThat(slotA, startsWith("kestra_"));

        // 2. Task A writes state under the shared flow-level key (computeKvStoreKey does not include taskId)
        var kvStore = runContextA.namespaceKv(runContextA.flowInfo().namespace());
        String combinedKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContextA, stateName, AbstractDebeziumTask.COMBINED_STATE_FILE, null);
        byte[] taskAOffsets = serializeOffsets(
            Map.of(
                offsetKey(slotA, slotA),
                "{\"lsn\":12345}".getBytes(StandardCharsets.UTF_8)
            )
        );
        kvStore.put(combinedKey, new KVValueAndMetadata(null, Map.of(AbstractDebeziumTask.STATE_KEY_OFFSETS, taskAOffsets)));

        // 3. Task B resolves afterward
        String slotB = PostgresService.resolveSlotName(runContextB, taskB);

        // 4. Task B must derive its own slot and NOT fall back to 'kestra'
        assertThat("Task B must NOT fall back to legacy 'kestra' slot", slotB, not(equalTo(PostgresService.LEGACY_SLOT_NAME)));
        assertThat("Task B must NOT collide with Task A's slot", slotB, not(equalTo(slotA)));
        assertThat(slotB, startsWith("kestra_"));
        assertTrue(PG_SLOT_PATTERN.matcher(slotB).matches());
    }

    @Test
    void legacyCapture_withEngineOffsets_fallsBackToKestraWithWarning() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_legacy", stateName).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Pre-1.4.3 legacy state: offsets contain "engine" / "kestra_" key
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        String combinedKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.COMBINED_STATE_FILE, null);
        byte[] legacyOffsets = serializeOffsets(
            Map.of(
                offsetKey(LEGACY_CONNECTOR_NAME, LEGACY_TOPIC_PREFIX),
                "{\"lsn\":12345}".getBytes(StandardCharsets.UTF_8)
            )
        );
        kvStore.put(combinedKey, new KVValueAndMetadata(null, Map.of(AbstractDebeziumTask.STATE_KEY_OFFSETS, legacyOffsets)));

        String slotName = PostgresService.resolveSlotName(runContext, task);

        assertThat(
            "Upgraded pre-1.4.3 legacy task with 'engine' offsets must fall back to legacy 'kestra' slot",
            slotName, is(PostgresService.LEGACY_SLOT_NAME)
        );

        // Verify that subsequent run continues using 'kestra' via the recorded slot metadata
        RunContext runContext2 = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String slot2 = PostgresService.resolveSlotName(runContext2, task);
        assertThat(
            "Subsequent run after upgrade must continue using 'kestra'",
            slot2, is(PostgresService.LEGACY_SLOT_NAME)
        );
    }

    @Test
    void upgradedCapture_withExistingConnectorOffsets_fallsBackToKestra() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_upgraded_143", stateName).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // 1.4.3 state: connector ID was derived, but slotName defaulted to "kestra" and no slot metadata was recorded
        String connectorId = task.deriveConnectorId(runContext);
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        String combinedKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.COMBINED_STATE_FILE, null);
        byte[] upgradedOffsets = serializeOffsets(
            Map.of(
                offsetKey(connectorId, connectorId),
                "{\"lsn\":9999}".getBytes(StandardCharsets.UTF_8)
            )
        );
        kvStore.put(combinedKey, new KVValueAndMetadata(null, Map.of(AbstractDebeziumTask.STATE_KEY_OFFSETS, upgradedOffsets)));

        String slotName = PostgresService.resolveSlotName(runContext, task);

        assertThat(
            "1.4.3 task with existing connector offsets but no slot metadata must fall back to 'kestra'",
            slotName, is(PostgresService.LEGACY_SLOT_NAME)
        );

        // Verify subsequent run continues using 'kestra'
        RunContext runContext2 = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String slot2 = PostgresService.resolveSlotName(runContext2, task);
        assertThat(slot2, is(PostgresService.LEGACY_SLOT_NAME));
    }

    @Test
    void explicitSlot_alwaysWins() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_explicit", stateName)
            .slotName(Property.ofValue("my_custom_slot"))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Even if legacy state existed, explicit slotName takes complete precedence
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        String combinedKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.COMBINED_STATE_FILE, null);
        byte[] legacyOffsets = serializeOffsets(
            Map.of(
                offsetKey(LEGACY_CONNECTOR_NAME, LEGACY_TOPIC_PREFIX),
                "{\"lsn\":1}".getBytes(StandardCharsets.UTF_8)
            )
        );
        kvStore.put(combinedKey, new KVValueAndMetadata(null, Map.of(AbstractDebeziumTask.STATE_KEY_OFFSETS, legacyOffsets)));

        String slotName = PostgresService.resolveSlotName(runContext, task);
        assertThat(slotName, is("my_custom_slot"));
    }

    @Test
    void explicitExpressionSlot_isRendered() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_expr", stateName)
            .slotName(Property.ofExpression("{{ inputs.slot }}"))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("slot", "rendered_slot"));

        String slotName = PostgresService.resolveSlotName(runContext, task);
        assertThat(slotName, is("rendered_slot"));
    }

    @Test
    void distinctTasks_deriveDistinctSlots() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture taskA = createCaptureBuilder("task_a", stateName).build();
        Capture taskB = createCaptureBuilder("task_b", stateName).build();

        RunContext runContextA = TestsUtils.mockRunContext(runContextFactory, taskA, Map.of());
        RunContext runContextB = TestsUtils.mockRunContext(runContextFactory, taskB, Map.of());

        String slotA = PostgresService.resolveSlotName(runContextA, taskA);
        String slotB = PostgresService.resolveSlotName(runContextB, taskB);

        assertThat(
            "Tasks in the same flow must receive distinct replication slots (preventing Issue #235)",
            slotA, not(equalTo(slotB))
        );
    }

    @Test
    void trigger_delegatesToCaptureWithIdenticalResolution() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Trigger trigger = Trigger.builder()
            .id("trigger_cdc")
            .type(Trigger.class.getName())
            .stateName(Property.ofValue(stateName))
            .build();

        Capture task = createCaptureBuilder(trigger.getId(), stateName)
            .slotName(trigger.getSlotName())
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        String triggerSlot = PostgresService.resolveSlotName(runContext, trigger);
        String taskSlot = PostgresService.resolveSlotName(runContext, task);

        assertThat(triggerSlot, equalTo(taskSlot));
        assertThat(triggerSlot, startsWith("kestra_"));
        assertTrue(PG_SLOT_PATTERN.matcher(triggerSlot).matches());
    }

    @Test
    void realtimeTrigger_delegatesToCaptureWithIdenticalResolution() throws Exception {
        String stateName = "state-" + IdUtils.create();
        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("realtime_cdc")
            .type(RealtimeTrigger.class.getName())
            .stateName(Property.ofValue(stateName))
            .build();

        Capture task = createCaptureBuilder(trigger.getId(), stateName)
            .slotName(trigger.getSlotName())
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        String triggerSlot = PostgresService.resolveSlotName(runContext, trigger);
        String taskSlot = PostgresService.resolveSlotName(runContext, task);

        assertThat(triggerSlot, equalTo(taskSlot));
        assertThat(triggerSlot, startsWith("kestra_"));
        assertTrue(PG_SLOT_PATTERN.matcher(triggerSlot).matches());
    }

    @Test
    void explicitBlankSlot_throwsIllegalArgumentException() {
        Capture task = createCaptureBuilder("blank_slot_task", "state-" + IdUtils.create())
            .slotName(Property.ofValue("   "))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> PostgresService.resolveSlotName(runContext, task)
        );
        assertThat(thrown.getMessage(), containsString("The 'slotName' property was specified but evaluated to an empty value"));
    }

    @Test
    void backwardCompatibility_oldStateReadable() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_old_compat", stateName).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Write legacy offsets in older per-file key format
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        String legacyFileKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.OFFSETS_DATA_FILE, null);
        byte[] legacyOffsets = serializeOffsets(
            Map.of(
                offsetKey(LEGACY_CONNECTOR_NAME, LEGACY_TOPIC_PREFIX),
                "{\"lsn\":42}".getBytes(StandardCharsets.UTF_8)
            )
        );
        kvStore.put(legacyFileKey, new KVValueAndMetadata(null, legacyOffsets));

        String slotName = PostgresService.resolveSlotName(runContext, task);
        assertThat(
            "Legacy per-file state must also trigger backward compatibility fallback to 'kestra'",
            slotName, is(PostgresService.LEGACY_SLOT_NAME)
        );
    }

    @Test
    void restoredOffsetFileOnDisk_isDetected() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_disk_compat", stateName).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Place legacy offset file directly in the runContext working directory (as if restored by restoreState)
        Path offsetFile = runContext.workingDir().path().resolve(AbstractDebeziumTask.OFFSETS_DATA_FILE);
        byte[] legacyOffsets = serializeOffsets(
            Map.of(
                offsetKey(LEGACY_CONNECTOR_NAME, LEGACY_TOPIC_PREFIX),
                "{\"lsn\":888}".getBytes(StandardCharsets.UTF_8)
            )
        );
        Files.write(offsetFile, legacyOffsets);

        String slotName = PostgresService.resolveSlotName(runContext, task);
        assertThat(
            "Legacy offsets restored on disk must trigger fallback to 'kestra'",
            slotName, is(PostgresService.LEGACY_SLOT_NAME)
        );
    }

    @Test
    void kvStorePutFailure_propagatesException() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_kv_put_fail", stateName).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var originalKvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        var failingKvStore = (io.kestra.core.storages.kv.KVStore) java.lang.reflect.Proxy.newProxyInstance(
            io.kestra.core.storages.kv.KVStore.class.getClassLoader(),
            new Class<?>[] { io.kestra.core.storages.kv.KVStore.class },
            (proxy, method, args) ->
            {
                if ("put".equals(method.getName())) {
                    throw new IOException("Simulated KV write failure");
                }
                try {
                    return method.invoke(originalKvStore, args);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        );

        var failingKvService = new io.kestra.core.services.KVStoreService() {
            @Override
            public io.kestra.core.storages.kv.KVStore get(String tenantId, String namespace, String stateNamespace) {
                return failingKvStore;
            }
        };

        var field = runContext.getClass().getDeclaredField("kvStoreService");
        field.setAccessible(true);
        field.set(runContext, failingKvService);

        IOException thrown = assertThrows(IOException.class, () -> PostgresService.resolveSlotName(runContext, task));
        assertThat(thrown.getMessage(), containsString("Simulated KV write failure"));
    }

    @Test
    void kvStoreReadFailure_propagatesException() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_kv_read_fail", stateName).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var originalKvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        var failingKvStore = (io.kestra.core.storages.kv.KVStore) java.lang.reflect.Proxy.newProxyInstance(
            io.kestra.core.storages.kv.KVStore.class.getClassLoader(),
            new Class<?>[] { io.kestra.core.storages.kv.KVStore.class },
            (proxy, method, args) ->
            {
                if ("getValue".equals(method.getName())) {
                    throw new IOException("Simulated KV read failure");
                }
                try {
                    return method.invoke(originalKvStore, args);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        );

        var failingKvService = new io.kestra.core.services.KVStoreService() {
            @Override
            public io.kestra.core.storages.kv.KVStore get(String tenantId, String namespace, String stateNamespace) {
                return failingKvStore;
            }
        };

        var field = runContext.getClass().getDeclaredField("kvStoreService");
        field.setAccessible(true);
        field.set(runContext, failingKvService);

        IOException thrown = assertThrows(IOException.class, () -> PostgresService.resolveSlotName(runContext, task));
        assertThat(thrown.getMessage(), containsString("Simulated KV read failure"));
    }

    @Test
    void corruptOffsetData_throwsIOException() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_corrupt", stateName).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        String combinedKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.COMBINED_STATE_FILE, null);
        kvStore.put(combinedKey, new KVValueAndMetadata(null, Map.of(AbstractDebeziumTask.STATE_KEY_OFFSETS, "corrupted_non_serialized_bytes".getBytes(StandardCharsets.UTF_8))));

        assertThrows(IOException.class, () -> PostgresService.resolveSlotName(runContext, task));
    }

}
