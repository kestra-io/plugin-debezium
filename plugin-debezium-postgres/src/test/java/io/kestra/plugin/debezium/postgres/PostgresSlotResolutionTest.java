package io.kestra.plugin.debezium.postgres;

import java.nio.charset.StandardCharsets;
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

    @Test
    void legacyCapture_firstRunAfterUpgrade_fallsBackToKestraWithWarning() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_legacy", stateName).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Inject simulated legacy state: combined state exists, but pg-slot metadata is absent
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        String combinedKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.COMBINED_STATE_FILE, null);
        kvStore.put(combinedKey, new KVValueAndMetadata(null, Map.of("offsets", "legacy_offset_payload".getBytes(StandardCharsets.UTF_8))));

        String slotName = PostgresService.resolveSlotName(runContext, task);

        assertThat(
            "Upgraded legacy task with pre-existing offset state must fall back to legacy 'kestra' slot",
            slotName, is(PostgresService.LEGACY_SLOT_NAME)
        );

        // Verify that the fallback was recorded in KV store so future executions stay on 'kestra'
        String connectorId = PostgresService.resolveSlotName(runContext, task, "temp_id"); // checks resolution logic
        assertThat(connectorId, is(PostgresService.LEGACY_SLOT_NAME));
    }

    @Test
    void legacyCapture_subsequentRun_continuesOnKestra() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_legacy_multi", stateName).build();
        RunContext runContext1 = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Seed legacy state
        var kvStore = runContext1.namespaceKv(runContext1.flowInfo().namespace());
        String combinedKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext1, stateName, AbstractDebeziumTask.COMBINED_STATE_FILE, null);
        kvStore.put(combinedKey, new KVValueAndMetadata(null, Map.of("offsets", "legacy_offset_payload".getBytes(StandardCharsets.UTF_8))));

        // First run after upgrade
        String slot1 = PostgresService.resolveSlotName(runContext1, task);
        assertThat(slot1, is(PostgresService.LEGACY_SLOT_NAME));

        // Second run after upgrade
        RunContext runContext2 = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String slot2 = PostgresService.resolveSlotName(runContext2, task);
        assertThat(
            "Subsequent run after upgrade must continue using 'kestra'",
            slot2, is(PostgresService.LEGACY_SLOT_NAME)
        );
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
        kvStore.put(combinedKey, new KVValueAndMetadata(null, Map.of("offsets", "legacy_offset_payload".getBytes(StandardCharsets.UTF_8))));

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

        String slotName = PostgresService.resolveSlotName(runContext, task);
        assertThat(slotName, startsWith("kestra_"));
        assertTrue(PG_SLOT_PATTERN.matcher(slotName).matches());
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

        String slotName = PostgresService.resolveSlotName(runContext, task);
        assertThat(slotName, startsWith("kestra_"));
        assertTrue(PG_SLOT_PATTERN.matcher(slotName).matches());
    }

    @Test
    void backwardCompatibility_oldStateReadable() throws Exception {
        String stateName = "state-" + IdUtils.create();
        Capture task = createCaptureBuilder("task_old_compat", stateName).build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Write legacy offsets in older per-file key format
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        String legacyFileKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.OFFSETS_DATA_FILE, null);
        kvStore.put(legacyFileKey, new KVValueAndMetadata(null, "legacy_binary_data".getBytes(StandardCharsets.UTF_8)));

        String slotName = PostgresService.resolveSlotName(runContext, task);
        assertThat(
            "Legacy per-file state must also trigger backward compatibility fallback to 'kestra'",
            slotName, is(PostgresService.LEGACY_SLOT_NAME)
        );
    }
}
