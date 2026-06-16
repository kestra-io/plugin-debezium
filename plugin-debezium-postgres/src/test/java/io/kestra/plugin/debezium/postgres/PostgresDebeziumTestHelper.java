package io.kestra.plugin.debezium.postgres;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.Callable;

import io.kestra.core.runners.RunContext;
import io.kestra.core.services.KVStoreService;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.kv.KVEntry;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVStoreException;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.Slugify;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import io.kestra.plugin.debezium.AbstractDebeziumTask;

final class PostgresDebeziumTestHelper {
    private PostgresDebeziumTestHelper() {
    }

    static void dropReplicationArtifacts(Callable<java.sql.Connection> connectionSupplier, String slotName, String publicationName) throws Exception {
        String sanitizedSlot = slotName.replace("'", "''");
        String sanitizedPublication = publicationName.replace("\"", "\"\"");

        try (var connection = connectionSupplier.call(); var statement = connection.createStatement()) {
            statement.execute(String.format("SELECT pg_drop_replication_slot('%s')", sanitizedSlot));
        } catch (SQLException e) {
            if (!"42704".equals(e.getSQLState())) { // undefined_object
                throw e;
            }
        }

        try (var connection = connectionSupplier.call(); var statement = connection.createStatement()) {
            statement.execute(String.format("DROP PUBLICATION IF EXISTS \"%s\"", sanitizedPublication));
        } catch (SQLException e) {
            if (!"42704".equals(e.getSQLState())) { // undefined_object
                throw e;
            }
        }
    }

    static void cleanupTaskState(RunContext runContext, AbstractDebeziumTask task) throws Exception {
        var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        var taskRunValue = runContext.storage().getTaskStorageContext()
            .map(StorageContext.Task::getTaskRunValue)
            .orElse(null);
        var stateName = runContext.render(task.getStateName()).as(String.class).orElse("debezium-state");

        // Combined atomic key (current format).
        deleteAllVersions(kvStore, AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.COMBINED_STATE_FILE, taskRunValue));

        // Legacy per-file keys (older format, kept for backward compatibility).
        deleteAllVersions(kvStore, AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.OFFSETS_DATA_FILE, taskRunValue));
        deleteAllVersions(kvStore, AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.DBHISTORY_DATA_FILE, taskRunValue));
    }

    static void cleanupFlowState(KVStoreService kvStoreService, String namespace, String flowId, String... stateNames) throws Exception {
        KVStore kvStore;
        try {
            kvStore = kvStoreService.get(TenantService.MAIN_TENANT, namespace, namespace);
        } catch (KVStoreException e) {
            return;
        }

        var flowPrefix = Slugify.of(flowId) + "_states_";
        var keysToDelete = kvStore.listAll().stream()
            .map(KVEntry::key)
            .distinct()
            .filter(key -> Arrays.stream(stateNames).anyMatch(stateName -> key.startsWith(flowPrefix + stateName)))
            .toList();

        for (var key : keysToDelete) {
            deleteAllVersions(kvStore, key);
        }
    }

    private static void deleteAllVersions(KVStore kvStore, String key) throws Exception {
        while (kvStore.delete(key)) {
            // Delete all versions to prevent fallback to stale previous entries.
        }
    }
}
