package io.kestra.plugin.debezium.db2;

import java.util.Arrays;

import io.kestra.core.services.KVStoreService;
import io.kestra.core.storages.kv.KVEntry;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVStoreException;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.Slugify;

final class Db2DebeziumTestHelper {
    private Db2DebeziumTestHelper() {
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
