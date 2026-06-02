package io.kestra.plugin.debezium;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;

import io.kestra.core.services.KVStoreService;
import io.kestra.core.storages.kv.KVEntry;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVStoreException;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.Slugify;
import org.h2.tools.RunScript;

public abstract class AbstractDebeziumTest {

    protected abstract String getUrl();

    protected abstract String getUsername();

    protected abstract String getPassword();

    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getUrl(), getUsername(), getPassword());
    }

    protected void executeSqlScript(String path) throws SQLException, URISyntaxException, FileNotFoundException {
        URL url = Objects.requireNonNull(AbstractDebeziumTest.class.getClassLoader().getResource(path));
        FileReader fileReader = new FileReader(new File(url.toURI()));
        RunScript.execute(getConnection(), fileReader);
    }

    protected static void cleanupFlowState(KVStoreService kvStoreService, String namespace, String flowId, String... stateNames) throws Exception {
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
            while (kvStore.delete(key)) {
                // Delete all versions to prevent fallback to stale previous entries.
            }
        }
    }
}
