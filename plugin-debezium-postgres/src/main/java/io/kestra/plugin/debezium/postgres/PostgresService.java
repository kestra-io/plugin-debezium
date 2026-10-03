package io.kestra.plugin.debezium.postgres;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.util.Locale;
import java.util.Properties;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.Hashing;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import io.kestra.plugin.debezium.AbstractDebeziumTask;

public abstract class PostgresService {
    public static final String LEGACY_SLOT_NAME = "kestra";

    public static void handleProperties(Properties properties, RunContext runContext, PostgresInterface postgres)
        throws IllegalVariableEvaluationException, IOException, OperatorCreationException, PKCSException {
        handleProperties(properties, runContext, postgres, null);
    }

    public static void handleProperties(Properties properties, RunContext runContext, PostgresInterface postgres, String connectorId)
        throws IllegalVariableEvaluationException, IOException, OperatorCreationException, PKCSException {
        properties.put("database.dbname", runContext.render(postgres.getDatabase()).as(String.class).orElseThrow());
        properties.put("plugin.name", runContext.render(postgres.getPluginName()).as(PostgresInterface.PluginName.class).orElseThrow().name().toLowerCase(Locale.ROOT));
        properties.put("slot.name", resolveSlotName(runContext, postgres, connectorId));

        PostgresInterface.SnapshotMode rSnapshotMode = runContext.render(postgres.getSnapshotMode()).as(PostgresInterface.SnapshotMode.class).orElseThrow();

        String debeziumSnapshotMode = switch (rSnapshotMode) {
            case NEVER -> "no_data";
            case INITIAL, ALWAYS, INITIAL_ONLY -> rSnapshotMode.name().toLowerCase(Locale.ROOT);
        };

        properties.put("snapshot.mode", debeziumSnapshotMode);

        if (postgres.getPublicationName() != null) {
            properties.put("publication.name", runContext.render(postgres.getPublicationName()).as(String.class).orElseThrow());
        }

        if (postgres.getSslMode() != null) {
            properties.put("database.sslmode", runContext.render(postgres.getSslMode()).as(PostgresInterface.SslMode.class).orElseThrow().name().toUpperCase(Locale.ROOT).replace("_", "-"));
        }

        if (postgres.getSslRootCert() != null) {
            properties.put(
                "database.sslrootcert",
                runContext.workingDir().createTempFile(runContext.render(postgres.getSslRootCert()).as(String.class).orElseThrow().getBytes(StandardCharsets.UTF_8), ".pem").toAbsolutePath()
                    .toString()
            );
        }

        if (postgres.getSslCert() != null) {
            properties.put(
                "database.sslcert",
                runContext.workingDir().createTempFile(runContext.render(postgres.getSslCert()).as(String.class).orElseThrow().getBytes(StandardCharsets.UTF_8), ".pem").toAbsolutePath()
                    .toString()
            );
        }

        if (postgres.getSslKey() != null) {
            properties.put(
                "database.sslkey", convertPrivateKey(
                    runContext,
                    runContext.render(postgres.getSslKey()).as(String.class).orElseThrow(),
                    runContext.render(postgres.getSslKeyPassword()).as(String.class).orElseThrow()
                )
            );
        }

        if (postgres.getSslKeyPassword() != null) {
            properties.put("database.sslpassword", runContext.render(postgres.getSslKeyPassword()).as(String.class).orElseThrow());
        }
    }

    private static Object readPem(RunContext runContext, String vars) throws IllegalVariableEvaluationException, IOException {
        try (
            StringReader reader = new StringReader(runContext.render(vars));
            PEMParser pemParser = new PEMParser(reader)
        ) {
            return pemParser.readObject();
        }
    }

    private static synchronized void addProvider() {
        Provider bc = Security.getProvider("BC");
        if (bc == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    private static String convertPrivateKey(RunContext runContext, String vars, String password)
        throws IOException, IllegalVariableEvaluationException, OperatorCreationException, PKCSException {
        PostgresService.addProvider();

        Object pemObject = readPem(runContext, vars);

        PrivateKeyInfo keyInfo;
        if (pemObject instanceof PEMEncryptedKeyPair) {
            if (password == null) {
                throw new IOException("Unable to import private key. Key is encrypted, but no password was provided.");
            }

            PEMDecryptorProvider decrypter = new JcePEMDecryptorProviderBuilder()
                .setProvider("BC")
                .build(password.toCharArray());

            PEMKeyPair decryptedKeyPair = ((PEMEncryptedKeyPair) pemObject).decryptKeyPair(decrypter);
            keyInfo = decryptedKeyPair.getPrivateKeyInfo();
        } else if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
            if (password == null) {
                throw new IOException("Unable to import private key. Key is encrypted, but no password was provided.");
            }

            InputDecryptorProvider inputDecryptorProvider = new JceOpenSSLPKCS8DecryptorProviderBuilder()
                .setProvider("BC")
                .build(password.toCharArray());

            keyInfo = ((PKCS8EncryptedPrivateKeyInfo) pemObject).decryptPrivateKeyInfo(inputDecryptorProvider);
        } else {
            keyInfo = ((PEMKeyPair) pemObject).getPrivateKeyInfo();
        }

        PrivateKey privateKey = new JcaPEMKeyConverter().getPrivateKey(keyInfo);

        return runContext.workingDir().createTempFile(privateKey.getEncoded(), ".der").toAbsolutePath().toString();
    }

    public static String resolveSlotName(RunContext runContext, PostgresInterface postgres)
        throws IllegalVariableEvaluationException {
        return resolveSlotName(runContext, postgres, null);
    }

    public static String resolveSlotName(RunContext runContext, PostgresInterface postgres, String connectorId)
        throws IllegalVariableEvaluationException {
        if (postgres.getSlotName() != null) {
            return runContext.render(postgres.getSlotName()).as(String.class).orElseThrow();
        }

        String effectiveConnectorId = connectorId != null ? connectorId : deriveDefaultConnectorId(runContext, postgres);

        try {
            var flowInfo = runContext.flowInfo();
            if (flowInfo != null && flowInfo.namespace() != null) {
                var kvStore = runContext.namespaceKv(flowInfo.namespace());
                var taskRunValue = runContext.storage().getTaskStorageContext()
                    .map(StorageContext.Task::getTaskRunValue)
                    .orElse(null);

                String stateName = "debezium-state";
                if (postgres instanceof AbstractDebeziumTask task && task.getStateName() != null) {
                    stateName = runContext.render(task.getStateName()).as(String.class).orElse("debezium-state");
                }

                String slotMetaFilename = "pg-slot-" + effectiveConnectorId + ".txt";
                String slotMetaKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, slotMetaFilename, taskRunValue);

                var existingSlot = kvStore.getValue(slotMetaKey);
                if (existingSlot.isPresent() && existingSlot.get().value() != null) {
                    Object val = existingSlot.get().value();
                    String recordedSlot = val instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : val.toString();
                    if (!recordedSlot.isBlank()) {
                        return recordedSlot;
                    }
                }

                String combinedKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.COMBINED_STATE_FILE, taskRunValue);
                String legacyOffsetKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.OFFSETS_DATA_FILE, taskRunValue);

                boolean hasLegacyState = (kvStore.getValue(combinedKey).isPresent() && kvStore.getValue(combinedKey).get().value() != null)
                    || (kvStore.getValue(legacyOffsetKey).isPresent() && kvStore.getValue(legacyOffsetKey).get().value() != null);

                if (hasLegacyState) {
                    runContext.logger().warn(
                        "PostgreSQL CDC task is resuming from the legacy default replication slot '{}'. " +
                            "To prevent conflicts with other tasks sharing the same database, consider explicitly configuring 'slotName'.",
                        LEGACY_SLOT_NAME
                    );
                    try {
                        kvStore.put(slotMetaKey, new KVValueAndMetadata(null, LEGACY_SLOT_NAME.getBytes(StandardCharsets.UTF_8)));
                    } catch (Exception e) {
                        runContext.logger().debug("Could not record legacy slot metadata: {}", e.getMessage());
                    }
                    return LEGACY_SLOT_NAME;
                }

                try {
                    kvStore.put(slotMetaKey, new KVValueAndMetadata(null, effectiveConnectorId.getBytes(StandardCharsets.UTF_8)));
                } catch (Exception e) {
                    runContext.logger().debug("Could not record slot metadata: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            runContext.logger().warn("Failed to check or record slot metadata from KV store: {}", e.getMessage());
        }

        return effectiveConnectorId;
    }

    private static String deriveDefaultConnectorId(RunContext runContext, PostgresInterface postgres) {
        var flowInfo = runContext.flowInfo();
        var taskRunInfo = runContext.taskRunInfo();

        String namespace = flowInfo != null && flowInfo.namespace() != null ? flowInfo.namespace() : "";
        String flowId = flowInfo != null && flowInfo.id() != null ? flowInfo.id() : "";

        String ownId = null;
        if (postgres instanceof io.kestra.core.models.tasks.Task task) {
            ownId = task.getId();
        } else if (postgres instanceof io.kestra.core.models.triggers.AbstractTrigger trigger) {
            ownId = trigger.getId();
        }

        String taskRunTaskId = taskRunInfo != null ? taskRunInfo.taskId() : null;
        String taskId = ownId != null ? ownId : (taskRunTaskId != null ? taskRunTaskId : "");
        String iterationValue = taskRunInfo != null && taskRunInfo.value() != null ? taskRunInfo.value().toString() : "";

        String identity = namespace + "|" + flowId + "|" + taskId + "|" + iterationValue;
        String hash = Hashing.hashToString(identity).substring(0, 8);
        return "kestra_" + hash;
    }
}
