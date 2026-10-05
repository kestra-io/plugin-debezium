package io.kestra.plugin.debezium.postgres;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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
import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.plugin.debezium.AbstractDebeziumInterface;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import io.kestra.plugin.debezium.AbstractDebeziumTask;

public abstract class PostgresService {
    public static final String LEGACY_SLOT_NAME = "kestra";

    public static void handleProperties(Properties properties, RunContext runContext, PostgresInterface postgres)
        throws IllegalVariableEvaluationException, IOException, OperatorCreationException, PKCSException {
        properties.put("database.dbname", runContext.render(postgres.getDatabase()).as(String.class).orElseThrow());
        properties.put("plugin.name", runContext.render(postgres.getPluginName()).as(PostgresInterface.PluginName.class).orElseThrow().name().toLowerCase(Locale.ROOT));
        properties.put("slot.name", resolveSlotName(runContext, postgres));

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
        throws IllegalVariableEvaluationException, IOException {
        if (postgres.getSlotName() != null) {
            return runContext.render(postgres.getSlotName())
                .as(String.class)
                .filter(slot -> !slot.isBlank())
                .orElseThrow(() -> new IllegalArgumentException(
                    "The 'slotName' property was specified but evaluated to an empty value. " +
                        "Provide a valid PostgreSQL replication slot name or omit the property to use the derived default."
                ));
        }

        AbstractDebeziumTask task = null;
        if (postgres instanceof AbstractDebeziumTask t) {
            task = t;
        } else if (postgres instanceof io.kestra.core.models.tasks.Task t) {
            var builder = Capture.builder().id(t.getId());
            if (postgres instanceof AbstractDebeziumInterface adi) {
                builder.stateName(adi.getStateName());
            }
            task = builder.build();
        } else if (postgres instanceof io.kestra.core.models.triggers.AbstractTrigger trigger) {
            var builder = Capture.builder().id(trigger.getId());
            if (postgres instanceof AbstractDebeziumInterface adi) {
                builder.stateName(adi.getStateName());
            }
            task = builder.build();
        }

        var effectiveConnectorId = task != null ? task.deriveConnectorId(runContext) : null;

        var flowInfo = runContext.flowInfo();
        if (flowInfo == null || flowInfo.namespace() == null || effectiveConnectorId == null) {
            return effectiveConnectorId != null ? effectiveConnectorId : LEGACY_SLOT_NAME;
        }

        var kvStore = runContext.namespaceKv(flowInfo.namespace());
        var taskRunValue = runContext.storage().getTaskStorageContext()
            .map(StorageContext.Task::getTaskRunValue)
            .orElse(null);

        var stateName = "debezium-state";
        if (task.getStateName() != null) {
            stateName = runContext.render(task.getStateName()).as(String.class).orElse("debezium-state");
        }

        var slotMetaFilename = "pg-slot-" + effectiveConnectorId + ".txt";
        var slotMetaKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, slotMetaFilename, taskRunValue);

        Optional<KVValue> existingSlot;
        try {
            existingSlot = kvStore.getValue(slotMetaKey);
        } catch (ResourceExpiredException | FileNotFoundException e) {
            existingSlot = Optional.empty();
        }

        if (existingSlot.isPresent() && existingSlot.get().value() != null) {
            var val = existingSlot.get().value();
            var recordedSlot = val instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : val.toString();
            if (!recordedSlot.isBlank()) {
                return recordedSlot;
            }
        }

        var hasLegacyState = false;
        var offsetFile = runContext.workingDir().path().resolve(AbstractDebeziumTask.OFFSETS_DATA_FILE);
        if (Files.exists(offsetFile) && Files.size(offsetFile) > 0) {
            hasLegacyState = task.hasLegacyOffsets(runContext, offsetFile);
        } else {
            var combinedKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.COMBINED_STATE_FILE, taskRunValue);
            Optional<KVValue> combinedValue;
            try {
                combinedValue = kvStore.getValue(combinedKey);
            } catch (ResourceExpiredException | FileNotFoundException e) {
                combinedValue = Optional.empty();
            }

            byte[] offsetData = null;
            if (combinedValue.isPresent() && combinedValue.get().value() != null) {
                var val = combinedValue.get().value();
                if (val instanceof Map<?, ?> stateMap) {
                    var rawOffsets = stateMap.get(AbstractDebeziumTask.STATE_KEY_OFFSETS);
                    if (rawOffsets instanceof byte[] bytes) {
                        offsetData = bytes;
                    }
                } else if (val instanceof byte[] bytes) {
                    offsetData = bytes;
                }
            }

            if (offsetData == null) {
                var legacyOffsetKey = AbstractDebeziumRealtimeTrigger.computeKvStoreKey(runContext, stateName, AbstractDebeziumTask.OFFSETS_DATA_FILE, taskRunValue);
                Optional<KVValue> legacyValue;
                try {
                    legacyValue = kvStore.getValue(legacyOffsetKey);
                } catch (ResourceExpiredException | FileNotFoundException e) {
                    legacyValue = Optional.empty();
                }
                if (legacyValue.isPresent() && legacyValue.get().value() != null) {
                    var val = legacyValue.get().value();
                    if (val instanceof byte[] bytes) {
                        offsetData = bytes;
                    }
                }
            }

            if (offsetData != null) {
                hasLegacyState = task.hasLegacyOffsets(runContext, offsetData);
            }
        }

        String chosenSlot;
        if (hasLegacyState) {
            runContext.logger().warn(
                "PostgreSQL CDC task is resuming from the legacy default replication slot '{}'. " +
                    "To prevent conflicts with other tasks sharing the same database, consider explicitly configuring 'slotName'.",
                LEGACY_SLOT_NAME
            );
            chosenSlot = LEGACY_SLOT_NAME;
        } else {
            chosenSlot = effectiveConnectorId;
        }

        kvStore.put(slotMetaKey, new KVValueAndMetadata(null, chosenSlot.getBytes(StandardCharsets.UTF_8)));

        return chosenSlot;
    }
}
