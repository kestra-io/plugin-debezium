package io.kestra.plugin.debezium;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.Hashing;

import ch.qos.logback.classic.LoggerContext;
import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwFunction;
import static io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger.computeKvStoreKey;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDebeziumTask extends Task implements RunnableTask<AbstractDebeziumTask.Output>, AbstractDebeziumInterface {

    private static final String OFFSETS_DATA_FILE = "offsets.dat";

    private static final String DBHISTORY_DATA_FILE = "dbhistory.dat";

    @Builder.Default
    protected Property<Format> format = Property.ofValue(Format.INLINE);

    @Builder.Default
    protected Property<Deleted> deleted = Property.ofValue(Deleted.ADD_FIELD);

    @Builder.Default
    protected Property<String> deletedFieldName = Property.ofValue("deleted");

    @Builder.Default
    protected Property<Key> key = Property.ofValue(Key.ADD_FIELD);

    @Builder.Default
    protected Property<Metadata> metadata = Property.ofValue(Metadata.ADD_FIELD);

    @Builder.Default
    protected Property<String> metadataFieldName = Property.ofValue("metadata");

    @Builder.Default
    protected Property<SplitTable> splitTable = Property.ofValue(SplitTable.TABLE);

    @Builder.Default
    protected Property<Boolean> ignoreDdl = Property.ofValue(true);

    protected Property<String> hostname;

    protected Property<String> port;

    protected Property<String> username;

    protected Property<String> password;

    private Object includedDatabases;

    private Object excludedDatabases;

    private Object includedTables;

    private Object excludedTables;

    private Object includedColumns;

    private Object excludedColumns;

    private Property<Map<String, String>> properties;

    @Builder.Default
    protected Property<String> stateName = Property.ofValue("debezium-state");

    @Schema(
        title = "The maximum number of rows to fetch before stopping.",
        description = "It's not an hard limit and is evaluated every second."
    )
    @PluginProperty(group = "execution")
    private Property<Integer> maxRecords;

    @Schema(
        title = "The maximum duration waiting for new rows.",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration;

    @Schema(
        title = "The maximum total processing duration.",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    @PluginProperty(group = "execution")
    @Builder.Default
    private Property<Duration> maxWait = Property.ofValue(Duration.ofSeconds(10));

    @Schema(
        title = "The maximum duration waiting for the snapshot to ends.",
        description = "It's not an hard limit and is evaluated every second.\n The properties 'maxRecord', 'maxDuration' and 'maxWait' are evaluated only after the snapshot is done."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> maxSnapshotDuration = Property.ofValue(Duration.ofHours(1));

    @Schema(
        title = "When to commit the offsets to the KV Store.",
        description = """
            Possible values are:
            - `ON_EACH_BATCH`: after each batch of records consumed by this task, the offsets will be stored in the KV Store. This avoids any duplicated records being consumed but can be costly if many events are produced.
            - `ON_STOP`: when this task completes, the offsets will be stored in the KV Store. This avoids any un-necessary writes to the KV Store.
            """
    )
    @Builder.Default
    private Property<AbstractDebeziumRealtimeTrigger.OffsetCommitMode> offsetsCommitMode = Property.ofValue(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_STOP);

    protected abstract boolean needDatabaseHistory();

    static {
        // https://issues.redhat.com/browse/DBZ-4904

        LoggerFactory.getLogger("org.apache.kafka.connect");
        LoggerFactory.getLogger("io.debezium");

        ((LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory())
            .getLoggerList()
            .stream()
            .filter(
                logger -> logger.getName().startsWith("org.apache.kafka.connect") ||
                    logger.getName().startsWith("io.debezium")
            )
            .forEach(
                logger -> logger.setLevel(ch.qos.logback.classic.Level.ERROR)
            );
    }

    @Override
    public AbstractDebeziumTask.Output run(RunContext runContext) throws Exception {
        AtomicInteger count = new AtomicInteger();
        AtomicBoolean snapshot = new AtomicBoolean(false);
        ZonedDateTime lastRecord = ZonedDateTime.now();

        Path offsetFile = runContext.workingDir().path().resolve(OFFSETS_DATA_FILE);
        this.restoreState(runContext, offsetFile);

        Path historyFile = runContext.workingDir().path().resolve(DBHISTORY_DATA_FILE);
        if (this.needDatabaseHistory()) {
            this.restoreState(runContext, historyFile);
        }

        var connectorId = deriveConnectorId(runContext);
        migrateOffsetFile(runContext.logger(), offsetFile, connectorId);
        if (this.needDatabaseHistory()) {
            migrateHistoryFile(runContext.logger(), historyFile, connectorId);
        }

        final Properties props = this.properties(runContext, offsetFile, historyFile);

        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

        CompletionCallback completionCallback = new CompletionCallback(runContext, executorService);
        ChangeConsumer changeConsumer = new ChangeConsumer(this, runContext, count, snapshot, lastRecord, offsetFile, historyFile);

        try (
            DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = DebeziumEngine.create(Connect.class)
                .using(this.getClass().getClassLoader())
                .using(props)
                .notifying(changeConsumer)
                .using(completionCallback)
                .build()
        ) {
            executorService.execute(engine);

            ZonedDateTime snapshotStarted = ZonedDateTime.now();
            boolean consumes;
            do {
                int previousCount = count.get();
                ZonedDateTime captureStarted = ZonedDateTime.now();
                Await.until(() ->
                {
                    try {
                        return this.ended(executorService, count, captureStarted, lastRecord, snapshot, runContext);
                    } catch (IllegalVariableEvaluationException e) {
                        throw new RuntimeException(e);
                    }
                }, Duration.ofSeconds(1));
                consumes = count.get() > previousCount;
                // if we are still snapshotting, allow waiting for more time until snapshot wait duration is reached
            } while (snapshot.get() && consumes && ZonedDateTime.now().isBefore(snapshotStarted.plus(runContext.render(this.maxSnapshotDuration).as(Duration.class).orElseThrow())));
        }

        if (completionCallback.getError() != null) {
            throw new Exception(completionCallback.getError());
        }

        this.shutdown(runContext.logger(), executorService);

        Output.OutputBuilder outputBuilder = Output.builder();

        if (offsetFile.toFile().exists()) {
            try (FileInputStream fis = new FileInputStream(offsetFile.toFile())) {
                String taskRunValue = runContext.storage().getTaskStorageContext().map(StorageContext.Task::getTaskRunValue).orElse(null);
                String stateName = runContext.render(this.stateName).as(String.class).orElseThrow();
                String fileName = offsetFile.getFileName().toString();
                String kvKey = computeKvStoreKey(runContext, stateName, fileName, taskRunValue);

                KVStore kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());

                kvStore.put(kvKey, new KVValueAndMetadata(null, fis.readAllBytes()));

                outputBuilder.stateOffsetKey(kvKey);
            }
        }

        if (this.needDatabaseHistory()) {
            try (FileInputStream fis = new FileInputStream(historyFile.toFile())) {
                String taskRunValue = runContext.storage().getTaskStorageContext().map(StorageContext.Task::getTaskRunValue).orElse(null);
                String stateName = runContext.render(this.stateName).as(String.class).orElseThrow();
                String fileName = historyFile.getFileName().toString();
                String kvKey = computeKvStoreKey(runContext, stateName, fileName, taskRunValue);

                KVStore kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());

                kvStore.put(kvKey, new KVValueAndMetadata(null, fis.readAllBytes()));

                outputBuilder.stateHistoryKey(kvKey);
            }
        }

        outputBuilder
            .uris(
                changeConsumer
                    .getRecords()
                    .entrySet()
                    .stream()
                    .map(throwFunction(e ->
                    {
                        e.getValue().getRight().flush();
                        e.getValue().getRight().close();

                        return new AbstractMap.SimpleEntry<>(
                            e.getKey(),
                            runContext.storage().putFile(e.getValue().getLeft())
                        );

                    }))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );

        changeConsumer.getRecordsCount().forEach((s, atomicInteger) ->
        {
            runContext.metric(Counter.of("records", atomicInteger.get(), "source", s));
        });

        runContext.logger().info(
            "Ended after receiving {} records: {}",
            changeConsumer.getRecordsCount().values().stream().mapToLong(AtomicInteger::get).sum(),
            changeConsumer.getRecordsCount()
        );

        return outputBuilder
            .size(count.get())
            .build();
    }

    /**
     * Derives a stable, unique connector identity scoped to this task's logical identity
     * (namespace + flow + task, plus iteration value when inside a ForEach).
     *
     * The 8-character hex suffix (murmur3-128 truncated) keeps the identifier short and
     * valid as a Debezium topic.prefix while guaranteeing uniqueness across concurrent
     * connectors on the same JVM so their JMX MBean names never collide.
     *
     * The same value is used for `topic.prefix`, `name`, and `database.server.name` so all
     * three remain consistent. A user-supplied `properties` map applied afterwards can still
     * override any of these values.
     */
    String deriveConnectorId(RunContext runContext) {
        var flowInfo = runContext.flowInfo();
        var taskRunInfo = runContext.taskRunInfo();

        String namespace = flowInfo.namespace() != null ? flowInfo.namespace() : "";
        String flowId = flowInfo.id() != null ? flowInfo.id() : "";

        String taskRunTaskId = taskRunInfo != null ? taskRunInfo.taskId() : null;
        String taskId = resolveTaskId(this.getId(), taskRunTaskId);

        // Include task-run value when present (ForEach / parallel iterations) to distinguish
        // concurrently running iterations of the same task.
        String iterationValue = taskRunInfo != null && taskRunInfo.value() != null ? taskRunInfo.value().toString() : "";

        return connectorIdFromParts(namespace, flowId, taskId, iterationValue);
    }

    /**
     * Resolves the task component of the connector identity.
     *
     * Prefers the task/trigger's own id: it is always set, including during trigger
     * evaluation where {@code taskRunInfo} is empty (no execution/taskrun exists yet).
     * Without this, two distinct Debezium triggers in the same flow would derive the same
     * id and their JMX MBeans would still collide. Falls back to the taskRunInfo task id,
     * then to an empty string, only as a safety net.
     *
     * Exposed package-private for unit testing without a full DI context.
     */
    static String resolveTaskId(String ownId, String taskRunTaskId) {
        if (ownId != null) {
            return ownId;
        }
        return taskRunTaskId != null ? taskRunTaskId : "";
    }

    /**
     * Pure function that computes the connector id from its constituent parts.
     * Exposed package-private for unit testing without a full DI context.
     */
    static String connectorIdFromParts(String namespace, String flowId, String taskId, String iterationValue) {
        String identity = namespace + "|" + flowId + "|" + taskId + "|" + iterationValue;
        // Truncate to 8 hex chars (32 bits of murmur3-128) — short but collision-resistant
        // enough for the expected cardinality of concurrent Kestra tasks.
        String hash = Hashing.hashToString(identity).substring(0, 8);
        return "kestra_" + hash;
    }

    // Pre-fix hardcoded values — the identity every existing offset file was written with.
    static final String LEGACY_CONNECTOR_NAME = "engine";
    static final String LEGACY_TOPIC_PREFIX = "kestra_";

    /**
     * Builds the compact JSON key that FileOffsetBackingStore stores in its HashMap.
     * Format: ["<name>",{"server":"<topicPrefix>"}] — no spaces, as written by Kafka Connect.
     */
    static String offsetKey(String connectorName, String topicPrefix) {
        return "[\"" + connectorName + "\",{\"server\":\"" + topicPrefix + "\"}]";
    }

    /**
     * Rewrites the offset file so the derived connector id is the active key.
     *
     * Without this, upgrading from the hardcoded "engine"/"kestra_" identity to the new
     * derived "kestra_<hex>" identity makes Debezium treat the run as a first-ever start
     * (no matching offset found) and triggers a full re-snapshot of already-captured rows.
     *
     * Safe: wrapped in try/catch so any failure leaves the file untouched — worst case is
     * the pre-existing re-snapshot behavior, not a new regression.
     */
    static void migrateOffsetFile(Logger logger, Path offsetFile, String newConnectorId) {
        if (!offsetFile.toFile().exists() || offsetFile.toFile().length() == 0) {
            return;
        }

        try {
            HashMap<byte[], byte[]> offsets;
            try (var ois = new ObjectInputStream(new FileInputStream(offsetFile.toFile()))) {
                @SuppressWarnings("unchecked")
                var loaded = (HashMap<byte[], byte[]>) ois.readObject();
                offsets = loaded;
            }

            var newKey = offsetKey(newConnectorId, newConnectorId).getBytes(StandardCharsets.UTF_8);
            var legacyKey = offsetKey(LEGACY_CONNECTOR_NAME, LEGACY_TOPIC_PREFIX).getBytes(StandardCharsets.UTF_8);

            // Idempotency: new key already present means this run was already migrated or is native.
            boolean alreadyMigrated = offsets.keySet().stream()
                .anyMatch(k -> Arrays.equals(k, newKey));
            if (alreadyMigrated) {
                return;
            }

            // MongoDB uses a different partition shape (e.g. {"rs":"...", "server_id":"..."});
            // its legacy offset key won't match LEGACY_CONNECTOR_NAME / LEGACY_TOPIC_PREFIX so
            // this method is effectively a no-op for MongoDB — the file is left unchanged.
            byte[] legacyValue = null;
            byte[] legacyKeyInMap = null;
            for (var entry : offsets.entrySet()) {
                if (Arrays.equals(entry.getKey(), legacyKey)) {
                    legacyValue = entry.getValue();
                    legacyKeyInMap = entry.getKey();
                    break;
                }
            }

            if (legacyValue == null) {
                return;
            }

            offsets.remove(legacyKeyInMap);
            offsets.put(newKey, legacyValue);

            try (var oos = new ObjectOutputStream(Files.newOutputStream(offsetFile))) {
                oos.writeObject(offsets);
            }

            logger.info("Migrated legacy Debezium offset from connector 'engine' to '{}'", newConnectorId);
        } catch (Exception e) {
            logger.warn("Could not migrate legacy offset file; Debezium may re-snapshot (file left untouched): {}", e.getMessage());
        }
    }

    /**
     * Rewrites the schema-history file (used by MySQL, Oracle, SQLServer, Db2) so that
     * references to the legacy server name "kestra_" become the new derived connector id.
     *
     * FileSchemaHistory stores one JSON object per line; each record embeds the logical server
     * name in its source/partition section. We do a targeted string replacement so that only
     * the server-name token changes and the rest of each record is preserved verbatim.
     *
     * Best-effort: any line we cannot safely rewrite is left unchanged. Any I/O or parse
     * failure leaves the file untouched.
     */
    static void migrateHistoryFile(Logger logger, Path historyFile, String newConnectorId) {
        if (!historyFile.toFile().exists() || historyFile.toFile().length() == 0) {
            return;
        }

        try {
            var lines = Files.readAllLines(historyFile, StandardCharsets.UTF_8);

            // Idempotency: if any line already references the new id, the file was already migrated.
            boolean alreadyMigrated = lines.stream().anyMatch(l -> l.contains("\"" + newConnectorId + "\""));
            if (alreadyMigrated) {
                return;
            }

            // Replace "kestra_" server token with the new id in every line that contains it.
            // We match the JSON string literal exactly to avoid touching unrelated content.
            var legacyToken = "\"" + LEGACY_TOPIC_PREFIX + "\"";
            var newToken = "\"" + newConnectorId + "\"";

            boolean anyChanged = false;
            var rewritten = new ArrayList<String>(lines.size());
            for (var line : lines) {
                if (line.contains(legacyToken)) {
                    rewritten.add(line.replace(legacyToken, newToken));
                    anyChanged = true;
                } else {
                    rewritten.add(line);
                }
            }

            if (!anyChanged) {
                return;
            }

            Files.write(historyFile, rewritten, StandardCharsets.UTF_8);
            logger.info("Migrated legacy Debezium schema history from server 'kestra_' to '{}'", newConnectorId);
        } catch (Exception e) {
            logger.warn("Could not migrate legacy schema-history file; Debezium may re-snapshot (file left untouched): {}", e.getMessage());
        }
    }

    protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) throws Exception {
        final Properties props = new Properties();

        var connectorId = deriveConnectorId(runContext);

        props.setProperty("name", connectorId);

        // offset
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", offsetFile.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "1000");

        // database
        props.setProperty("database.server.name", connectorId);

        if (this.needDatabaseHistory()) {
            props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
            props.setProperty("schema.history.internal.file.filename", historyFile.toAbsolutePath().toString());
        }

        // connection
        if (this.hostname != null) {
            props.setProperty("database.hostname", runContext.render(this.hostname).as(String.class).orElseThrow());
        }

        if (this.port != null) {
            props.setProperty("database.port", runContext.render(this.port).as(String.class).orElseThrow());
        }

        if (this.username != null) {
            props.setProperty("database.user", runContext.render(this.username).as(String.class).orElseThrow());
        }

        if (this.password != null) {
            props.setProperty("database.password", runContext.render(this.password).as(String.class).orElseThrow());
        }

        // https://debezium.io/documentation/reference/configuration/avro.html
        props.setProperty("key.converter.schemas.enable", "false");
        props.setProperty("value.converter.schemas.enable", "false");

        // delete are send with a full rows, we don't want to emulate kafka behaviour
        props.setProperty("tombstones.on.delete", "false");

        // required — use the same connector-unique id so JMX MBean names never collide
        props.setProperty("topic.prefix", connectorId);

        if (this.includedDatabases != null) {
            props.setProperty("database.include.list", joinProperties(runContext, this.includedDatabases));
        }

        if (this.excludedDatabases != null) {
            props.setProperty("database.exclude.list", joinProperties(runContext, this.excludedDatabases));
        }

        if (this.includedTables != null) {
            props.setProperty("table.include.list", joinProperties(runContext, this.includedTables));
        }

        if (this.excludedTables != null) {
            props.setProperty("table.exclude.list", joinProperties(runContext, this.excludedTables));
        }

        if (this.includedColumns != null) {
            props.setProperty("column.include.list", joinProperties(runContext, this.includedColumns));
        }

        if (this.excludedColumns != null) {
            props.setProperty("column.exclude.list", joinProperties(runContext, this.excludedColumns));
        }

        if (this.properties != null) {
            for (Map.Entry<String, String> entry : runContext.render(this.properties).asMap(String.class, String.class).entrySet()) {
                props.setProperty(runContext.render(entry.getKey()), runContext.render(entry.getValue()));
            }
        }

        return props;
    }

    @SuppressWarnings("unchecked")
    protected static String joinProperties(RunContext runContext, Object raw) {
        List<String> value = raw instanceof Collection ? (List<String>) raw : List.of((String) raw);

        return value.stream()
            // debezium needs commas escaped to split properly
            .map(x -> x.replaceAll(",", "\\,"))
            .collect(Collectors.joining(","));
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(ExecutorService executorService, AtomicInteger count, ZonedDateTime start, ZonedDateTime lastRecord, AtomicBoolean snapshot, RunContext runContext)
        throws IllegalVariableEvaluationException {
        if (executorService.isShutdown()) {
            return true;
        }

        // when snapshotting, we didn't take into account maxRecords
        var renderedMaxRecords = runContext.render(maxRecords).as(Integer.class);
        if (!snapshot.get() && renderedMaxRecords.isPresent() && count.get() >= renderedMaxRecords.get()) {
            return true;
        }

        var renderedMaxDuration = runContext.render(maxDuration).as(Duration.class);
        if (renderedMaxDuration.isPresent() && ZonedDateTime.now().isAfter(start.plus(renderedMaxDuration.get()))) {
            return true;
        }

        var renderedMaxWait = runContext.render(maxWait).as(Duration.class);
        if (renderedMaxWait.isPresent() && ZonedDateTime.now().isAfter(lastRecord.plus(renderedMaxWait.get()))) {
            return true;
        }

        return false;
    }

    protected void restoreState(RunContext runContext, Path path) throws IOException {
        try {
            String taskRunValue = runContext.storage().getTaskStorageContext()
                .map(StorageContext.Task::getTaskRunValue)
                .orElse(null);
            String stateName = runContext.render(this.stateName).as(String.class).orElseThrow();
            String filename = path.getFileName().toString();

            String kvKey = computeKvStoreKey(runContext, stateName, filename, taskRunValue);

            KVStore kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
            Optional<KVValue> kvValue = kvStore.getValue(kvKey);

            if (kvValue.isPresent() && kvValue.get().value() != null) {
                byte[] stateData = (byte[]) kvValue.get().value();
                FileUtils.copyInputStreamToFile(
                    new ByteArrayInputStream(stateData),
                    path.toFile()
                );
            }
            ;
        } catch (FileNotFoundException | ResourceExpiredException ignored) {

        } catch (IllegalVariableEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    protected void saveOffsetsForTask(RunContext runContext, Path offsetFile, Path historyFile) throws IOException {
        try {
            String taskRunValue = runContext.storage().getTaskStorageContext()
                .map(StorageContext.Task::getTaskRunValue)
                .orElse(null);
            String stateName = runContext.render(this.stateName).as(String.class).orElseThrow();

            KVStore kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());

            if (offsetFile.toFile().exists()) {
                try (FileInputStream fis = new FileInputStream(offsetFile.toFile())) {
                    String kvKey = computeKvStoreKey(runContext, stateName, offsetFile.getFileName().toString(), taskRunValue);
                    kvStore.put(kvKey, new KVValueAndMetadata(null, fis.readAllBytes()));
                }
            }

            if (this.needDatabaseHistory() && historyFile.toFile().exists()) {
                try (FileInputStream fis = new FileInputStream(historyFile.toFile())) {
                    String kvKey = computeKvStoreKey(runContext, stateName, historyFile.getFileName().toString(), taskRunValue);
                    kvStore.put(kvKey, new KVValueAndMetadata(null, fis.readAllBytes()));
                }
            }
        } catch (IllegalVariableEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    private void shutdown(Logger logger, ExecutorService executorService) {
        try {
            executorService.shutdown();
            while (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.trace("Waiting another 5 seconds for the embedded engine to shut down");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The KV Store key under which the state of the offset is stored"
        )
        private String stateOffsetKey;

        @Schema(
            title = "The KV Store key under which the state of the database history is stored"
        )
        private String stateHistoryKey;

        @Schema(
            title = "The number of fetched rows"
        )
        private Integer size;

        @Schema(
            title = "URI of the generated internal storage file"
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> uris;
    }

    public enum Key {
        ADD_FIELD,
        DROP,
    }

    public enum Metadata {
        ADD_FIELD,
        DROP,
    }

    public enum Format {
        RAW,
        INLINE,
        WRAP,
    }

    public enum Deleted {
        ADD_FIELD,
        NULL,
        DROP
    }

    public enum SplitTable {
        OFF,
        DATABASE,
        TABLE
    }
}
