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

import com.fasterxml.jackson.databind.node.ObjectNode;

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
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.serializers.JacksonMapper;
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

    public static final String OFFSETS_DATA_FILE = "offsets.dat";

    public static final String DBHISTORY_DATA_FILE = "dbhistory.dat";

    // Single combined key written atomically; replaces the two legacy per-file keys.
    public static final String COMBINED_STATE_FILE = "debezium-state.dat";

    // Map keys used inside the combined KV entry.
    public static final String STATE_KEY_OFFSETS = "offsets";

    public static final String STATE_KEY_HISTORY = "history";

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
        title = "The maximum number of rows to fetch before stopping",
        description = "It's not an hard limit and is evaluated every second."
    )
    @PluginProperty(group = "execution")
    private Property<Integer> maxRecords;

    @Schema(
        title = "The maximum duration waiting for new rows",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration;

    @Schema(
        title = "The maximum total processing duration",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    @PluginProperty(group = "execution")
    @Builder.Default
    private Property<Duration> maxWait = Property.ofValue(Duration.ofSeconds(10));

    @Schema(
        title = "The maximum duration waiting for the snapshot to ends",
        description = "It's not an hard limit and is evaluated every second.\n The properties 'maxRecord', 'maxDuration' and 'maxWait' are evaluated only after the snapshot is done."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> maxSnapshotDuration = Property.ofValue(Duration.ofHours(1));

    @Schema(
        title = "When to commit the offsets to the KV Store",
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
        Path historyFile = runContext.workingDir().path().resolve(DBHISTORY_DATA_FILE);
        this.restoreState(runContext, offsetFile, historyFile);

        var identity = resolveEffectiveIdentity(runContext);
        migrateOffsetFile(runContext.logger(), offsetFile, identity.name(), identity.topicPrefix());
        if (this.needDatabaseHistory()) {
            migrateHistoryFile(runContext.logger(), historyFile, identity.topicPrefix());
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

        var combinedKey = saveFinalState(runContext, offsetFile, historyFile);
        if (combinedKey != null) {
            outputBuilder.stateOffsetKey(combinedKey);
            outputBuilder.stateHistoryKey(combinedKey);
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

    /** Carries the effective connector name and topic prefix after applying user overrides. */
    record ConnectorIdentity(String name, String topicPrefix) {}

    /**
     * Resolves the effective connector name and topic.prefix for this run.
     *
     * The base values are the derived connector id. If the user supplied a `properties` map that
     * overrides `name` or `topic.prefix`, those override values win — matching exactly what
     * {@link #properties(RunContext, Path, Path)} applies at runtime. Migration must target the
     * same effective key that Debezium will actually look up in the offset store.
     */
    ConnectorIdentity resolveEffectiveIdentity(RunContext runContext) throws IllegalVariableEvaluationException {
        var connectorId = deriveConnectorId(runContext);
        var effectiveName = connectorId;
        var effectiveTopicPrefix = connectorId;

        if (this.properties != null) {
            var userProps = runContext.render(this.properties).asMap(String.class, String.class);
            if (userProps.containsKey("name")) {
                effectiveName = runContext.render(userProps.get("name"));
            }
            if (userProps.containsKey("topic.prefix")) {
                effectiveTopicPrefix = runContext.render(userProps.get("topic.prefix"));
            }
        }

        return new ConnectorIdentity(effectiveName, effectiveTopicPrefix);
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
     * Rewrites the offset file so the effective connector identity is the active key.
     *
     * Without this, upgrading from the hardcoded "engine"/"kestra_" identity to the new
     * derived "kestra_<hex>" identity makes Debezium treat the run as a first-ever start
     * (no matching offset found) and triggers a full re-snapshot of already-captured rows.
     *
     * The target key is built from the effective name and topic.prefix, which may differ when
     * the user overrides either in the `properties` map — matching what Debezium will look up.
     *
     * Safe: wrapped in try/catch so any failure leaves the file untouched — worst case is
     * the pre-existing re-snapshot behavior, not a new regression.
     */
    static void migrateOffsetFile(Logger logger, Path offsetFile, String effectiveName, String effectiveTopicPrefix) {
        if (!offsetFile.toFile().exists() || offsetFile.toFile().length() == 0) {
            return;
        }

        try {
            HashMap<byte[], byte[]> offsets;
            try (var fis = new FileInputStream(offsetFile.toFile());
                 var ois = new ObjectInputStream(fis)) {
                @SuppressWarnings("unchecked")
                var loaded = (HashMap<byte[], byte[]>) ois.readObject();
                offsets = loaded;
            }

            var newKey = offsetKey(effectiveName, effectiveTopicPrefix).getBytes(StandardCharsets.UTF_8);
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

            logger.info("Migrated legacy Debezium offset from connector 'engine'/'kestra_' to '{}'/'{}''", effectiveName, effectiveTopicPrefix);
        } catch (Exception e) {
            logger.warn("Could not migrate legacy offset file; Debezium may re-snapshot (file left untouched): {}", e.getMessage());
        }
    }

    /**
     * Rewrites the schema-history file (used by MySQL, Oracle, SQLServer, Db2) so that
     * {@code source.server} entries matching the legacy server name "kestra_" become the
     * new effective topic prefix.
     *
     * FileSchemaHistory stores one JSON object per line. Scoping the rewrite to
     * {@code source.server} prevents corrupting DDL text or column values that happen to
     * contain the literal string "kestra_".
     *
     * Best-effort: any line that cannot be parsed as JSON is left unchanged verbatim. Any I/O
     * or overall failure leaves the file untouched.
     */
    static void migrateHistoryFile(Logger logger, Path historyFile, String newTopicPrefix) {
        if (!historyFile.toFile().exists() || historyFile.toFile().length() == 0) {
            return;
        }

        try {
            var lines = Files.readAllLines(historyFile, StandardCharsets.UTF_8);

            // Idempotency: if any source.server already equals the new prefix, file is migrated.
            var mapper = JacksonMapper.ofJson();
            boolean alreadyMigrated = lines.stream().anyMatch(line -> {
                try {
                    var node = mapper.readTree(line);
                    var sourceServer = node.path("source").path("server");
                    return !sourceServer.isMissingNode() && newTopicPrefix.equals(sourceServer.asText());
                } catch (Exception ignored) {
                    return false;
                }
            });
            if (alreadyMigrated) {
                return;
            }

            boolean anyChanged = false;
            var rewritten = new ArrayList<String>(lines.size());
            for (var line : lines) {
                String out = line;
                try {
                    var node = mapper.readTree(line);
                    var source = node.path("source");
                    if (!source.isMissingNode() && source.isObject()) {
                        var serverNode = source.path("server");
                        if (!serverNode.isMissingNode() && LEGACY_TOPIC_PREFIX.equals(serverNode.asText())) {
                            ((ObjectNode) source).put("server", newTopicPrefix);
                            out = mapper.writeValueAsString(node);
                            anyChanged = true;
                        }
                    }
                } catch (Exception ignored) {
                    // Unparseable line left as-is — best-effort guarantee.
                }
                rewritten.add(out);
            }

            if (!anyChanged) {
                return;
            }

            Files.write(historyFile, rewritten, StandardCharsets.UTF_8);
            logger.info("Migrated legacy Debezium schema history from server 'kestra_' to '{}'", newTopicPrefix);
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

    /**
     * Restores debezium state from KV. Tries the combined atomic key first; falls back to the
     * two legacy per-file keys so existing deployments upgrade without a forced re-snapshot.
     */
    public void restoreState(RunContext runContext, Path offsetFile, Path historyFile) throws IOException {
        try {
            var taskRunValue = runContext.storage().getTaskStorageContext()
                .map(StorageContext.Task::getTaskRunValue)
                .orElse(null);
            var stateName = runContext.render(this.stateName).as(String.class).orElseThrow();
            var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());

            var combinedKey = computeKvStoreKey(runContext, stateName, COMBINED_STATE_FILE, taskRunValue);
            var combinedValue = kvStore.getValue(combinedKey);

            if (combinedValue.isPresent() && combinedValue.get().value() != null) {
                @SuppressWarnings("unchecked")
                var stateMap = (Map<String, Object>) combinedValue.get().value();
                restoreFileFromMap(stateMap, STATE_KEY_OFFSETS, offsetFile);
                if (this.needDatabaseHistory()) {
                    restoreFileFromMap(stateMap, STATE_KEY_HISTORY, historyFile);
                }
                return;
            }

            // Legacy fallback: read the two separate keys written by older versions.
            restoreLegacyKey(kvStore, runContext, stateName, OFFSETS_DATA_FILE, offsetFile, taskRunValue);
            if (this.needDatabaseHistory()) {
                restoreLegacyKey(kvStore, runContext, stateName, DBHISTORY_DATA_FILE, historyFile, taskRunValue);
            }
        } catch (FileNotFoundException | ResourceExpiredException ignored) {
        } catch (IllegalVariableEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void restoreFileFromMap(Map<String, Object> stateMap, String key, Path targetFile) throws IOException {
        var data = stateMap.get(key);
        if (data instanceof byte[] bytes) {
            Files.write(targetFile, bytes);
        }
    }

    private static void restoreLegacyKey(KVStore kvStore, RunContext runContext, String stateName, String filename, Path targetFile, String taskRunValue)
        throws IOException, ResourceExpiredException {
        try {
            var kvKey = computeKvStoreKey(runContext, stateName, filename, taskRunValue);
            var kvValue = kvStore.getValue(kvKey);
            if (kvValue.isPresent() && kvValue.get().value() != null) {
                FileUtils.copyInputStreamToFile(
                    new ByteArrayInputStream((byte[]) kvValue.get().value()),
                    targetFile.toFile()
                );
            }
        } catch (IllegalVariableEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes offset + history as ONE atomic KV entry so the two states can never desync on crash.
     * Returns the combined key written, or null if the entry would be inconsistent (offsets absent,
     * or history absent for a history-needing connector) — in that case nothing is written.
     */
    public String saveStateAtomically(RunContext runContext, Path offsetFile, Path historyFile) throws IOException {
        try {
            if (!offsetFile.toFile().exists()) {
                return null;
            }
            if (this.needDatabaseHistory() && !historyFile.toFile().exists()) {
                // Writing offsets without history would cause "db history topic is missing" on restore.
                runContext.logger().debug(
                    "Skipping atomic state save: history file not yet available ({}). Will retry on next batch or end-of-run.",
                    historyFile.getFileName()
                );
                return null;
            }

            var taskRunValue = runContext.storage().getTaskStorageContext()
                .map(StorageContext.Task::getTaskRunValue)
                .orElse(null);
            var stateName = runContext.render(this.stateName).as(String.class).orElseThrow();
            var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
            var combinedKey = computeKvStoreKey(runContext, stateName, COMBINED_STATE_FILE, taskRunValue);

            var stateMap = new LinkedHashMap<String, byte[]>();
            stateMap.put(STATE_KEY_OFFSETS, Files.readAllBytes(offsetFile));
            if (this.needDatabaseHistory()) {
                stateMap.put(STATE_KEY_HISTORY, Files.readAllBytes(historyFile));
            }

            kvStore.put(combinedKey, new KVValueAndMetadata(null, stateMap));
            return combinedKey;
        } catch (IllegalVariableEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    protected void saveOffsetsForTask(RunContext runContext, Path offsetFile, Path historyFile) throws IOException {
        saveStateAtomically(runContext, offsetFile, historyFile);
    }

    /**
     * Persists state at end-of-run. Unlike the per-batch save (which logs a skip at debug, since the
     * history file routinely lags during early streaming), this warns when offsets were produced but
     * nothing could be persisted — the last-chance case that silently forces a re-snapshot next run.
     */
    public String saveFinalState(RunContext runContext, Path offsetFile, Path historyFile) throws IOException {
        var combinedKey = saveStateAtomically(runContext, offsetFile, historyFile);
        if (combinedKey == null && offsetFile.toFile().exists()) {
            runContext.logger().warn(
                "Debezium produced offsets but state was not persisted because the schema history file ({}) is missing; "
                + "the next run will re-snapshot from scratch.",
                historyFile.getFileName()
            );
        }
        return combinedKey;
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
            title = "The KV Store key under which the combined Debezium state (offset + schema history) is stored",
            description = """
                Both `stateOffsetKey` and `stateHistoryKey` point to the same combined entry written atomically.
                The entry holds a map with keys `offsets` and `history` so both states are always consistent.
                """
        )
        private String stateOffsetKey;

        @Deprecated
        @Schema(
            title = "Deprecated — use stateOffsetKey",
            description = "Deprecated — use stateOffsetKey."
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
