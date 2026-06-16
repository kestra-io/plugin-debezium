package io.kestra.plugin.debezium;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
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
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.Await;

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
        Path historyFile = runContext.workingDir().path().resolve(DBHISTORY_DATA_FILE);
        this.restoreState(runContext, offsetFile, historyFile);

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
            String combinedKey = saveStateAtomically(runContext, offsetFile, historyFile);
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

    protected Properties properties(RunContext runContext, Path offsetFile, Path historyFile) throws Exception {
        final Properties props = new Properties();

        props.setProperty("name", "engine");

        // offset
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", offsetFile.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "1000");

        // database
        props.setProperty("database.server.name", "kestra");

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

        // required
        props.setProperty("topic.prefix", "kestra_");

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
     * Returns the combined key written.
     */
    public String saveStateAtomically(RunContext runContext, Path offsetFile, Path historyFile) throws IOException {
        try {
            var taskRunValue = runContext.storage().getTaskStorageContext()
                .map(StorageContext.Task::getTaskRunValue)
                .orElse(null);
            var stateName = runContext.render(this.stateName).as(String.class).orElseThrow();
            var kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
            var combinedKey = computeKvStoreKey(runContext, stateName, COMBINED_STATE_FILE, taskRunValue);

            var stateMap = new LinkedHashMap<String, byte[]>();
            if (offsetFile.toFile().exists()) {
                stateMap.put(STATE_KEY_OFFSETS, Files.readAllBytes(offsetFile));
            }
            if (this.needDatabaseHistory() && historyFile.toFile().exists()) {
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
            title = "The KV Store key under which the combined Debezium state (offset + schema history) is stored.",
            description = """
                Both `stateOffsetKey` and `stateHistoryKey` point to the same combined entry written atomically.
                The entry holds a map with keys `offsets` and `history` so both states are always consistent.
                """
        )
        private String stateOffsetKey;

        @Schema(
            title = "The KV Store key under which the combined Debezium state (offset + schema history) is stored.",
            description = "Same as `stateOffsetKey` — both fields point to the single atomic combined entry."
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
