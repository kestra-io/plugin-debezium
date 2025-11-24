package io.kestra.plugin.debezium;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.Hashing;
import io.kestra.core.utils.Slugify;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDebeziumRealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<AbstractDebeziumRealtimeTrigger.StreamOutput> {
    @Builder.Default
    protected Property<AbstractDebeziumTask.Format> format = Property.ofValue(AbstractDebeziumTask.Format.INLINE);

    @Builder.Default
    protected Property<AbstractDebeziumTask.Deleted> deleted = Property.ofValue(AbstractDebeziumTask.Deleted.ADD_FIELD);

    @Builder.Default
    protected Property<String> deletedFieldName = Property.ofValue("deleted");

    @Builder.Default
    protected Property<AbstractDebeziumTask.Key> key = Property.ofValue(AbstractDebeziumTask.Key.ADD_FIELD);

    @Builder.Default
    protected Property<AbstractDebeziumTask.Metadata> metadata = Property.ofValue(AbstractDebeziumTask.Metadata.ADD_FIELD);

    @Builder.Default
    protected Property<String> metadataFieldName = Property.ofValue("metadata");

    @Builder.Default
    protected Property<AbstractDebeziumTask.SplitTable> splitTable = Property.ofValue(AbstractDebeziumTask.SplitTable.TABLE);

    @Builder.Default
    protected Property<Boolean> ignoreDdl = Property.ofValue(true);

    protected Property<String> hostname;

    protected Property<String> port;

    protected Property<String> username;

    protected Property<String> password;

    protected Object includedDatabases;

    protected Object excludedDatabases;

    protected Object includedTables;

    protected Object excludedTables;

    protected Object includedColumns;

    protected Object excludedColumns;

    protected Property<Map<String, String>> properties;

    @Builder.Default
    protected Property<String> stateName = Property.ofValue("debezium-state");

    @Schema(
        title = "How to commit the offsets to the KV Store.",
        description = """
            Possible values are:
            - ON_EACH_BATCH: after each batch of records consumed by this trigger, the offsets will be stored in the KV Store. This avoids any duplicated records being consumed but can be costly if many events are produced.
            - ON_STOP: when this trigger is stopped or killed, the offsets will be stored in the KV Store. This avoid any un-necessary writes to the KV Store, but if the trigger is not stopped gracefully, the KV Store value may not be updated leading to duplicated records consumption."""
    )
    @Builder.Default
    private Property<OffsetCommitMode> offsetsCommitMode = Property.ofValue(OffsetCommitMode.ON_EACH_BATCH);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicReference<DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>>> engineReference = new AtomicReference<>();

    public Publisher<AbstractDebeziumRealtimeTrigger.StreamOutput> publisher(AbstractDebeziumTask task, RunContext runContext) {
        return Flux.create(sink -> {
                try {
                    // restore state
                    Path offsetFile = runContext.workingDir().path().resolve("offsets.dat");
                    restoreStateFromKv(runContext, task, offsetFile, "offsets.dat");

                    // database history
                    Path historyFile = runContext.workingDir().path().resolve("dbhistory.dat");
                    if (task.needDatabaseHistory()) {
                        restoreStateFromKv(runContext, task, historyFile, "dbhistory.dat");
                    }

                    // props
                    final Properties props = task.properties(runContext, offsetFile, historyFile);

                    // callback
                    ChangeConsumer changeConsumer = new ChangeConsumer(task, runContext, new AtomicInteger(), null, ZonedDateTime.now());

                    // start
                    var offsetMode = runContext.render(offsetsCommitMode).as(OffsetCommitMode.class).orElseThrow();
                    var engineBuilder = DebeziumEngine.create(Connect.class)
                        .using(this.getClass().getClassLoader())
                        .using(props)
                        .notifying(
                            (list, recordCommitter) -> {
                                changeConsumer.handleBatch(list, recordCommitter, sink);
                                if (OffsetCommitMode.ON_EACH_BATCH.equals(offsetMode)) {
                                    try {
                                        saveOffsets(task, runContext, offsetFile, historyFile);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            }
                        )
                        .using((success, message, error) -> {
                            if (error != null) {
                                sink.error(error);
                            }
                        });

                    try (DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = engineBuilder.build()) {
                        engineReference.set(engine);
                        engine.run();
                    }

                    if (OffsetCommitMode.ON_STOP.equals(offsetMode)) {
                        saveOffsets(task, runContext, offsetFile, historyFile);
                    }
                } catch (Exception e) {
                    sink.error(e);
                } finally {
                    sink.complete();
                }
            });
    }

    private static void restoreStateFromKv(RunContext runContext, AbstractDebeziumTask task, Path targetFile, String filename) throws IOException{
        try {
            String taskRunValue = runContext.storage().getTaskStorageContext().map(StorageContext.Task::getTaskRunValue).orElse(null);

            String stateName = runContext.render(task.stateName).as(String.class).orElseThrow();

            String kvKey = computeKvStoreKey(runContext, stateName, filename, taskRunValue);

            KVStore kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
            Optional<KVValue> kvValue = kvStore.getValue(kvKey);

            if (kvValue.isPresent() && kvValue.get().value() != null) {
                byte[] stateData = (byte[]) kvValue.get().value();
                Files.write(targetFile, stateData);
            }

        } catch (IllegalVariableEvaluationException | ResourceExpiredException e) {
            throw new RuntimeException(e);
        }

    }

    private static void saveOffsets(AbstractDebeziumTask task, RunContext runContext, Path offsetFile, Path historyFile) throws IOException {
        String taskRunValue = runContext.storage().getTaskStorageContext().map(StorageContext.Task::getTaskRunValue).orElse(null);

        KVStore kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());

        if (offsetFile.toFile().exists()) {
            try (FileInputStream fis = new FileInputStream(offsetFile.toFile())) {
               String stateName = runContext.render(task.stateName).as(String.class).orElseThrow();

               String kvKey = computeKvStoreKey(
                   runContext,
                   stateName,
                   offsetFile.getFileName().toString(),
                   taskRunValue
               );

               kvStore.put(kvKey,new KVValueAndMetadata(null, fis.readAllBytes()));
            } catch (IllegalVariableEvaluationException e) {
                throw new RuntimeException(e);
            }
        }

        if (task.needDatabaseHistory() && historyFile.toFile().exists()) {
            try (FileInputStream fis = new FileInputStream(historyFile.toFile())) {
                String stateName = runContext.render(task.stateName).as(String.class).orElseThrow();

                String kvKey = computeKvStoreKey(
                    runContext,
                    stateName,
                    historyFile.getFileName().toString(),
                    taskRunValue
                );

                kvStore.put(kvKey,new KVValueAndMetadata(null, fis.readAllBytes()));;
            } catch (IllegalVariableEvaluationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static String computeKvStoreKey(RunContext runContext, String stateName, String filename, String taskRunValue) throws IllegalVariableEvaluationException{

        String separator = "_";
        boolean hashTaskRunValue = taskRunValue != null;

        String flowId = runContext.flowInfo().id();

        // KV is always flow-scoped as it's bound to a single task, not shared by multiple tasks inside a namespace
        String flowIdPrefix = (flowId == null) ? "" : (Slugify.of(flowId) + separator);
        String prefix = flowIdPrefix + "states" + separator + stateName;

        if (taskRunValue != null) {
            String taskRunSuffix = hashTaskRunValue ? Hashing.hashToString(taskRunValue) : taskRunValue;
            prefix = prefix + separator + taskRunSuffix;
        }

        // Append filename
        return prefix + separator + filename;
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        Optional.ofNullable(engineReference.get()).ifPresent(engine -> {
            try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
                executorService.execute(() -> {
                    try {
                        engine.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                if (wait) {
                    try {
                        if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                            executorService.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
    }

    @Builder
    @Getter
    public static class StreamOutput implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Stream.", description = "Stream source")
        private String stream;

        @Schema(title = "Data.", description = "Data extracted.")
        private Map<String, Object> data;

    }

    public enum OffsetCommitMode {
        ON_EACH_BATCH,
        ON_STOP
    }
}
