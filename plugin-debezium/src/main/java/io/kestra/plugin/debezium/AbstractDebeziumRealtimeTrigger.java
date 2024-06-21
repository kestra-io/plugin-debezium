package io.kestra.plugin.debezium;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Path;
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
    protected AbstractDebeziumTask.Format format = AbstractDebeziumTask.Format.INLINE;

    @Builder.Default
    protected AbstractDebeziumTask.Deleted deleted = AbstractDebeziumTask.Deleted.ADD_FIELD;

    @Builder.Default
    protected String deletedFieldName = "deleted";

    @Builder.Default
    protected AbstractDebeziumTask.Key key = AbstractDebeziumTask.Key.ADD_FIELD;

    @Builder.Default
    protected AbstractDebeziumTask.Metadata metadata = AbstractDebeziumTask.Metadata.ADD_FIELD;

    @Builder.Default
    protected String metadataFieldName = "metadata";

    @Builder.Default
    protected AbstractDebeziumTask.SplitTable splitTable = AbstractDebeziumTask.SplitTable.TABLE;

    @Builder.Default
    protected Boolean ignoreDdl = true;

    protected String hostname;

    protected String port;

    protected String username;

    protected String password;

    protected Object includedDatabases;

    protected Object excludedDatabases;

    protected Object includedTables;

    protected Object excludedTables;

    protected Object includedColumns;

    protected Object excludedColumns;

    protected Map<String, String> properties;

    @Builder.Default
    protected String stateName = "debezium-state";

    @Schema(
        title = "How to commit the offsets to the state store.",
        description = """
            Possible values are:
            - ON_EACH_BATCH: after each batch of records consumed by this trigger, the offsets will be stored in the state store. This avoids any duplicated records being consumed but can be costly if a lot of events are produced.
            - ON_STOP: when this trigger is stopped or killed, the offsets will be stored in the state store. This avoid any un-necessary write to the state store, but if the trigger is not stopped gracefully the state store may not be updated leading to duplicated records consumption."""
    )
    @PluginProperty
    @Builder.Default
    private OffsetCommitMode offsetsCommitMode = OffsetCommitMode.ON_EACH_BATCH;

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
                    task.restoreState(runContext, offsetFile);

                    // database history
                    Path historyFile = runContext.workingDir().path().resolve("dbhistory.dat");
                    if (task.needDatabaseHistory()) {
                        task.restoreState(runContext, historyFile);
                    }

                    // props
                    final Properties props = task.properties(runContext, offsetFile, historyFile);

                    // callback
                    ChangeConsumer changeConsumer = new ChangeConsumer(task, runContext, new AtomicInteger(), null, ZonedDateTime.now());

                    // start
                    var engineBuilder = DebeziumEngine.create(Connect.class)
                        .using(this.getClass().getClassLoader())
                        .using(props)
                        .notifying(
                            (list, recordCommitter) -> {
                                changeConsumer.handleBatch(list, recordCommitter, sink);
                                if (offsetsCommitMode == OffsetCommitMode.ON_EACH_BATCH) {
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

                    if (offsetsCommitMode == OffsetCommitMode.ON_STOP) {
                        saveOffsets(task, runContext, offsetFile, historyFile);
                    }
                } catch (Exception e) {
                    sink.error(e);
                } finally {
                    sink.complete();
                }
            });
    }

    private static void saveOffsets(AbstractDebeziumTask task, RunContext runContext, Path offsetFile, Path historyFile) throws IOException {
        if (offsetFile.toFile().exists()) {
            runContext.storage().putTaskStateFile(offsetFile.toFile(), task.stateName, offsetFile.getFileName().toFile().toString());
        }

        if (task.needDatabaseHistory() && historyFile.toFile().exists()) {
            runContext.storage().putTaskStateFile(historyFile.toFile(), task.stateName, historyFile.getFileName().toFile().toString());
        }
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
            try(ExecutorService executorService = Executors.newSingleThreadExecutor()) {
                executorService.execute(() -> {
                    try {
                        engine.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                executorService.shutdown();

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

        private String stream;

        private Map<String, Object> data;

    }

    public enum OffsetCommitMode {
        ON_EACH_BATCH,
        ON_STOP
    }
}
