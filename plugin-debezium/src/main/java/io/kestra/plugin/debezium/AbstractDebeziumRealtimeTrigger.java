package io.kestra.plugin.debezium;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
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
public abstract class AbstractDebeziumRealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<AbstractDebeziumTask.Output> {
    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicReference<DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>>> engineReference = new AtomicReference<>();

    public Publisher<AbstractDebeziumRealtimeTrigger.StreamOutput> publisher(AbstractDebeziumTask task, RunContext runContext) throws Exception {
        return Flux.create(sink -> {
                try {
                    // restore state
                    Path offsetFile = runContext.tempDir().resolve("offsets.dat");
                    task.restoreState(runContext, offsetFile);

                    // database history
                    Path historyFile = runContext.tempDir().resolve("dbhistory.dat");
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
                            (list, recordCommitter) -> changeConsumer.handleBatch(list, recordCommitter, sink)
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

                    // This may be too late, and we may not be able to write the file.
                    // If the problem occurs, the writing should be done on the stop method.
                    // Another issue is if people kill Kestra,
                    // we may need to periodically save offsets (for ex each 100 rows or 1mn).
                    if (offsetFile.toFile().exists()) {
                        runContext.storage().putTaskStateFile(offsetFile.toFile(), task.stateName, offsetFile.getFileName().toFile().toString());
                    }

                    if (task.needDatabaseHistory()) {
                        runContext.storage().putTaskStateFile(historyFile.toFile(), task.stateName, historyFile.getFileName().toFile().toString());
                    }
                } catch (Exception e) {
                    sink.error(e);
                } finally {
                    sink.complete();
                }
            });
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
}
