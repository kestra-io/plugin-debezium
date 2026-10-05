package io.kestra.plugin.debezium;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceRecord;
import org.reactivestreams.Publisher;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Hashing;
import io.kestra.core.utils.Slugify;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDebeziumRealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<AbstractDebeziumRealtimeTrigger.StreamOutput> {

    private static final Duration DEFAULT_RECONNECT_INITIAL_DELAY = Duration.ofSeconds(1);

    private static final Duration DEFAULT_RECONNECT_MAX_DELAY = Duration.ofMinutes(1);

    /** Spread simultaneous reconnects. Applied only to the wait, not to the logged base delay. */
    private static final double JITTER_FACTOR = 0.2d;

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
        title = "When to commit the offsets to the KV Store",
        description = """
            - `ON_EACH_BATCH`: after each batch of records consumed by this trigger, the offsets will be stored in the KV Store. This avoids any duplicated records being consumed but can be costly if many events are produced.
            - `ON_STOP`: when this trigger is stopped or killed, the offsets will be stored in the KV Store. This avoids any un-necessary writes to the KV Store, but if the trigger is not stopped gracefully, the KV Store value may not be updated leading to duplicated records consumption.
            """
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<OffsetCommitMode> offsetsCommitMode = Property.ofValue(OffsetCommitMode.ON_STOP);

    @Schema(
        title = "Initial wait before reconnecting after the connection fails to start",
        description = """
            Defaults to `PT1S`. Must be positive and less than or equal to `reconnectMaxDelay`.

            The first attempt connects immediately. If the task does not stay up, the trigger waits this long \
            and then doubles the wait on each further failure, up to `reconnectMaxDelay`. The actual wait varies by up to 20% \
            so triggers that fail together do not reconnect at the same moment. The trigger stays running: \
            a failure is logged and retried on this worker, and does not create a failed execution for each attempt. \
            The wait returns to this value only after the task has stayed up for at least `reconnectMaxDelay`.
            """
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Duration> reconnectInitialDelay = Property.ofValue(DEFAULT_RECONNECT_INITIAL_DELAY);

    @Schema(
        title = "Maximum wait between reconnect attempts",
        description = "Defaults to `PT1M`. Must be greater than or equal to `reconnectInitialDelay`."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Duration> reconnectMaxDelay = Property.ofValue(DEFAULT_RECONNECT_MAX_DELAY);

    @Schema(
        title = "Maximum reconnects after the first connection attempt",
        description = """
            Optional. Minimum `0`. Leave empty to reconnect until the database is reachable again.

            How many times to reconnect after the first attempt fails to stay up. \
            `0` connects once. When the limit is reached, the trigger stays subscribed and waits until it is stopped. \
            Ending the stream here would make Kestra start a new worker, and this count would begin again. \
            A task that stays up for at least `reconnectMaxDelay` resets this count. Negative values are rejected when the trigger starts.
            """
    )
    @PluginProperty(group = "reliability")
    private Property<Integer> maxReconnectAttempts;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final Object reconnectMonitor = new Object();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>>> engineReference = new AtomicReference<>();

    public Publisher<AbstractDebeziumRealtimeTrigger.StreamOutput> publisher(AbstractDebeziumTask task, RunContext runContext) throws IllegalVariableEvaluationException {

        var rOffsetsCommitMode = runContext.render(offsetsCommitMode).as(OffsetCommitMode.class).orElse(OffsetCommitMode.ON_STOP);
        var offsetFile = runContext.workingDir().path().resolve(AbstractDebeziumTask.OFFSETS_DATA_FILE);
        var historyFile = runContext.workingDir().path().resolve(AbstractDebeziumTask.DBHISTORY_DATA_FILE);
        // Validate before the Flux exists. A rejected delay fails evaluate() once. Failing the
        // subscription instead would make Kestra schedule a new worker about once a second.
        ReconnectPolicy reconnectPolicy = reconnectPolicy(runContext);

        return Flux.create(sink ->
        {
            sink.onCancel(this::cancelPublisher);
            try {
                task.restoreState(runContext, offsetFile, historyFile);

                var identity = task.resolveEffectiveIdentity(runContext);
                AbstractDebeziumTask.migrateOffsetFile(runContext.logger(), offsetFile, identity.name(), identity.topicPrefix());
                if (task.needDatabaseHistory()) {
                    AbstractDebeziumTask.migrateHistoryFile(runContext.logger(), historyFile, identity.topicPrefix());
                }

                final Properties props = task.properties(runContext, offsetFile, historyFile);
                ChangeConsumer changeConsumer = new ChangeConsumer(task, runContext, new AtomicInteger(), null, ZonedDateTime.now(), offsetFile, historyFile);

                ReconnectLoop.run(
                    reconnectPolicy,
                    new EngineReconnectControl(sink, runContext, reconnectPolicy),
                    () -> runOneEngine(task, runContext, props, changeConsumer, sink, rOffsetsCommitMode, offsetFile, historyFile, reconnectPolicy)
                );
            } catch (Exception e) {
                sink.error(e);
            } finally {
                if (rOffsetsCommitMode == OffsetCommitMode.ON_STOP) {
                    try {
                        task.saveFinalState(runContext, offsetFile, historyFile);
                    } catch (IOException e) {
                        sink.error(new RuntimeException(e));
                    }
                }

                sink.complete();
            }
        });
    }

    private ReconnectPolicy reconnectPolicy(RunContext runContext) throws IllegalVariableEvaluationException {
        var rInitialDelay = reconnectInitialDelay == null
            ? DEFAULT_RECONNECT_INITIAL_DELAY
            : runContext.render(reconnectInitialDelay).as(Duration.class).orElse(DEFAULT_RECONNECT_INITIAL_DELAY);
        var rMaxDelay = reconnectMaxDelay == null
            ? DEFAULT_RECONNECT_MAX_DELAY
            : runContext.render(reconnectMaxDelay).as(Duration.class).orElse(DEFAULT_RECONNECT_MAX_DELAY);
        var rMaxAttempts = maxReconnectAttempts == null
            ? null
            : runContext.render(maxReconnectAttempts).as(Integer.class).orElse(null);
        return ReconnectPolicy.of(rInitialDelay, rMaxDelay, rMaxAttempts);
    }

    private ReconnectLoop.AttemptResult runOneEngine(
        AbstractDebeziumTask task,
        RunContext runContext,
        Properties props,
        ChangeConsumer changeConsumer,
        FluxSink<StreamOutput> sink,
        OffsetCommitMode offsetsCommitMode,
        Path offsetFile,
        Path historyFile,
        ReconnectPolicy reconnectPolicy) {
        var taskStartedAt = new AtomicLong();
        var completionError = new AtomicReference<Throwable>();
        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;
        try {
            engine = DebeziumEngine.create(Connect.class)
                .using(this.getClass().getClassLoader())
                .using(props)
                .notifying((list, recordCommitter) ->
                {
                    changeConsumer.handleBatch(list, recordCommitter, sink, offsetsCommitMode);
                    if (offsetsCommitMode == OffsetCommitMode.ON_EACH_BATCH) {
                        try {
                            saveOffsets(task, runContext, offsetFile, historyFile);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .using(new DebeziumEngine.ConnectorCallback() {
                    @Override
                    public void taskStarted() {
                        taskStartedAt.compareAndSet(0, System.nanoTime());
                    }
                })
                .using((success, message, error) ->
                {
                    if (error != null) {
                        completionError.compareAndSet(null, error);
                    }
                })
                .build();
        } catch (Exception e) {
            Throwable error = completionError.get();
            return ReconnectLoop.AttemptResult.failure(error != null ? error : e, false);
        }

        synchronized (reconnectMonitor) {
            if (!isActive.get() || sink.isCancelled()) {
                closeQuietly(engine);
                return ReconnectLoop.AttemptResult.stopped();
            }
            engineReference.set(engine);
        }

        if (!isActive.get() || sink.isCancelled()) {
            releaseEngine(engine);
            return ReconnectLoop.AttemptResult.stopped();
        }

        try {
            engine.run();
        } catch (Exception e) {
            completionError.compareAndSet(null, e);
        } finally {
            releaseEngine(engine);
        }

        if (!isActive.get() || sink.isCancelled()) {
            return ReconnectLoop.AttemptResult.stopped();
        }

        Throwable error = completionError.get();
        if (error == null) {
            error = new IllegalStateException("Debezium engine stopped while the realtime trigger was still active");
        }
        boolean stable = ReconnectPolicy.ranStably(taskStartedAt.get(), System.nanoTime(), reconnectPolicy.maxDelay());
        return ReconnectLoop.AttemptResult.failure(error, stable);
    }

    private void releaseEngine(DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine) {
        boolean owned;
        synchronized (reconnectMonitor) {
            owned = engineReference.compareAndSet(engine, null);
        }
        if (owned) {
            closeQuietly(engine);
        }
    }

    private static void closeQuietly(DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine) {
        try {
            engine.close();
        } catch (IllegalStateException alreadyShutDown) {
            // stop() and the attempt loop can both observe an engine that has already stopped.
        } catch (IOException closeFailure) {
            // Closing is best-effort. A second close must not fail the subscription.
        }
    }

    /**
     * @return {@code false} when the trigger was stopped or the wait was interrupted
     */
    boolean awaitBackoff(Duration delay) {
        long deadline = System.nanoTime() + delay.toNanos();
        synchronized (reconnectMonitor) {
            while (isActive.get()) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    return isActive.get();
                }
                long waitMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
                if (waitMillis <= 0) {
                    waitMillis = 1;
                }
                try {
                    reconnectMonitor.wait(waitMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return false;
        }
    }

    private void awaitUntilStopped() {
        synchronized (reconnectMonitor) {
            while (isActive.get()) {
                try {
                    reconnectMonitor.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void cancelPublisher() {
        stop(false);
    }

    private static void saveOffsets(AbstractDebeziumTask task, RunContext runContext, Path offsetFile, Path historyFile) throws IOException {
        task.saveStateAtomically(runContext, offsetFile, historyFile);
    }

    public static String computeKvStoreKey(RunContext runContext, String stateName, String filename, String taskRunValue) throws IllegalVariableEvaluationException {

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
        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;
        synchronized (reconnectMonitor) {
            if (!isActive.compareAndSet(true, false)) {
                return;
            }
            engine = engineReference.getAndSet(null);
            reconnectMonitor.notifyAll();
        }
        if (engine == null) {
            return;
        }

        // Do not use try-with-resources: ExecutorService.close() waits for the task.
        // stop() also runs on the engine thread when the subscription is cancelled, and joining
        // close() from that thread deadlocks the engine shutdown.
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
        executorService.execute(() -> closeQuietly(engine));
        executorService.shutdown();
        if (!wait) {
            return;
        }
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private final class EngineReconnectControl implements ReconnectLoop.Control {
        private final FluxSink<StreamOutput> sink;
        private final RunContext runContext;
        private final ReconnectPolicy reconnectPolicy;

        private EngineReconnectControl(FluxSink<StreamOutput> sink, RunContext runContext, ReconnectPolicy reconnectPolicy) {
            this.sink = sink;
            this.runContext = runContext;
            this.reconnectPolicy = reconnectPolicy;
        }

        @Override
        public boolean keepGoing() {
            return isActive.get() && !sink.isCancelled();
        }

        @Override
        public boolean awaitBackoff(Duration delay) {
            double factor = ThreadLocalRandom.current().nextDouble(-JITTER_FACTOR, JITTER_FACTOR);
            return AbstractDebeziumRealtimeTrigger.this.awaitBackoff(ReconnectPolicy.withJitter(delay, factor));
        }

        @Override
        public void onRetry(Throwable error, ReconnectPolicy.Retry retry) {
            if (retry.firstOfStreak() && error != null) {
                runContext.logger().warn(
                    "Debezium realtime trigger failed (attempt {}); reconnecting in {}. The trigger stays running and retries on this worker.",
                    retry.attempt(),
                    retry.delay(),
                    error
                );
            } else if (retry.firstOfStreak()) {
                runContext.logger().warn(
                    "Debezium realtime trigger failed (attempt {}); reconnecting in {}. The trigger stays running and retries on this worker.",
                    retry.attempt(),
                    retry.delay()
                );
            } else {
                runContext.logger().warn(
                    "Debezium realtime trigger failed (attempt {}); reconnecting in {}: {}",
                    retry.attempt(),
                    retry.delay(),
                    error == null ? "unknown error" : error.toString()
                );
            }
        }

        @Override
        public void onAttemptsExhausted(Throwable error) {
            if (error != null) {
                runContext.logger().error(
                    "Debezium realtime trigger failed to connect and maxReconnectAttempts ({}) is exhausted. Waiting until the trigger is stopped.",
                    reconnectPolicy.maxReconnectAttempts(),
                    error
                );
            } else {
                runContext.logger().error(
                    "Debezium realtime trigger failed to connect and maxReconnectAttempts ({}) is exhausted. Waiting until the trigger is stopped.",
                    reconnectPolicy.maxReconnectAttempts()
                );
            }
        }

        @Override
        public void parkUntilStopped() {
            awaitUntilStopped();
        }
    }

    @Builder
    @Getter
    public static class StreamOutput implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Stream", description = "Stream source")
        @PluginProperty(group = "advanced")
        private String stream;

        @Schema(title = "Data", description = "Data extracted.")
        @PluginProperty(group = "advanced")
        private Map<String, Object> data;
    }

    public enum OffsetCommitMode {
        ON_EACH_BATCH,
        ON_STOP
    }
}
