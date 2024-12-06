package io.kestra.plugin.debezium;

import ch.qos.logback.classic.LoggerContext;
import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.ExecutorsUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.kestra.core.runners.DefaultRunContext;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDebeziumTask extends Task implements RunnableTask<AbstractDebeziumTask.Output>, AbstractDebeziumInterface {
    @Builder.Default
    protected Property<Format> format = Property.of(Format.INLINE);

    @Builder.Default
    protected Property<Deleted> deleted = Property.of(Deleted.ADD_FIELD);

    @Builder.Default
    protected Property<String> deletedFieldName = Property.of("deleted");

    @Builder.Default
    protected Property<Key> key = Property.of(Key.ADD_FIELD);

    @Builder.Default
    protected Property<Metadata> metadata = Property.of(Metadata.ADD_FIELD);

    @Builder.Default
    protected Property<String> metadataFieldName = Property.of("metadata");

    @Builder.Default
    protected Property<SplitTable> splitTable = Property.of(SplitTable.TABLE);

    @Builder.Default
    protected Property<Boolean> ignoreDdl = Property.of(true);

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
    protected Property<String> stateName = Property.of("debezium-state");

    @Schema(
        title = "The maximum number of rows to fetch before stopping.",
        description = "It's not an hard limit and is evaluated every second."
    )
    @PluginProperty
    private Property<Integer> maxRecords;

    @Schema(
        title = "The maximum duration waiting for new rows.",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    private Property<Duration> maxDuration;

    @Schema(
        title = "The maximum total processing duration.",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    @PluginProperty
    @Builder.Default
    private Property<Duration> maxWait = Property.of(Duration.ofSeconds(10));

    @Schema(
        title = "The maximum duration waiting for the snapshot to ends.",
        description = "It's not an hard limit and is evaluated every second.\n The properties 'maxRecord', 'maxDuration' and 'maxWait' are evaluated only after the snapshot is done."
    )
    @Builder.Default
    private Property<Duration> maxSnapshotDuration = Property.of(Duration.ofHours(1));

    protected abstract boolean needDatabaseHistory();

    static {
        // https://issues.redhat.com/browse/DBZ-4904

        LoggerFactory.getLogger("org.apache.kafka.connect");
        LoggerFactory.getLogger("io.debezium");

        ((LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory())
            .getLoggerList()
            .stream()
            .filter(logger -> logger.getName().startsWith("org.apache.kafka.connect") ||
                logger.getName().startsWith("io.debezium")
            )
            .forEach(
                logger -> logger.setLevel(ch.qos.logback.classic.Level.ERROR)
            );
    }

    @Override
    public AbstractDebeziumTask.Output run(RunContext runContext) throws Exception {
        ExecutorService executorService = ((DefaultRunContext)runContext).getApplicationContext()
            .getBean(ExecutorsUtils.class)
            .singleThreadExecutor(this.getClass().getSimpleName());

        AtomicInteger count = new AtomicInteger();
        AtomicBoolean snapshot = new AtomicBoolean(false);
        ZonedDateTime lastRecord = ZonedDateTime.now();

        // restore state
        Path offsetFile = runContext.workingDir().path().resolve("offsets.dat");
        this.restoreState(runContext, offsetFile);

        // database history
        Path historyFile = runContext.workingDir().path().resolve("dbhistory.dat");
        if (this.needDatabaseHistory()) {
            this.restoreState(runContext, historyFile);
        }

        // props
        final Properties props = this.properties(runContext, offsetFile, historyFile);

        // callback
        CompletionCallback completionCallback = new CompletionCallback(runContext, executorService);
        ChangeConsumer changeConsumer = new ChangeConsumer(this, runContext, count, snapshot, lastRecord);

        // start
        try (DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = DebeziumEngine.create(Connect.class)
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
                Await.until(() -> {
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

        this.shutdown(runContext.logger(), executorService);

        if (completionCallback.getError() != null) {
            throw new Exception(completionCallback.getError());
        }

        Output.OutputBuilder outputBuilder = Output.builder();

        // outputs state
        if (offsetFile.toFile().exists()) {
            try (FileInputStream fis = new FileInputStream(offsetFile.toFile())) {
                outputBuilder.stateOffsetKey(runContext.stateStore().putState(
                    runContext.render(this.stateName).as(String.class).orElseThrow(),
                    offsetFile.getFileName().toFile().toString(),
                    runContext.storage().getTaskStorageContext().map(StorageContext.Task::getTaskRunValue).orElse(null),
                    fis.readAllBytes()
                ));
            }
        }

        if (this.needDatabaseHistory()) {
            try (FileInputStream fis = new FileInputStream(historyFile.toFile())) {
                outputBuilder.stateHistoryKey(runContext.stateStore().putState(
                    runContext.render(this.stateName).as(String.class).orElseThrow(),
                    historyFile.getFileName().toFile().toString(),
                    runContext.storage().getTaskStorageContext().map(StorageContext.Task::getTaskRunValue).orElse(null),
                    fis.readAllBytes()
                ));
            }
        }

        // records
        outputBuilder
            .uris(
                changeConsumer
                    .getRecords()
                    .entrySet()
                    .stream()
                    .map(throwFunction(e -> {
                        e.getValue().getRight().flush();
                        e.getValue().getRight().close();

                        return new AbstractMap.SimpleEntry<>(
                            e.getKey(),
                            runContext.storage().putFile(e.getValue().getLeft())
                        );

                    }))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );

        // metrics & logs
        changeConsumer.getRecordsCount().forEach((s, atomicInteger) -> {
            runContext.metric(Counter.of("records", atomicInteger.get(), "source" , s));
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
    private boolean ended(ExecutorService executorService, AtomicInteger count, ZonedDateTime start, ZonedDateTime lastRecord, AtomicBoolean snapshot, RunContext runContext) throws IllegalVariableEvaluationException {
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
            InputStream taskStateFile = runContext.stateStore().getState(
                runContext.render(this.stateName).as(String.class).orElseThrow(),
                path.getFileName().toString(),
                runContext.storage().getTaskStorageContext().map(StorageContext.Task::getTaskRunValue).orElse(null)
            );
            FileUtils.copyInputStreamToFile(taskStateFile, path.toFile());
        } catch (FileNotFoundException | ResourceExpiredException ignored) {

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
