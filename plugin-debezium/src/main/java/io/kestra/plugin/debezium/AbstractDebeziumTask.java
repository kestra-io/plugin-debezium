package io.kestra.plugin.debezium;

import ch.qos.logback.classic.LoggerContext;
import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
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
    protected Format format = Format.INLINE;

    @Builder.Default
    protected Deleted deleted = Deleted.ADD_FIELD;

    @Builder.Default
    protected String deletedFieldName = "deleted";

    @Builder.Default
    protected Key key = Key.ADD_FIELD;

    @Builder.Default
    protected Metadata metadata = Metadata.ADD_FIELD;

    @Builder.Default
    protected String metadataFieldName = "metadata";

    @Builder.Default
    protected SplitTable splitTable = SplitTable.TABLE;

    @Builder.Default
    protected Boolean ignoreDdl = true;

    protected String hostname;

    protected String port;

    protected String username;

    protected String password;

    private Object includedDatabases;

    private Object excludedDatabases;

    private Object includedTables;

    private Object excludedTables;

    private Object includedColumns;

    private Object excludedColumns;

    private Map<String, String> properties;

    @Builder.Default
    protected String stateName = "debezium-state";

    @Schema(
        title = "The maximum number of rows to fetch before stopping.",
        description = "It's not an hard limit and is evaluated every second."
    )
    @PluginProperty
    private Integer maxRecords;

    @Schema(
        title = "The maximum duration waiting for new rows.",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    @PluginProperty
    private Duration maxDuration;

    @Schema(
        title = "The maximum total processing duration.",
        description = "It's not an hard limit and is evaluated every second.\n It is taken into account after the snapshot if any."
    )
    @PluginProperty
    @Builder.Default
    private Duration maxWait = Duration.ofSeconds(10);

    @Schema(
        title = "The maximum duration waiting for the snapshot to ends.",
        description = "It's not an hard limit and is evaluated every second.\n The properties 'maxRecord', 'maxDuration' and 'maxWait' are evaluated only after the snapshot is done."
    )
    @PluginProperty
    @Builder.Default
    private Duration maxSnapshotDuration = Duration.ofHours(1);

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
                Await.until(() -> this.ended(executorService, count, captureStarted, lastRecord, snapshot), Duration.ofSeconds(1));
                consumes = count.get() > previousCount;
                // if we are still snapshotting, allow waiting for more time until snapshot wait duration is reached
            } while (snapshot.get() && consumes && ZonedDateTime.now().isBefore(snapshotStarted.plus(this.maxSnapshotDuration)));
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
                    this.stateName,
                    offsetFile.getFileName().toFile().toString(),
                    runContext.storage().getTaskStorageContext().map(StorageContext.Task::getTaskRunValue).orElse(null),
                    fis.readAllBytes()
                ));
            }
        }

        if (this.needDatabaseHistory()) {
            try (FileInputStream fis = new FileInputStream(historyFile.toFile())) {
                outputBuilder.stateHistoryKey(runContext.stateStore().putState(
                    this.stateName,
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
            props.setProperty("database.hostname", runContext.render(this.hostname));
        }

        if (this.port != null) {
            props.setProperty("database.port", runContext.render(this.port));
        }

        if (this.username != null) {
            props.setProperty("database.user", runContext.render(this.username));
        }

        if (this.password != null) {
            props.setProperty("database.password", runContext.render(this.password));
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
            for (Map.Entry<String, String> entry : this.properties.entrySet()) {
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
    private boolean ended(ExecutorService executorService, AtomicInteger count, ZonedDateTime start, ZonedDateTime lastRecord, AtomicBoolean snapshot) {
        if (executorService.isShutdown()) {
            return true;
        }

        // when snapshotting, we didn't take into account maxRecords
        if (!snapshot.get() && this.maxRecords != null && count.get() >= this.maxRecords) {
            return true;
        }

        if (this.maxDuration != null && ZonedDateTime.now().isAfter(start.plus(this.maxDuration))) {
            return true;
        }

        if (this.maxWait != null && ZonedDateTime.now().isAfter(lastRecord.plus(this.maxWait))) {
            return true;
        }

        return false;
    }

    protected void restoreState(RunContext runContext, Path path) throws IOException {
        try {
            InputStream taskStateFile = runContext.stateStore().getState(
                this.stateName,
                path.getFileName().toString(),
                runContext.storage().getTaskStorageContext().map(StorageContext.Task::getTaskRunValue).orElse(null)
            );
            FileUtils.copyInputStreamToFile(taskStateFile, path.toFile());
        } catch (FileNotFoundException | ResourceExpiredException ignored) {

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
            title = "The KV Store key under which the state with offset is stored"
        )
        private String stateOffsetKey;

        @Schema(
            title = "The KV Store key under which the state with database history is stored"
        )
        private String stateHistoryKey;

        @Schema(
            title = "The size of the rows fetch"
        )
        private Integer size;

        @Schema(
            title = "Uri of the generated internal storage file"
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
