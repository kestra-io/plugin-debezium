package io.kestra.plugin.debezium;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import jakarta.inject.Inject;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class DebeziumLifecycleTest {

    @Inject
    private RunContextFactory runContextFactory;

    @SuperBuilder
    @NoArgsConstructor
    public static class DummyTask extends AbstractDebeziumTask {
        @Override
        protected boolean needDatabaseHistory() {
            return false;
        }
    }

    @Test
    void changeConsumerUpdatesSharedLastRecordAndTaskStarted() {
        var task = DummyTask.builder().build();
        var runContext = runContextFactory.of();
        var count = new AtomicInteger();
        var snapshot = new AtomicBoolean(false);
        var lastRecord = new AtomicReference<>(ZonedDateTime.now().minusMinutes(5));
        var taskStarted = new AtomicBoolean(false);

        var consumer = new ChangeConsumer(
            task, runContext, count, snapshot, lastRecord, taskStarted, Path.of("offsets"), Path.of("history")
        );

        assertThat(consumer.getTaskStarted().get(), is(false));
        var initialLastRecord = lastRecord.get();

        var schema = SchemaBuilder.struct().name("test").field("id", Schema.INT32_SCHEMA).build();
        var struct = new Struct(schema).put("id", 1);
        var sourceRecord = new SourceRecord(
            Map.of("snapshot", false), Map.of(), "topic", null, null, null, schema, struct
        );

        consumer.handleBatch(List.of(new TestChangeEvent(sourceRecord)), new NoopCommitter());

        assertThat(consumer.getTaskStarted().get(), is(true));
        assertThat(taskStarted.get(), is(true));
        assertThat(lastRecord.get().isAfter(initialLastRecord), is(true));
    }

    @Test
    void changeConsumerRejectsNullLastRecordOrTaskStarted() {
        var task = DummyTask.builder().build();
        var runContext = runContextFactory.of();
        var count = new AtomicInteger();
        var snapshot = new AtomicBoolean(false);
        var lastRecord = new AtomicReference<>(ZonedDateTime.now());
        var taskStarted = new AtomicBoolean(false);

        assertThrows(NullPointerException.class, () -> new ChangeConsumer(
            task, runContext, count, snapshot, null, taskStarted, Path.of("offsets"), Path.of("history")
        ));

        assertThrows(NullPointerException.class, () -> new ChangeConsumer(
            task, runContext, count, snapshot, lastRecord, null, Path.of("offsets"), Path.of("history")
        ));
    }

    @Test
    void endedDoesNotTimeoutOnMaxWaitBeforeTaskStarted() throws Exception {
        var task = DummyTask.builder()
            .maxWait(Property.ofValue(Duration.ofSeconds(10)))
            .build();
        var runContext = runContextFactory.of();

        var executor = Executors.newSingleThreadExecutor();
        try {
            var count = new AtomicInteger(0);
            var start = ZonedDateTime.now().minusSeconds(30);
            var lastRecord = new AtomicReference<>(ZonedDateTime.now().minusSeconds(20));
            var taskStarted = new AtomicBoolean(false);
            var snapshot = new AtomicBoolean(false);

            var ended = task.ended(executor, count, start, lastRecord, taskStarted, snapshot, runContext);
            assertThat(ended, is(false));

            taskStarted.set(true);
            ended = task.ended(executor, count, start, lastRecord, taskStarted, snapshot, runContext);
            assertThat(ended, is(true));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void endedExtendsWaitWhenNewRecordsArrive() throws Exception {
        var task = DummyTask.builder()
            .maxWait(Property.ofValue(Duration.ofSeconds(10)))
            .build();
        var runContext = runContextFactory.of();

        var executor = Executors.newSingleThreadExecutor();
        try {
            var count = new AtomicInteger(1);
            var start = ZonedDateTime.now().minusSeconds(25);
            var lastRecord = new AtomicReference<>(ZonedDateTime.now());
            var taskStarted = new AtomicBoolean(true);
            var snapshot = new AtomicBoolean(false);

            var ended = task.ended(executor, count, start, lastRecord, taskStarted, snapshot, runContext);
            assertThat(ended, is(false));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void endedHonorsMaxRecordsAndMaxDuration() throws Exception {
        var task = DummyTask.builder()
            .maxRecords(Property.ofValue(5))
            .maxDuration(Property.ofValue(Duration.ofSeconds(30)))
            .maxWait(Property.ofValue(Duration.ofSeconds(10)))
            .build();
        var runContext = runContextFactory.of();

        var executor = Executors.newSingleThreadExecutor();
        try {
            var count = new AtomicInteger(5);
            var start = ZonedDateTime.now().minusSeconds(5);
            var lastRecord = new AtomicReference<>(ZonedDateTime.now());
            var taskStarted = new AtomicBoolean(true);
            var snapshot = new AtomicBoolean(false);

            assertThat(task.ended(executor, count, start, lastRecord, taskStarted, snapshot, runContext), is(true));

            snapshot.set(true);
            assertThat(task.ended(executor, count, start, lastRecord, taskStarted, snapshot, runContext), is(false));
            snapshot.set(false);

            count.set(2);
            var oldStart = ZonedDateTime.now().minusSeconds(35);
            assertThat(task.ended(executor, count, oldStart, lastRecord, taskStarted, snapshot, runContext), is(true));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void endedReturnsTrueImmediatelyWhenExecutorShutdown() throws Exception {
        var task = new DummyTask();
        var runContext = runContextFactory.of();

        var executor = Executors.newSingleThreadExecutor();
        executor.shutdown();

        var ended = task.ended(
            executor,
            new AtomicInteger(0),
            ZonedDateTime.now(),
            new AtomicReference<>(ZonedDateTime.now()),
            new AtomicBoolean(false),
            new AtomicBoolean(false),
            runContext
        );

        assertThat(ended, is(true));
    }

    @Test
    void isShutdownArtifactIdentifiesExpectedShutdownExceptions() {
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new InterruptedException("sleep interrupted")), is(true));

        var mbeanInterruption = new RuntimeException("Unable to register the MBean", new InterruptedException("sleep interrupted"));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(mbeanInterruption), is(true));

        var alreadyShutDown = new IllegalStateException("Engine has been already shut down");
        assertThat(AbstractDebeziumTask.isShutdownArtifact(alreadyShutDown), is(true));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new IllegalStateException("Engine is already being shutting down.")), is(true));

        assertThat(AbstractDebeziumTask.isShutdownArtifact(new RuntimeException("Generic unhandled error")), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new IllegalStateException("Table not found")), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new IllegalArgumentException("Invalid configuration property")), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new RuntimeException("Authentication failed")), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new RuntimeException("Authentication failed", new InterruptedException("sleep interrupted"))), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new RuntimeException("Invalid connection string URI: mongodb://", new IllegalArgumentException())), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new java.net.UnknownHostException("mongodb.invalid.domain")), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new IOException("Connection refused")), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new java.sql.SQLException("Login failed for user")), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new org.apache.kafka.connect.errors.ConnectException("Query failed", new java.sql.SQLException("syntax error"))), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new org.apache.kafka.connect.errors.ConnectException("Task failed to start")), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new RuntimeException("Connector failed", new SecurityException("Bad credentials"))), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new RuntimeException("Unexpected internal null pointer", new NullPointerException())), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new RuntimeException("Error while processing batch", new InterruptedException("sleep interrupted"))), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new org.apache.kafka.connect.errors.ConnectException("Failed to commit offsets", new InterruptedException("sleep interrupted"))), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(new org.apache.kafka.connect.errors.ConnectException("Error committing offsets", new InterruptedException("sleep interrupted"))), is(false));
        assertThat(AbstractDebeziumTask.isShutdownArtifact(null), is(false));
    }

    @Test
    void closeQuietlyHandlesAlreadyShutdownEngineWithoutThrowing() {
        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> mockEngine = new DebeziumEngine<>() {
            @Override
            public void run() {
            }

            @Override
            public void close() {
                throw new IllegalStateException("Engine has been already shut down");
            }
        };

        assertDoesNotThrow(() -> AbstractDebeziumTask.closeQuietly(mockEngine));

        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> unexpectedFailEngine = new DebeziumEngine<>() {
            @Override
            public void run() {
            }

            @Override
            public void close() {
                throw new IllegalStateException("Unexpected internal failure");
            }
        };

        assertThrows(IllegalStateException.class, () -> AbstractDebeziumTask.closeQuietly(unexpectedFailEngine));

        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> ioFailEngine = new DebeziumEngine<>() {
            @Override
            public void run() {
            }

            @Override
            public void close() throws IOException {
                throw new IOException("I/O failure during close");
            }
        };

        assertThrows(RuntimeException.class, () -> AbstractDebeziumTask.closeQuietly(ioFailEngine));
    }

    @Test
    void closeBestEffortSwallowsUnexpectedErrorsAndLogs() {
        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> unexpectedFailEngine = new DebeziumEngine<>() {
            @Override
            public void run() {
            }

            @Override
            public void close() {
                throw new IllegalStateException("Unexpected internal failure");
            }
        };

        assertDoesNotThrow(() -> AbstractDebeziumTask.closeBestEffort(unexpectedFailEngine, null));

        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> ioFailEngine = new DebeziumEngine<>() {
            @Override
            public void run() {
            }

            @Override
            public void close() throws IOException {
                throw new IOException("I/O failure during close");
            }
        };

        assertDoesNotThrow(() -> AbstractDebeziumTask.closeBestEffort(ioFailEngine, null));
        assertDoesNotThrow(() -> AbstractDebeziumTask.closeBestEffort(null, null));
    }

    private record TestChangeEvent(SourceRecord value) implements ChangeEvent<SourceRecord, SourceRecord> {
        @Override
        public SourceRecord key() {
            return null;
        }

        @Override
        public String destination() {
            return value.topic();
        }

        @Override
        public Integer partition() {
            return null;
        }
    }

    private static class NoopCommitter implements DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> {
        @Override
        public void markProcessed(ChangeEvent<SourceRecord, SourceRecord> record) {
        }

        @Override
        public void markBatchFinished() {
        }

        @Override
        public void markProcessed(ChangeEvent<SourceRecord, SourceRecord> record, DebeziumEngine.Offsets offsets) {
        }

        @Override
        public DebeziumEngine.Offsets buildOffsets() {
            return null;
        }
    }
}
