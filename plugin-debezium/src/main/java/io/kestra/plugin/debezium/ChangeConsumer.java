package io.kestra.plugin.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.debezium.models.Envelope;
import io.kestra.plugin.debezium.models.Message;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.source.SourceRecord;
import reactor.core.publisher.FluxSink;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {
    private final AbstractDebeziumTask abstractDebeziumTask;

    private final RunContext runContext;

    private final AtomicInteger count;
    private final AtomicBoolean snapshot;

    @SuppressWarnings("unused")
    private ZonedDateTime lastRecord;

    private final Path offsetFile;
    private final Path historyFile;

    @Getter
    private final Map<String, Pair<File, OutputStream>> records = new HashMap<>();

    @Getter
    private final Map<String, AtomicInteger> recordsCount = new ConcurrentHashMap<>();

    public ChangeConsumer(AbstractDebeziumTask abstractDebeziumTask, RunContext runContext, AtomicInteger count, AtomicBoolean snapshot, ZonedDateTime lastRecord, Path offsetFile, Path historyFile) {
        this.abstractDebeziumTask = abstractDebeziumTask;
        this.runContext = runContext;
        this.count = count;
        this.snapshot = snapshot;
        this.lastRecord = lastRecord;
        this.offsetFile = offsetFile;
        this.historyFile = historyFile;
    }

    @SneakyThrows
    @Override
    public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) {

        var rOffsetsCommitMode = runContext.render(abstractDebeziumTask.getOffsetsCommitMode()).as(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.class)
            .orElse(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_STOP);

        lastRecord = ZonedDateTime.now();

        for (ChangeEvent<SourceRecord, SourceRecord> r : records) {
            SourceRecord record = r.value();
            if (record.sourceOffset().containsKey("snapshot") && record.sourceOffset().get("snapshot").equals(Boolean.TRUE)) {
                snapshot.compareAndSet(false, true);
            } else {
                snapshot.compareAndSet(true, false);
            }

            Pair<Message, Message> message = MapConverter.convert(record);

            Map<String, Object> result = this.handle(message);

            if (result != null) {
                this.write(result, message.getValue().getSource());
            }

            committer.markProcessed(r);
        }

        committer.markBatchFinished();

        // Save offsets after batch if configured
        if (rOffsetsCommitMode == AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_EACH_BATCH) {
            abstractDebeziumTask.saveOffsetsForTask(runContext, offsetFile, historyFile);
        }
    }

    public void handleBatch(
        List<ChangeEvent<SourceRecord, SourceRecord>> records,
        DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer,
        FluxSink<AbstractDebeziumRealtimeTrigger.StreamOutput> sink,
        AbstractDebeziumRealtimeTrigger.OffsetCommitMode offsetCommitMode
    ) {
        lastRecord = ZonedDateTime.now();

        try {
            for (ChangeEvent<SourceRecord, SourceRecord> r : records) {
                SourceRecord record = r.value();

                Pair<Message, Message> message = MapConverter.convert(record);

                Map<String, Object> result = this.handle(message);

                if (result != null) {
                    this.emit(result, message.getValue().getSource(), sink);
                }

                committer.markProcessed(r);
            }

            committer.markBatchFinished();

            // Save offsets after batch if configured
            if (offsetCommitMode == AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_EACH_BATCH) {
                abstractDebeziumTask.saveOffsetsForTask(runContext, offsetFile, historyFile);
            }
        } catch (Exception exception) {
            sink.error(exception);
        }
    }

    private Map<String, Object> handle(Pair<Message, Message> message) throws IllegalVariableEvaluationException {
        if (this.isFilter(message)) {
            return null;
        }

        switch (runContext.render(abstractDebeziumTask.getFormat()).as(AbstractDebeziumTask.Format.class).orElseThrow()) {
            case RAW:
                return this.handleFormatRaw(message);
            case INLINE:
                return this.handleFormatInline(message);
            case WRAP:
                return this.handleFormatWrap(message);
            default:
                throw new IllegalArgumentException("Invalid Format '" + this.abstractDebeziumTask.getFormat() + "");
        }
    }

    private void emit(Map<String, Object> result, Message.Source source, FluxSink<AbstractDebeziumRealtimeTrigger.StreamOutput> sink) throws IllegalVariableEvaluationException {
        String stream = switch (runContext.render(this.abstractDebeziumTask.getSplitTable()).as(AbstractDebeziumTask.SplitTable.class).orElseThrow()) {
            case OFF -> "data";
            case TABLE -> source.getDb() + "." + source.getTable();
            case DATABASE -> source.getDb();
        };

        AbstractDebeziumRealtimeTrigger.StreamOutput output = AbstractDebeziumRealtimeTrigger.StreamOutput.builder()
            .stream(stream)
            .data(result)
            .build();

        sink.next(output);
    }

    private void write(Map<String, Object> result, Message.Source source) throws IOException, IllegalVariableEvaluationException {
        String stream;

        switch (runContext.render(this.abstractDebeziumTask.getSplitTable()).as(AbstractDebeziumTask.SplitTable.class).orElseThrow()) {
            case OFF:
                stream = "data";
                break;
            case TABLE:
                stream = source.getDb() + "." + source.getTable();
                break;
            case DATABASE:
                stream = source.getDb();
                break;
            default:
                throw new IllegalArgumentException("Invalid SplitTable '" + this.abstractDebeziumTask.getSplitTable().toString() + "");
        }

        if (!this.records.containsKey(stream)) {
            Path tempFile = runContext.workingDir().createTempFile(stream);
            this.records.put(stream, Pair.of(tempFile.toFile(), new FileOutputStream(tempFile.toFile())));
        }

        this.recordsCount.computeIfAbsent(stream, k -> new AtomicInteger()).incrementAndGet();

        int saved = count.incrementAndGet();

        if (saved > 0 && saved % 5000 == 0) {
            runContext.logger().debug("Received {} records: {}", count, this.recordsCount);
        }

        FileSerde.write(this.records.get(stream).getRight(), result);
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean isFilter(Pair<Message, Message> message) throws IllegalVariableEvaluationException {
        if (!(message.getValue() instanceof Envelope) && runContext.render(abstractDebeziumTask.getIgnoreDdl()).as(Boolean.class).orElseThrow()) {
            return true;
        }

        if (message.getValue() == null && runContext.render(abstractDebeziumTask.getDeleted()).as(AbstractDebeziumTask.Deleted.class).orElseThrow() == AbstractDebeziumTask.Deleted.DROP) {
            return true;
        }

        if (!(message.getValue() instanceof Envelope) && runContext.render(this.abstractDebeziumTask.getFormat()).as(AbstractDebeziumTask.Format.class).orElseThrow() != AbstractDebeziumTask.Format.RAW) {
            return true;
        }

        return false;
    }

    private Map<String, Object> handleFormatRaw(Pair<Message, Message> message) throws IllegalVariableEvaluationException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", message.getKey());
        result.put("value", message.getValue());

        this.addDeleted(result, message);

        return result;
    }

    private Map<String, Object> handleFormatInline(Pair<Message, Message> message) throws IllegalVariableEvaluationException {
        Envelope value = (Envelope) message.getValue();

        Map<String, Object> result = this.formatInlineWithoutAdditional(value);

        this.addDeleted(result, message);
        this.addKey(result, message);
        this.addMetadata(result, value);

        return result;
    }

    private Map<String, Object> handleFormatWrap(Pair<Message, Message> message) throws IllegalVariableEvaluationException {
        Envelope value = (Envelope) message.getValue();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("record", this.formatInlineWithoutAdditional(value));

        this.addDeleted(result, message);
        this.addKey(result, message);
        this.addMetadata(result, value);

        return result;
    }

    private Map<String, Object> formatInlineWithoutAdditional(Envelope value) {
        Map<String, Object> result = new LinkedHashMap<>();

        if (value.getOperation() == io.debezium.data.Envelope.Operation.DELETE) {
            result.putAll(value.getBefore());
        } else {
            result.putAll(Objects.requireNonNullElse(value.getAfter(), Collections.emptyMap()));
        }

        return result;
    }

    private void addDeleted(Map<String, Object> result, Pair<Message, Message> message) throws IllegalVariableEvaluationException {
        if (runContext.render(this.abstractDebeziumTask.getDeleted()).as(AbstractDebeziumTask.Deleted.class).orElseThrow() == AbstractDebeziumTask.Deleted.ADD_FIELD && message.getValue() instanceof Envelope) {
            io.debezium.data.Envelope.Operation operation = ((Envelope) message.getValue()).getOperation();

            result.put(runContext.render(this.abstractDebeziumTask.getDeletedFieldName()).as(String.class).orElseThrow(), operation == io.debezium.data.Envelope.Operation.DELETE || operation == io.debezium.data.Envelope.Operation.TRUNCATE);
        }
    }

    private void addKey(Map<String, Object> result, Pair<Message, Message> message) throws IllegalVariableEvaluationException {
        if (runContext.render(this.abstractDebeziumTask.getKey()).as(AbstractDebeziumTask.Key.class).orElseThrow() == AbstractDebeziumTask.Key.ADD_FIELD && message.getKey() != null) {
            result.putAll(JacksonMapper.toMap(message.getKey()));
        }
    }

    private void addMetadata(Map<String, Object> result, Envelope envelope) throws IllegalVariableEvaluationException {
        if (runContext.render(this.abstractDebeziumTask.getMetadata()).as(AbstractDebeziumTask.Metadata.class).orElseThrow() == AbstractDebeziumTask.Metadata.ADD_FIELD) {
            Map<Object, Object> metadata = new HashMap<>();

            if (envelope.getProperties() != null) {
                metadata.putAll(envelope.getProperties());
            }

            if (envelope.getOperation() != null) {
                metadata.put("operation", envelope.getOperation());
            }

            if (envelope.getTransaction() != null) {
                metadata.put("transaction", envelope.getTransaction());
            }

            if (envelope.getSource() != null) {
                metadata.put("source", envelope.getSource());
            }

            if (envelope.getTimestamp() != null) {
                metadata.put("timestamp", envelope.getTimestamp());
            }

            result.put(runContext.render(abstractDebeziumTask.getMetadataFieldName()).as(String.class).orElseThrow(), metadata);
        }
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return DebeziumEngine.ChangeConsumer.super.supportsTombstoneEvents();
    }
}
