package io.kestra.plugin.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.debezium.models.Envelope;
import io.kestra.plugin.debezium.models.Message;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.source.SourceRecord;
import reactor.core.publisher.FluxSink;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FluxConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {
    private final AbstractDebeziumTask abstractDebeziumTask;

    @Getter
    private final FluxSink<AbstractDebeziumTask.StreamOutput> sink;

    @Getter
    private final Map<String, AtomicInteger> recordsCount =  new ConcurrentHashMap<>();

    public FluxConsumer(AbstractDebeziumTask abstractDebeziumTask, FluxSink<AbstractDebeziumTask.StreamOutput> sink) {
        this.abstractDebeziumTask = abstractDebeziumTask;
	    this.sink = sink;
    }

    @SneakyThrows
    @Override
    public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) {
        for (ChangeEvent<SourceRecord, SourceRecord> r : records) {
            SourceRecord record = r.value();

            Pair<Message, Message> message = MapConverter.convert(record);

            Map<String, Object> result = this.handle(message);

            if (result != null) {
                this.write(result, message.getValue().getSource());
            }

            committer.markProcessed(r);
        }

        committer.markBatchFinished();
    }

    private Map<String, Object> handle(Pair<Message, Message> message) {
        if (this.isFilter(message)) {
            return null;
        }

	    return switch (abstractDebeziumTask.getFormat()) {
		    case RAW -> this.handleFormatRaw(message);
		    case INLINE -> this.handleFormatInline(message);
		    case WRAP -> this.handleFormatWrap(message);
		    default ->
			    throw new IllegalArgumentException("Invalid Format '" + this.abstractDebeziumTask.getFormat() + "");
	    };
    }

    private void write(Map<String, Object> result, Message.Source source) throws IOException {
        String stream = switch (this.abstractDebeziumTask.getSplitTable()) {
	        case OFF -> "data";
	        case TABLE -> source.getDb() + "." + source.getTable();
	        case DATABASE -> source.getDb();
	        default ->
		        throw new IllegalArgumentException("Invalid SplitTable '" + this.abstractDebeziumTask.getSplitTable() + "");
        };

	    sink.next(AbstractDebeziumTask.StreamOutput.builder().stream(stream).data(result).build());
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean isFilter(Pair<Message, Message> message) {
        if (!(message.getValue() instanceof Envelope) && abstractDebeziumTask.getIgnoreDdl()) {
            return true;
        }

        if (message.getValue() == null && abstractDebeziumTask.getDeleted() == AbstractDebeziumTask.Deleted.DROP) {
            return true;
        }

        if (!(message.getValue() instanceof Envelope) && this.abstractDebeziumTask.getFormat() != AbstractDebeziumTask.Format.RAW) {
            return true;
        }

        return false;
    }

    private Map<String, Object> handleFormatRaw(Pair<Message, Message> message) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", message.getKey());
        result.put("value", message.getValue());

        this.addDeleted(result, message);

        return result;
    }

    private Map<String, Object> handleFormatInline(Pair<Message, Message> message) {
        Envelope value = (Envelope) message.getValue();

        Map<String, Object> result = this.formatInlineWithoutAdditional(value);

        this.addDeleted(result, message);
        this.addKey(result, message);
        this.addMetadata(result, value);

        return result;
    }

    private Map<String, Object> handleFormatWrap(Pair<Message, Message> message) {
        Envelope value = (Envelope) message.getValue();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("record", this.formatInlineWithoutAdditional(value));

        this.addDeleted(result, message);
        this.addKey(result, message);
        this.addMetadata(result, value);

        return result;
    }

    private Map<String, Object> formatInlineWithoutAdditional(Envelope value ) {
        Map<String, Object> result = new LinkedHashMap<>();

        if (value.getOperation() == io.debezium.data.Envelope.Operation.DELETE) {
            result.putAll(value.getBefore());
        } else {
            result.putAll(value.getAfter());
        }

        return result;
    }

    private void addDeleted(Map<String, Object> result, Pair<Message, Message> message) {
        if (this.abstractDebeziumTask.getDeleted() == AbstractDebeziumTask.Deleted.ADD_FIELD && message.getValue() instanceof Envelope) {
            io.debezium.data.Envelope.Operation operation = ((Envelope) message.getValue()).getOperation();

            result.put(this.abstractDebeziumTask.getDeletedFieldName(), operation == io.debezium.data.Envelope.Operation.DELETE || operation == io.debezium.data.Envelope.Operation.TRUNCATE);
        }
    }

    private void addKey(Map<String, Object> result, Pair<Message, Message> message) {
        if (this.abstractDebeziumTask.getKey() == AbstractDebeziumTask.Key.ADD_FIELD && message.getKey() != null) {
            result.putAll(JacksonMapper.toMap(message.getKey()));
        }
    }

    private void addMetadata(Map<String, Object> result, Envelope envelope) {
        if (this.abstractDebeziumTask.getMetadata() == AbstractDebeziumTask.Metadata.ADD_FIELD) {
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

            result.put(abstractDebeziumTask.getMetadataFieldName(), metadata);
        }
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return DebeziumEngine.ChangeConsumer.super.supportsTombstoneEvents();
    }
}
