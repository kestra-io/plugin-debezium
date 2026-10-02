package io.kestra.plugin.debezium;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import io.debezium.data.Envelope;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import jakarta.inject.Inject;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class ChangeConsumerTest {
    private static final Schema ROW_SCHEMA = SchemaBuilder.struct()
        .name("test.public.events.Value")
        .field("events_id", Schema.INT32_SCHEMA)
        .field("event_title", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
        .name("test.Source")
        .field("db", Schema.STRING_SCHEMA)
        .field("table", Schema.STRING_SCHEMA)
        .build();

    private static final Envelope ENVELOPE = Envelope.defineSchema()
        .withName("test.public.events.Envelope")
        .withRecord(ROW_SCHEMA)
        .withSource(SOURCE_SCHEMA)
        .build();

    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void deletedNullDoesNotAddKey() {
        AbstractDebeziumTask task = new AbstractDebeziumTask() {
            {
                deleted = Property.ofValue(Deleted.NULL);
            }

            @Override
            protected boolean needDatabaseHistory() {
                return false;
            }
        };

        Struct before = new Struct(ROW_SCHEMA).put("events_id", 1).put("event_title", "Machine Head");
        Struct source = new Struct(SOURCE_SCHEMA).put("db", "postgres").put("table", "events");

        // schemaless key so it is converted (keys with a schema are currently always dropped by MapConverter)
        SourceRecord record = new SourceRecord(
            Map.of(), Map.of(), "test.public.events", null,
            null, Map.of("events_id", 1),
            ENVELOPE.schema(), ENVELOPE.delete(before, source, Instant.now())
        );

        List<AbstractDebeziumRealtimeTrigger.StreamOutput> outputs = this.consume(task, record);

        assertThat(outputs.size(), is(1));
        Map<String, Object> data = outputs.getFirst().getData();

        // the key must not put the primary key value back over the nulled column
        assertTrue(data.containsKey("events_id"));
        assertThat(data.get("events_id"), is(nullValue()));
        assertThat(data.get("event_title"), is(nullValue()));
        assertThat(((Map<String, Object>) data.get("metadata")).get("operation"), is(Envelope.Operation.DELETE));
    }

    private List<AbstractDebeziumRealtimeTrigger.StreamOutput> consume(AbstractDebeziumTask task, SourceRecord record) {
        RunContext runContext = runContextFactory.of();
        ChangeConsumer consumer = new ChangeConsumer(task, runContext, new AtomicInteger(), new AtomicBoolean(), ZonedDateTime.now(), Path.of("offsets"), Path.of("history"));

        return Flux.<AbstractDebeziumRealtimeTrigger.StreamOutput> create(sink ->
        {
            consumer.handleBatch(List.of(new TestChangeEvent(record)), new NoopCommitter(), sink, AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_STOP);
            sink.complete();
        }).collectList().block();
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
