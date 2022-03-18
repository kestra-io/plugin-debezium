package io.kestra.plugin.debezium;

import io.debezium.time.*;
import io.debezium.time.Date;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.Year;
import org.apache.kafka.connect.data.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class MapConverterTest {
    private final static Schema STRUCT_SCHEMA = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).field("b", Schema.INT32_SCHEMA).build();

    static Stream<Arguments> source() {
        return Stream.of(
            // primitive
            Arguments.of(Schema.INT8_SCHEMA, Byte.valueOf("8"), null),
            Arguments.of(Schema.INT16_SCHEMA, Short.parseShort("16"), null),
            Arguments.of(Schema.INT32_SCHEMA, 32, null),
            Arguments.of(Schema.INT64_SCHEMA, 64L, null),
            Arguments.of(Schema.FLOAT32_SCHEMA, 32F, null),
            Arguments.of(Schema.FLOAT64_SCHEMA, 64D, null),
            Arguments.of(Schema.BOOLEAN_SCHEMA, true, null),
            Arguments.of(Schema.STRING_SCHEMA, "test", null),
            Arguments.of(Schema.BYTES_SCHEMA, "test".getBytes(), null),
            Arguments.of(Schema.BYTES_SCHEMA, ByteBuffer.wrap("test".getBytes()), "test".getBytes()),

            // complex
            Arguments.of(SchemaBuilder.array(Schema.INT32_SCHEMA).build(), List.of(1, 2, 3), null),
            Arguments.of(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(), Map.of("a", 1, "b", 2, "c", 3), null),
            Arguments.of(STRUCT_SCHEMA, new Struct(STRUCT_SCHEMA).put("a", "test").put("b", 32),  Map.of("a", "test", "b", 32)),

            // logicalType
            Arguments.of(SchemaBuilder.int32().name(Date.SCHEMA_NAME).build(), Long.valueOf(Instant.parse("2019-10-06T18:27:49Z").atZone(ZoneId.systemDefault()).toLocalDate().toEpochDay()).intValue(), Instant.parse("2019-10-06T18:27:49Z").atZone(ZoneId.systemDefault()).toLocalDate()),
            Arguments.of(SchemaBuilder.string().name(Interval.SCHEMA_NAME).build(), "P2Y", Period.parse("P2Y")),
            Arguments.of(SchemaBuilder.int64().name(MicroDuration.SCHEMA_NAME).build(), Duration.ofSeconds(5).toSeconds(), Duration.ofSeconds(5)),
            Arguments.of(SchemaBuilder.int64().name(MicroTime.SCHEMA_NAME).build(), Instant.parse("2019-10-06T18:27:49Z").atZone(ZoneId.systemDefault()).toLocalTime().toNanoOfDay() / 1000, Instant.parse("2019-10-06T18:27:49Z").atZone(ZoneId.systemDefault()).toLocalTime()),
            Arguments.of(SchemaBuilder.int64().name(MicroTimestamp.SCHEMA_NAME).build(), Instant.parse("2019-10-06T18:27:49Z").toEpochMilli() * 1000, Instant.parse("2019-10-06T18:27:49Z")),
            Arguments.of(SchemaBuilder.int64().name(NanoDuration.SCHEMA_NAME).build(), Duration.ofSeconds(5).toNanos(), Duration.ofSeconds(5)),
            Arguments.of(SchemaBuilder.int64().name(NanoTime.SCHEMA_NAME).build(), Instant.parse("2019-10-06T18:27:49Z").atZone(ZoneId.systemDefault()).toLocalTime().toNanoOfDay(), Instant.parse("2019-10-06T18:27:49Z").atZone(ZoneId.systemDefault()).toLocalTime()),
            Arguments.of(SchemaBuilder.int64().name(NanoTimestamp.SCHEMA_NAME).build(), Instant.parse("2019-10-06T18:27:49Z").toEpochMilli() * 1000 * 1000, Instant.parse("2019-10-06T18:27:49Z")),
            Arguments.of(SchemaBuilder.int32().name(Time.SCHEMA_NAME).build(), Instant.parse("2019-10-06T18:27:49Z").atZone(ZoneId.systemDefault()).toLocalTime().toNanoOfDay() / 1000 / 1000, Instant.parse("2019-10-06T18:27:49Z").atZone(ZoneId.systemDefault()).toLocalTime()),
            Arguments.of(SchemaBuilder.int32().name(Timestamp.SCHEMA_NAME).build(), Instant.parse("2019-10-06T18:27:49Z").toEpochMilli(), Instant.parse("2019-10-06T18:27:49Z")),
            Arguments.of(SchemaBuilder.int32().name(Year.SCHEMA_NAME).build(), 2020, LocalDate.of(2020, 1, 1)),
            Arguments.of(SchemaBuilder.string().name(ZonedTime.SCHEMA_NAME).build(), "18:27:49Z", OffsetTime.parse("18:27:49Z")),
            Arguments.of(SchemaBuilder.string().name(ZonedTimestamp.SCHEMA_NAME).build(), "2019-10-06T18:27:49Z", ZonedDateTime.parse("2019-10-06T18:27:49Z")),
            Arguments.of(SchemaBuilder.int32().name(Decimal.LOGICAL_NAME).build(), BigDecimal.valueOf(12L), null),

            // default
            Arguments.of(SchemaBuilder.int32().defaultValue(32).build(), null, 32)
        );
    }

    @ParameterizedTest
    @MethodSource("source")
    void convert(Schema schema, Object value, Object expected) {
        Object convert = MapConverter.convert(schema, value);

        assertThat(convert, is(expected != null ? expected : value));
    }
}
