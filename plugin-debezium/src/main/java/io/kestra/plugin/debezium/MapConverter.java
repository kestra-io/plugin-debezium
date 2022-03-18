package io.kestra.plugin.debezium;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.time.*;
import io.debezium.time.Date;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.Year;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.debezium.models.Envelope;
import io.kestra.plugin.debezium.models.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;

public class MapConverter {
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();

    public static Pair<Message, Message> convert(SourceRecord record) {
        Object key = record.keySchema() == null ? MapConverter.convert(record.keySchema(), record.key()) : null;
        Object value = MapConverter.convert(record.valueSchema(), record.value());

        return Pair.of(
            key != null ? MAPPER.convertValue(key, Message.class) : null,
            value instanceof Envelope ? (Envelope) value : MAPPER.convertValue(value, Message.class)
        );
    }

    @SuppressWarnings("RedundantCast")
    public static Object convert(Schema schema, Object value) {
        if (value == null) {
            if (schema == null) {
                // Any schema is valid, and we don't have a default, so treat this as an optional schema
                return null;
            }

            if (schema.defaultValue() != null) {
                return convert(schema, schema.defaultValue());
            }

            if (schema.isOptional()) {
                return null;
            }

            throw new IllegalArgumentException("Conversion error: null value for field that is required and has no default value");
        }

        try {
            // debezium types
            if (schema != null && schema.name() != null) {
                switch (schema.name()) {
                    case Date.SCHEMA_NAME:
                        if (!(value instanceof Integer)) {
                            throw new IllegalArgumentException("Invalid type for Date, expected Integer but was " + value.getClass() + " for '" + value + "'");
                        }
                        return LocalDate.ofEpochDay((int) value);

                    case Interval.SCHEMA_NAME:
                        if (!(value instanceof String)) {
                            throw new IllegalArgumentException("Invalid type for Interval, expected String but was " + value.getClass() + " for '" + value + "'");
                        }
                        return Period.parse((String) value);

                    case MicroDuration.SCHEMA_NAME:
                        if (!(value instanceof Long)) {
                            throw new IllegalArgumentException("Invalid type for MicroDuration, expected Long but was " + value.getClass() + " for '" + value + "'");
                        }
                        return Duration.ofMillis((Long) value * 1000);

                    case MicroTime.SCHEMA_NAME:
                        if (!(value instanceof Long)) {
                            throw new IllegalArgumentException("Invalid type for MicroTime, expected Long but was " + value.getClass() + " for '" + value + "'");
                        }
                        return LocalTime.ofNanoOfDay((Long) value * 1000);

                    case MicroTimestamp.SCHEMA_NAME:
                        if (!(value instanceof Long)) {
                            throw new IllegalArgumentException("Invalid type for MicroTimestamp, expected Long but was " + value.getClass() + " for '" + value + "'");
                        }
                        return Instant.ofEpochSecond(0L, (Long) value * 1000);

                    case NanoDuration.SCHEMA_NAME:
                        if (!(value instanceof Long)) {
                            throw new IllegalArgumentException("Invalid type for NanoDuration, expected Long but was " + value.getClass() + " for '" + value + "'");
                        }
                        return Duration.ofNanos((Long) value);

                    case NanoTime.SCHEMA_NAME:
                        if (!(value instanceof Long)) {
                            throw new IllegalArgumentException("Invalid type for NanoTime, expected Long but was " + value.getClass() + " for '" + value + "'");
                        }
                        return LocalTime.ofNanoOfDay((Long) value);

                    case NanoTimestamp.SCHEMA_NAME:
                        if (!(value instanceof Long)) {
                            throw new IllegalArgumentException("Invalid type for NanoTimestamp, expected Long but was " + value.getClass() + " for '" + value + "'");
                        }
                        return Instant.ofEpochSecond(0L, (Long) value);

                    case Time.SCHEMA_NAME:
                        if (!(value instanceof Long)) {
                            throw new IllegalArgumentException("Invalid type for Timestamp, expected Long but was " + value.getClass() + " for '" + value + "'");
                        }
                        return LocalTime.ofNanoOfDay((Long) value * 1000 * 1000);

                    case Timestamp.SCHEMA_NAME:
                        if (!(value instanceof Long)) {
                            throw new IllegalArgumentException("Invalid type for Timestamp, expected Long but was " + value.getClass() + " for '" + value + "'");
                        }
                        return Instant.ofEpochMilli((Long) value);

                    case Year.SCHEMA_NAME:
                        if (!(value instanceof Integer)) {
                            throw new IllegalArgumentException("Invalid type for Year, expected Integer but was " + value.getClass() + " for '" + value + "'");
                        }
                        return LocalDate.of((Integer) value, 1, 1);

                    case ZonedTime.SCHEMA_NAME:
                        if (!(value instanceof String)) {
                            throw new IllegalArgumentException("Invalid type for ZonedTime, expected String but was " + value.getClass() + " for '" + value + "'");
                        }
                        return OffsetTime.parse((String) value);


                    case ZonedTimestamp.SCHEMA_NAME:
                        if (!(value instanceof String)) {
                            throw new IllegalArgumentException("Invalid type for ZonedTimestamp, expected String but was " + value.getClass() + " for '" + value + "'");
                        }
                        return ZonedDateTime.parse((String) value);

                    case Decimal.LOGICAL_NAME:
                        if (!(value instanceof BigDecimal)) {
                            throw new IllegalArgumentException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass() + " for '" + value + "'");
                        }
                        return (BigDecimal) value;
                }
            }

            // standard connect types
            final Schema.Type schemaType;
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(value.getClass());
                if (schemaType == null) {
                    throw new IllegalArgumentException("Java class " + value.getClass() + " does not have corresponding schema type.");
                }
            } else {
                schemaType = schema.type();
            }

            switch (schemaType) {
                case INT8:
                    return (Byte) value;
                case INT16:
                    return (Short) value;
                case INT32:
                    return (Integer) value;
                case INT64:
                    return (Long) value;
                case FLOAT32:
                    return (Float) value;
                case FLOAT64:
                    return (Double) value;
                case BOOLEAN:
                    return (Boolean) value;
                case STRING:
                    return (CharSequence) value;
                case BYTES:
                    if (value instanceof byte[])
                        return (byte[]) value;
                    else if (value instanceof ByteBuffer)
                        return ((ByteBuffer) value).array();
                    else
                        throw new IllegalArgumentException("Invalid type for bytes type: " + value.getClass());
                case ARRAY: {
                    Collection<?> collection = (Collection<?>) value;
                    List<Object> list = new ArrayList<>();
                    for (Object elem : collection) {
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        Object fieldValue = convert(valueSchema, elem);
                        list.add(fieldValue);
                    }
                    return list;
                }
                case MAP: {
                    Map<?, ?> map = (Map<?, ?>) value;

                    // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
                    boolean objectMode;
                    if (schema == null) {
                        objectMode = true;
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            if (!(entry.getKey() instanceof String)) {
                                objectMode = false;
                                break;
                            }
                        }
                    } else {
                        objectMode = schema.keySchema().type() == Schema.Type.STRING;
                    }
                    Map<String, Object> obj = null;
                    List<Object> list = null;
                    if (objectMode)
                        obj = new LinkedHashMap<>();
                    else
                        list = new ArrayList<>();

                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        Schema keySchema = schema == null ? null : schema.keySchema();
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        Object mapKey = convert(keySchema, entry.getKey());
                        Object  mapValue = convert(valueSchema, entry.getValue());

                        if (objectMode)
                            obj.put((String) mapKey, mapValue);
                        else
                            list.add(List.of(mapKey, mapValue));
                    }
                    return objectMode ? obj : list;
                }
                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (!struct.schema().equals(schema)) {
                        throw new IllegalArgumentException("Mismatching schema.");
                    }

                    Map<String, Object> obj = new LinkedHashMap<>();
                    for (Field field : schema.fields()) {
                        obj.put(field.name(), convert(field.schema(), struct.get(field)));
                    }

                    if (schema.name() != null && io.debezium.data.Envelope.isEnvelopeSchema(schema)) {
                        return MAPPER.convertValue(obj, Envelope.class);
                    } else {
                        return obj;
                    }

                }
            }

            throw new IllegalArgumentException("Couldn't convert " + value + " to JSON.");
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new IllegalArgumentException("Invalid type for " + schemaTypeStr + ": " + value.getClass() + ", value:" + value, e);
        }
    }
}
