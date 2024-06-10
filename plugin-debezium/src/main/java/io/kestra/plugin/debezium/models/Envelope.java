package io.kestra.plugin.debezium.models;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.kestra.plugin.debezium.CustomParameterDeserializer;
import lombok.AllArgsConstructor;
import lombok.Data;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Map;

@Data
@AllArgsConstructor
public class Envelope extends Message {
    @JsonIgnore
    @Nullable
    io.debezium.data.Envelope.Operation operation;

    @JsonIgnore
    @Nullable
    Instant timestamp;

    @Nullable
    @JsonDeserialize(using = CustomParameterDeserializer.class)
    Map<String, Object> before;

    @Nullable
    @JsonDeserialize(using = CustomParameterDeserializer.class)
    Map<String, Object> after;

    @Nullable
    Map<String, Object> transaction;

    @JsonGetter("op")
    public String getOp() {
        return this.operation != null ? this.operation.code() : null;
    }

    public void setOp(String value) {
        this.operation = io.debezium.data.Envelope.Operation.forCode(value);
    }

    @JsonGetter("ts_ms")
    public Long getTs() {
        return this.timestamp != null ? this.timestamp.toEpochMilli() : null;
    }

    @JsonSetter("ts_ms")
    public void setTsMs(Long value) {
        this.timestamp = Instant.ofEpochMilli(value);
    }
}
