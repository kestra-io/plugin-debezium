package io.kestra.plugin.debezium.models;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Message {
    Source source;

    Map<String, Object> properties;

    @JsonAnyGetter
    public Map<String, Object> getProperties(){
        return properties != null ? properties : new HashMap<>();
    }

    @JsonAnySetter
    public void addProperties(String property, Object value){
        if (properties == null) {
            properties = new HashMap<>();
        }

        properties.put(property, value);
    }

    @Data
    @AllArgsConstructor
    public static class Source extends Message {
        String version;

        String connector;

        String name;

        @Nullable
        Instant timestamp;

        String snapshot;

        String db;

        String table;

        Integer row;

        @JsonSetter("ts_ms")
        public void setTsMs(Long value) {
            this.timestamp = Instant.ofEpochMilli(value);
        }
    }
}
