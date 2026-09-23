package io.kestra.plugin.debezium;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Jackson 2 deserializer for Debezium {@code before}/{@code after} envelope fields, which can be
 * either a nested JSON object (relational connectors) or a JSON-encoded string (e.g. MongoDB).
 */
public class CustomParameterDeserializer extends JsonDeserializer<Map<String, Object>> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Map<String, Object> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        if (p.hasToken(JsonToken.VALUE_STRING)) {
            return mapper.readValue(p.getText(), Map.class);
        }

        return mapper.readValue(p, Map.class);
    }
}
