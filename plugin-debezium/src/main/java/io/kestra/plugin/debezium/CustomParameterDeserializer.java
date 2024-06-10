package io.kestra.plugin.debezium;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class CustomParameterDeserializer extends JsonDeserializer<Map<String, Object>> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Map<String, Object> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        if (ctxt.getParser().currentToken().name().equalsIgnoreCase("value_string")) {
            return mapper.readValue(p.getText(), Map.class);
        }
        return mapper.readValue(p, Map.class);
    }
}