package io.kestra.plugin.debezium;

import java.util.Map;

import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.json.JsonMapper;

/**
 * Jackson 3 twin of {@link CustomParameterDeserializer}, required since Micronaut 5 / core expose
 * Jackson 3 at the HTTP boundary and {@code @JsonDeserialize(using = ...)} is not bridged by
 * Micronaut's Jackson2AnnotationSupport. Both deserializers must be stacked on the same field.
 */
public class Jackson3CustomParameterDeserializer extends ValueDeserializer<Map<String, Object>> {
    // FAIL_ON_TRAILING_TOKENS is enabled by default in Jackson 3, but mapper.readValue(p, ...) is used
    // here to read a single value mid-stream from an already-open parser positioned on the envelope
    // field, not a whole document, so the following envelope property must not be rejected as trailing.
    private final JsonMapper mapper = JsonMapper.builder()
        .disable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .build();

    @Override
    public Map<String, Object> deserialize(JsonParser p, DeserializationContext ctxt) {
        if (p.hasToken(JsonToken.VALUE_STRING)) {
            return mapper.readValue(p.getText(), Map.class);
        }

        return mapper.readValue(p, Map.class);
    }
}
