package io.kestra.plugin.debezium;

import java.util.Map;

import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.json.JsonMapper;

/**
 * Jackson 3 twin of {@link CustomParameterDeserializer}, required since Micronaut 5 / core expose
 * Jackson 3 at the HTTP boundary and {@code @JsonDeserialize(using = ...)} is not bridged by
 * Micronaut's Jackson2AnnotationSupport. Both deserializers must be stacked on the same field.
 */
public class Jackson3CustomParameterDeserializer extends ValueDeserializer<Map<String, Object>> {
    private final JsonMapper mapper = JsonMapper.builder().build();

    @Override
    public Map<String, Object> deserialize(JsonParser p, DeserializationContext ctxt) {
        if (p.hasToken(JsonToken.VALUE_STRING)) {
            return mapper.readValue(p.getText(), Map.class);
        }

        // Read a single value from the current parser position via the context, NOT the local mapper:
        // Jackson 3 enables FAIL_ON_TRAILING_TOKENS by default, so mapper.readValue(p, ...) would treat
        // the nested object as a whole document and reject the following envelope property as a trailing
        // token. ctxt.readValue reads just this value and leaves the parser positioned for the rest.
        return ctxt.readValue(p, Map.class);
    }
}
