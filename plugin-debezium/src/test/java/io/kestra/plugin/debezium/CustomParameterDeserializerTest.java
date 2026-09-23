package io.kestra.plugin.debezium;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.debezium.models.Envelope;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Ensures {@link CustomParameterDeserializer} is wired on {@link Envelope#getBefore()} / {@link Envelope#getAfter()}
 * and handles both nested objects and JSON-encoded strings.
 */
class CustomParameterDeserializerTest {
    @Test
    void nestedObject() throws Exception {
        var json = """
            {
                "before": {"id": 1, "name": "before-name"},
                "after": {"id": 1, "name": "after-name"}
            }""";

        assertBeforeAfter(json, Map.of("id", 1, "name", "before-name"), Map.of("id", 1, "name", "after-name"));
    }

    @Test
    void jsonEncodedString() throws Exception {
        var json = """
            {
                "before": "{\\"id\\": 1, \\"name\\": \\"before-name\\"}",
                "after": "{\\"id\\": 1, \\"name\\": \\"after-name\\"}"
            }""";

        assertBeforeAfter(json, Map.of("id", 1, "name", "before-name"), Map.of("id", 1, "name", "after-name"));
    }

    @Test
    void nullValues() throws Exception {
        var json = """
            {
                "before": null,
                "after": null
            }""";

        assertBeforeAfter(json, null, null);
    }

    @Test
    void absentValues() throws Exception {
        assertBeforeAfter("{}", null, null);
    }

    private void assertBeforeAfter(String json, Map<String, Object> expectedBefore, Map<String, Object> expectedAfter) throws Exception {
        var envelope = JacksonMapper.ofJson().readValue(json, Envelope.class);
        assertThat(envelope.getBefore(), expectedBefore == null ? is(nullValue()) : is(expectedBefore));
        assertThat(envelope.getAfter(), expectedAfter == null ? is(nullValue()) : is(expectedAfter));
    }
}
