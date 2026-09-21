package io.kestra.plugin.debezium;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.debezium.models.Envelope;

import tools.jackson.databind.json.JsonMapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Ensures {@link CustomParameterDeserializer} (Jackson 2) and {@link Jackson3CustomParameterDeserializer}
 * (Jackson 3) are both correctly wired on {@link Envelope#getBefore()} / {@link Envelope#getAfter()},
 * since the two Jackson majors coexist and neither {@code @JsonDeserialize} annotation is bridged
 * to the other Jackson major.
 */
class CustomParameterDeserializerTest {
    private static final JsonMapper JACKSON3_MAPPER = JsonMapper.builder().build();

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
        var jackson2 = JacksonMapper.ofJson().readValue(json, Envelope.class);
        assertThat(jackson2.getBefore(), expectedBefore == null ? is(nullValue()) : is(expectedBefore));
        assertThat(jackson2.getAfter(), expectedAfter == null ? is(nullValue()) : is(expectedAfter));

        var jackson3 = JACKSON3_MAPPER.readValue(json, Envelope.class);
        assertThat(jackson3.getBefore(), expectedBefore == null ? is(nullValue()) : is(expectedBefore));
        assertThat(jackson3.getAfter(), expectedAfter == null ? is(nullValue()) : is(expectedAfter));
    }
}
