package io.kestra.plugin.debezium.mysql;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class OffsetCommitModeTest {

    @Test
    void testTriggerDefaultOffsetCommitMode() {
        Trigger trigger = Trigger.builder()
            .id("test")
            .serverId(Property.ofValue("123456789"))
            .build();

        assertThat(trigger.getOffsetsCommitMode(), notNullValue());
        // Test that the property is set to the default value
        assertThat(trigger.getOffsetsCommitMode().toString().contains("ON_EACH_BATCH"), is(true));
    }

    @Test
    void testTriggerCustomOffsetCommitMode() {
        Trigger trigger = Trigger.builder()
            .id("test")
            .serverId(Property.ofValue("123456789"))
            .offsetsCommitMode(Property.ofValue(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_STOP))
            .build();

        assertThat(trigger.getOffsetsCommitMode(), notNullValue());
        // Test that the property is set to the custom value
        assertThat(trigger.getOffsetsCommitMode().toString().contains("ON_STOP"), is(true));
    }

    @Test
    void testCaptureDefaultOffsetCommitMode() {
        Capture capture = Capture.builder()
            .id("test")
            .serverId(Property.ofValue("123456789"))
            .build();

        assertThat(capture.getOffsetsCommitMode(), notNullValue());
        // Test that the property is set to the default value
        assertThat(capture.getOffsetsCommitMode().toString().contains("ON_EACH_BATCH"), is(true));
    }

    @Test
    void testCaptureCustomOffsetCommitMode() {
        Capture capture = Capture.builder()
            .id("test")
            .serverId(Property.ofValue("123456789"))
            .offsetsCommitMode(Property.ofValue(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_STOP))
            .build();

        assertThat(capture.getOffsetsCommitMode(), notNullValue());
        // Test that the property is set to the custom value
        assertThat(capture.getOffsetsCommitMode().toString().contains("ON_STOP"), is(true));
    }
}
