package io.kestra.plugin.debezium;

import io.debezium.engine.DebeziumEngine;
import io.kestra.core.runners.RunContext;
import lombok.Getter;

public class CompletionCallback implements DebeziumEngine.CompletionCallback {
    private final RunContext runContext;

    @Getter
    private Throwable error;

    public CompletionCallback(RunContext runContext) {
        this.runContext = runContext;
    }

    @Override
    public void handle(boolean success, String message, Throwable error) {
        if (success) {
            runContext.logger().info("Debezium ended successfully with '{}'", message);
        } else {
            runContext.logger().warn("Debezium failed with '{}'", message);
        }

        this.error = error;
    }
}
