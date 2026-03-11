package io.kestra.plugin.debezium;

import java.util.concurrent.ExecutorService;

import io.kestra.core.runners.RunContext;

import io.debezium.engine.DebeziumEngine;
import lombok.Getter;

public class CompletionCallback implements DebeziumEngine.CompletionCallback {
    private final RunContext runContext;

    private final ExecutorService executorService;

    @Getter
    private Throwable error;

    public CompletionCallback(RunContext runContext, ExecutorService executorService) {
        this.runContext = runContext;
        this.executorService = executorService;
    }

    @Override
    public void handle(boolean success, String message, Throwable error) {
        if (success) {
            runContext.logger().info("Debezium ended successfully with '{}'", message);
        } else {
            runContext.logger().warn("Debezium failed with '{}'", message);
        }

        this.error = error;
        this.executorService.shutdown();
    }
}
