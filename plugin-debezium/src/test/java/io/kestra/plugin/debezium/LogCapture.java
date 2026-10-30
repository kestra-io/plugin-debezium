package io.kestra.plugin.debezium;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.AppenderBase;

/**
 * Records every event logged in the JVM until closed, for the Debezium log level and secret leak tests.
 */
public final class LogCapture extends AppenderBase<ILoggingEvent> implements AutoCloseable {
    // the engine and JDBC threads append while the test thread reads
    private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

    private LogCapture() {
    }

    public static LogCapture record() {
        var capture = new LogCapture();
        capture.setContext(root().getLoggerContext());
        capture.start();
        root().addAppender(capture);
        return capture;
    }

    @Override
    protected void append(ILoggingEvent event) {
        events.add(event);
    }

    @Override
    public void close() {
        root().detachAppender(this);
        stop();
    }

    public boolean hasDebeziumOutputAtOrBelow(Level level) {
        return events.stream().anyMatch(
            event -> event.getLoggerName().startsWith("io.debezium.")
                && event.getLevel().toInt() <= ch.qos.logback.classic.Level.toLevel(level.name()).toInt()
        );
    }

    // names of the loggers that printed any of the secrets, in the message or in an attached exception
    public List<String> loggersPrinting(List<String> secrets) {
        return events.stream()
            .filter(event -> secrets.stream().anyMatch(fullText(event)::contains))
            .map(ILoggingEvent::getLoggerName)
            .distinct()
            .toList();
    }

    private static String fullText(ILoggingEvent event) {
        return event.getFormattedMessage() + (event.getThrowableProxy() == null ? "" : ThrowableProxyUtil.asString(event.getThrowableProxy()));
    }

    private static Logger root() {
        return (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    }
}
