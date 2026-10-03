package io.kestra.plugin.debezium;

import java.time.Duration;
import java.util.Optional;

/**
 * How long a realtime trigger waits before opening another Debezium engine.
 *
 * <p>
 * The first connection attempt is immediate. This type is only consulted after an attempt has
 * already failed. A slow failure (for example a TCP timeout that lasts longer than the current
 * delay) still increases the delay: elapsed time is not an input. The delay resets only when the
 * failed attempt had reached {@code taskStarted}, because that attempt was a new outage rather
 * than another startup failure.
 *
 * <p>
 * Not thread-safe. The publisher thread that owns the subscription is the only caller.
 */
final class ReconnectPolicy {

    private final Duration initialDelay;
    private final Duration maxDelay;
    private final Integer maxReconnectAttempts;

    private Duration currentDelay;
    private int reconnectsInStreak;

    private ReconnectPolicy(Duration initialDelay, Duration maxDelay, Integer maxReconnectAttempts) {
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
        this.maxReconnectAttempts = maxReconnectAttempts;
        this.currentDelay = initialDelay;
    }

    static ReconnectPolicy of(Duration initialDelay, Duration maxDelay, Integer maxReconnectAttempts) {
        if (initialDelay == null || initialDelay.isZero() || initialDelay.isNegative()) {
            throw new IllegalArgumentException("reconnectInitialDelay must be positive");
        }
        if (maxDelay == null || maxDelay.compareTo(initialDelay) < 0) {
            throw new IllegalArgumentException(
                "reconnectMaxDelay must be greater than or equal to reconnectInitialDelay"
            );
        }
        if (maxReconnectAttempts != null && maxReconnectAttempts < 0) {
            throw new IllegalArgumentException("maxReconnectAttempts must be zero or positive");
        }
        return new ReconnectPolicy(initialDelay, maxDelay, maxReconnectAttempts);
    }

    Integer maxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    /**
     * @param taskStarted whether this attempt reached Debezium's {@code taskStarted} callback
     * @return the wait before the next attempt, or empty when {@code maxReconnectAttempts} is exhausted
     */
    Optional<Retry> afterFailure(boolean taskStarted) {
        if (taskStarted) {
            currentDelay = initialDelay;
            reconnectsInStreak = 0;
        }
        if (maxReconnectAttempts != null && reconnectsInStreak >= maxReconnectAttempts) {
            return Optional.empty();
        }

        Duration delay = currentDelay;
        boolean firstOfStreak = reconnectsInStreak == 0;
        int attempt = reconnectsInStreak + 1;
        currentDelay = cappedDouble(currentDelay);
        reconnectsInStreak++;
        return Optional.of(new Retry(delay, attempt, firstOfStreak));
    }

    private Duration cappedDouble(Duration delay) {
        if (delay.compareTo(maxDelay) >= 0) {
            return maxDelay;
        }
        try {
            Duration doubled = delay.multipliedBy(2);
            if (doubled.compareTo(maxDelay) > 0) {
                return maxDelay;
            }
            return doubled;
        } catch (ArithmeticException overflow) {
            return maxDelay;
        }
    }

    record Retry(Duration delay, int attempt, boolean firstOfStreak) {
    }
}
