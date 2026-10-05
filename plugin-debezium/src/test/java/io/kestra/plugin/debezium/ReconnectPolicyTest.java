package io.kestra.plugin.debezium;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReconnectPolicyTest {

    @Test
    void failedStartupsBackOffExponentiallyAndCap() {
        var policy = ReconnectPolicy.of(Duration.ofSeconds(1), Duration.ofSeconds(60), null);

        assertThat(
            delays(policy, 8, false), contains(
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                Duration.ofSeconds(4),
                Duration.ofSeconds(8),
                Duration.ofSeconds(16),
                Duration.ofSeconds(32),
                Duration.ofSeconds(60),
                Duration.ofSeconds(60)
            )
        );
    }

    @Test
    void slowStartupFailureStillIncreasesTheDelay() {
        // Elapsed time is not an input. A connect timeout longer than the current delay
        // is still a failed startup and must not reset the sequence.
        var policy = ReconnectPolicy.of(Duration.ofSeconds(1), Duration.ofSeconds(60), null);

        assertThat(delays(policy, 2, false), contains(Duration.ofSeconds(1), Duration.ofSeconds(2)));
    }

    @Test
    void stableRunResetsTheDelayAndTheAttemptCount() {
        var policy = ReconnectPolicy.of(Duration.ofSeconds(1), Duration.ofSeconds(60), 1);

        var first = policy.afterFailure(false).orElseThrow();
        assertThat(first.delay(), is(Duration.ofSeconds(1)));
        assertThat(first.attempt(), is(1));
        assertThat(first.firstOfStreak(), is(true));
        assertThat(policy.afterFailure(false).isEmpty(), is(true));

        var reset = policy.afterFailure(true).orElseThrow();
        assertThat(reset.delay(), is(Duration.ofSeconds(1)));
        assertThat(reset.attempt(), is(1));
        assertThat(reset.firstOfStreak(), is(true));
        assertThat(policy.afterFailure(false).isEmpty(), is(true));

        var continued = ReconnectPolicy.of(Duration.ofSeconds(1), Duration.ofSeconds(60), null);
        continued.afterFailure(false);
        continued.afterFailure(false);
        var afterStart = continued.afterFailure(true).orElseThrow();
        assertThat(afterStart.delay(), is(Duration.ofSeconds(1)));
        assertThat(afterStart.firstOfStreak(), is(true));
        var next = continued.afterFailure(false).orElseThrow();
        assertThat(next.delay(), is(Duration.ofSeconds(2)));
        assertThat(next.firstOfStreak(), is(false));
    }

    @Test
    void shortLivedStartKeepsBackingOff() {
        long startedAt = 1_000L;
        long diedImmediately = startedAt + Duration.ofMillis(50).toNanos();
        var stablePeriod = Duration.ofSeconds(60);

        assertThat(ReconnectPolicy.ranStably(0, diedImmediately, stablePeriod), is(false));
        assertThat(ReconnectPolicy.ranStably(startedAt, diedImmediately, stablePeriod), is(false));
        assertThat(ReconnectPolicy.ranStably(startedAt, startedAt + stablePeriod.toNanos(), stablePeriod), is(true));

        var policy = ReconnectPolicy.of(Duration.ofSeconds(1), stablePeriod, 3);
        assertThat(delays(policy, 3, false), contains(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(4)));
        assertThat(policy.afterFailure(false).isEmpty(), is(true));
    }

    @Test
    void jitterSpreadsTheWaitWithoutGoingNonPositive() {
        var delay = Duration.ofSeconds(60);

        assertThat(nanosClose(ReconnectPolicy.withJitter(delay, 0.2d), Duration.ofSeconds(72)), is(true));
        assertThat(nanosClose(ReconnectPolicy.withJitter(delay, -0.2d), Duration.ofSeconds(48)), is(true));
        assertThat(ReconnectPolicy.withJitter(delay, 0d), is(delay));
        assertThat(ReconnectPolicy.withJitter(Duration.ofNanos(1), -0.9d).isNegative(), is(false));
        assertThat(ReconnectPolicy.withJitter(Duration.ofNanos(1), -0.9d).isZero(), is(false));
    }

    @Test
    void zeroReconnectAttemptsParksAfterTheFirstFailure() {
        var policy = ReconnectPolicy.of(Duration.ofSeconds(1), Duration.ofMinutes(1), 0);

        assertThat(policy.afterFailure(false).isEmpty(), is(true));
    }

    @Test
    void doublingDelayDoesNotOverflow() {
        Duration initial = Duration.ofSeconds(Long.MAX_VALUE / 2 + 1);
        Duration max = Duration.ofSeconds(Long.MAX_VALUE);
        var policy = ReconnectPolicy.of(initial, max, null);

        assertThat(policy.afterFailure(false).orElseThrow().delay(), is(initial));
        assertThat(policy.afterFailure(false).orElseThrow().delay(), is(max));
    }

    @Test
    void nonPositiveInitialDelayFailsBeforeSubscribe() {
        assertRejectedBeforeSubscribe(Duration.ZERO, Duration.ofMinutes(1), null, "reconnectInitialDelay");
        assertRejectedBeforeSubscribe(Duration.ofSeconds(-1), Duration.ofMinutes(1), null, "reconnectInitialDelay");
    }

    @Test
    void maxDelayShorterThanInitialDelayFailsBeforeSubscribe() {
        assertRejectedBeforeSubscribe(Duration.ofSeconds(2), Duration.ofSeconds(1), null, "reconnectMaxDelay");
    }

    @Test
    void negativeAttemptLimitFailsBeforeSubscribe() {
        assertRejectedBeforeSubscribe(Duration.ofSeconds(1), Duration.ofMinutes(1), -1, "maxReconnectAttempts");
    }

    private static boolean nanosClose(Duration actual, Duration expected) {
        return Math.abs(actual.toNanos() - expected.toNanos()) < 1_000L;
    }

    private static List<Duration> delays(ReconnectPolicy policy, int failures, boolean stable) {
        var delays = new ArrayList<Duration>();
        for (int i = 0; i < failures; i++) {
            delays.add(policy.afterFailure(stable).orElseThrow().delay());
        }
        return delays;
    }

    /**
     * Same order as {@code AbstractDebeziumRealtimeTrigger#publisher}: validate, and only then
     * create a publisher a caller could subscribe to.
     */
    private static void assertRejectedBeforeSubscribe(Duration initial, Duration max, Integer attempts, String message) {
        var subscribed = new AtomicBoolean();
        var exception = assertThrows(IllegalArgumentException.class, () ->
        {
            ReconnectPolicy.of(initial, max, attempts);
            Flux.create(sink -> subscribed.set(true)).subscribe();
        });

        assertThat(exception.getMessage(), containsString(message));
        assertThat(subscribed.get(), is(false));
    }
}
