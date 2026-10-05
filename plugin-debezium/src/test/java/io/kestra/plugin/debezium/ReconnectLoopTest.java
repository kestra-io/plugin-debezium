package io.kestra.plugin.debezium;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class ReconnectLoopTest {

    @Test
    void attemptsDoNotOverlapAndStopAtTheReconnectLimit() throws Exception {
        var inFlight = new AtomicInteger();
        var maxInFlight = new AtomicInteger();
        var attempts = new AtomicInteger();
        var policy = ReconnectPolicy.of(Duration.ofSeconds(1), Duration.ofSeconds(60), 3);
        var control = new RecordingControl(true);

        var completed = new AtomicBoolean();
        var error = new AtomicReference<Throwable>();
        var subscriber = new Thread(() -> Flux.<Void> create(sink ->
        {
            ReconnectLoop.run(policy, control, () ->
            {
                int now = inFlight.incrementAndGet();
                maxInFlight.accumulateAndGet(now, Math::max);
                attempts.incrementAndGet();
                inFlight.decrementAndGet();
                return ReconnectLoop.AttemptResult.failure(new IOException("connection refused"), false);
            });
            sink.complete();
        }).subscribe(ignored ->
        {
        }, error::set, () -> completed.set(true)));

        subscriber.start();
        assertThat(control.parked.await(5, TimeUnit.SECONDS), is(true));

        assertThat(attempts.get(), is(4));
        assertThat(maxInFlight.get(), is(1));
        assertThat(inFlight.get(), is(0));
        assertThat(control.sleeps, contains(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(4)));
        assertThat(control.exhausted, is(1));
        assertThat(completed.get(), is(false));
        assertThat(error.get(), is(nullValue()));

        control.release.countDown();
        subscriber.join(5_000);
        assertThat(subscriber.isAlive(), is(false));
        assertThat(completed.get(), is(true));
        assertThat(error.get(), is(nullValue()));
    }

    @Test
    void stableRunResetsTheNextWait() {
        var attempt = new AtomicInteger();
        var policy = ReconnectPolicy.of(Duration.ofSeconds(1), Duration.ofSeconds(60), null);
        var control = new RecordingControl(false);
        control.stopAfterSleeps = 3;

        var completed = new AtomicBoolean();
        var error = new AtomicReference<Throwable>();
        Flux.<Void> create(sink ->
        {
            ReconnectLoop.run(policy, control, () ->
            {
                int n = attempt.incrementAndGet();
                return ReconnectLoop.AttemptResult.failure(new IOException("connection refused"), n == 3);
            });
            sink.complete();
        }).subscribe(ignored ->
        {
        }, error::set, () -> completed.set(true));

        assertThat(control.sleeps, contains(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(1)));
        assertThat(completed.get(), is(true));
        assertThat(error.get(), is(nullValue()));
        assertThat(control.exhausted, is(0));
    }

    @Test
    void stopDuringBackoffDoesNotStartAnotherAttemptAndCompletes() {
        var attempts = new AtomicInteger();
        var policy = ReconnectPolicy.of(Duration.ofSeconds(1), Duration.ofSeconds(60), null);
        var control = new RecordingControl(false);
        control.stopOnFirstSleep = true;

        var completed = new AtomicBoolean();
        var error = new AtomicReference<Throwable>();
        Flux.<Void> create(sink ->
        {
            ReconnectLoop.run(policy, control, () ->
            {
                attempts.incrementAndGet();
                return ReconnectLoop.AttemptResult.failure(new IOException("connection refused"), false);
            });
            sink.complete();
        }).subscribe(ignored ->
        {
        }, error::set, () -> completed.set(true));

        assertThat(attempts.get(), is(1));
        assertThat(control.sleeps, contains(Duration.ofSeconds(1)));
        assertThat(control.exhausted, is(0));
        assertThat(completed.get(), is(true));
        assertThat(error.get(), is(nullValue()));
    }

    @Test
    void stoppedAttemptDoesNotReconnect() {
        var attempts = new AtomicInteger();
        var policy = ReconnectPolicy.of(Duration.ofSeconds(1), Duration.ofSeconds(60), null);
        var control = new RecordingControl(false);

        Flux.<Void> create(sink ->
        {
            ReconnectLoop.run(policy, control, () ->
            {
                attempts.incrementAndGet();
                return ReconnectLoop.AttemptResult.stopped();
            });
            sink.complete();
        }).subscribe();

        assertThat(attempts.get(), is(1));
        assertThat(control.sleeps, is(empty()));
    }

    private static final class RecordingControl implements ReconnectLoop.Control {
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final List<Duration> sleeps = new ArrayList<>();
        private final CountDownLatch parked = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final boolean parkBlocks;
        private int exhausted;
        private int stopAfterSleeps = -1;
        private boolean stopOnFirstSleep;

        private RecordingControl(boolean parkBlocks) {
            this.parkBlocks = parkBlocks;
        }

        @Override
        public boolean keepGoing() {
            return active.get();
        }

        @Override
        public boolean awaitBackoff(Duration delay) {
            sleeps.add(delay);
            if (stopOnFirstSleep || (stopAfterSleeps >= 0 && sleeps.size() >= stopAfterSleeps)) {
                active.set(false);
                return false;
            }
            return true;
        }

        @Override
        public void onRetry(Throwable error, ReconnectPolicy.Retry retry) {
        }

        @Override
        public void onAttemptsExhausted(Throwable error) {
            exhausted++;
        }

        @Override
        public void parkUntilStopped() {
            parked.countDown();
            if (!parkBlocks) {
                return;
            }
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
