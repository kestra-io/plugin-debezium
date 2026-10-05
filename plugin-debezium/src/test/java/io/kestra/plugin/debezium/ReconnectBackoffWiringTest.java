package io.kestra.plugin.debezium;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.TriggerContext;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class ReconnectBackoffWiringTest {

    @Test
    void stopWakesAwaitBackoff() throws Exception {
        var trigger = new FixtureTrigger();
        var returned = new AtomicReference<Boolean>();
        var waiter = new Thread(() -> returned.set(trigger.awaitBackoff(Duration.ofHours(1))));

        waiter.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (waiter.getState() != Thread.State.TIMED_WAITING) {
            if (!waiter.isAlive() || System.nanoTime() > deadline) {
                throw new AssertionError("awaitBackoff did not block on the reconnect monitor");
            }
            Thread.onSpinWait();
        }

        trigger.stop();
        waiter.join(5_000);

        assertThat(waiter.isAlive(), is(false));
        assertThat(returned.get(), is(false));
        trigger.stop();
    }

    static class FixtureTrigger extends AbstractDebeziumRealtimeTrigger {
        @Override
        public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) {
            throw new UnsupportedOperationException();
        }
    }
}
