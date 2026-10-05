package io.kestra.plugin.debezium;

import java.time.Duration;

/**
 * Runs connection attempts one at a time on the calling thread.
 * The next attempt starts only after the previous attempt has returned and the backoff wait has elapsed.
 */
final class ReconnectLoop {

    private ReconnectLoop() {
    }

    static void run(ReconnectPolicy policy, Control control, Attempt attempt) {
        while (control.keepGoing()) {
            AttemptResult result = attempt.run();
            if (result.wasStopped() || !control.keepGoing()) {
                return;
            }

            var retry = policy.afterFailure(result.stable());
            if (retry.isEmpty()) {
                control.onAttemptsExhausted(result.error());
                control.parkUntilStopped();
                return;
            }

            control.onRetry(result.error(), retry.get());
            if (!control.awaitBackoff(retry.get().delay())) {
                return;
            }
        }
    }

    @FunctionalInterface
    interface Attempt {
        AttemptResult run();
    }

    interface Control {
        boolean keepGoing();

        /**
         * @return {@code false} when the wait ends because the trigger was stopped or cancelled
         */
        boolean awaitBackoff(Duration delay);

        void onRetry(Throwable error, ReconnectPolicy.Retry retry);

        void onAttemptsExhausted(Throwable error);

        void parkUntilStopped();
    }

    static final class AttemptResult {
        private final Throwable error;
        private final boolean stable;
        private final boolean stopped;

        private AttemptResult(Throwable error, boolean stable, boolean stopped) {
            this.error = error;
            this.stable = stable;
            this.stopped = stopped;
        }

        static AttemptResult stopped() {
            return new AttemptResult(null, false, true);
        }

        static AttemptResult failure(Throwable error, boolean stable) {
            return new AttemptResult(error, stable, false);
        }

        Throwable error() {
            return error;
        }

        boolean stable() {
            return stable;
        }

        boolean wasStopped() {
            return stopped;
        }
    }
}
