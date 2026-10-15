package io.kestra.plugin.debezium.postgres;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.PGConnection;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumRealtimeTrigger;
import io.kestra.plugin.debezium.AbstractDebeziumTest;

import jakarta.inject.Inject;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the real realtime publisher; scheduler execution mapping is covered separately. */
@KestraTest
class CopyRealtimeRestartRegressionTest extends AbstractDebeziumTest {
    private static final int EXPECTED_ROWS = 40127;
    private static final String TABLE = "copy_realtime_events";
    private static final String SLOT = "kestra_copy_realtime_check";
    private static final String PUBLICATION = "kestra_copy_realtime_pub";

    @Inject
    private RunContextFactory runContextFactory;

    @Override
    protected String getUrl() {
        return "jdbc:postgresql://127.0.0.1:65432/";
    }

    @Override
    protected String getUsername() {
        return TestUtils.username();
    }

    @Override
    protected String getPassword() {
        return TestUtils.password();
    }

    @Test
    @Timeout(300)
    void copySurvivesRealtimePublisherRestart() throws Exception {
        PostgresDebeziumTestHelper.dropReplicationArtifacts(this::getConnection, SLOT, PUBLICATION);
        try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
            statement.execute("CREATE TABLE " + TABLE + " (event_id INT PRIMARY KEY, payload TEXT NOT NULL)");
        }
        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .hostname(Property.ofValue(TestUtils.hostname()))
            .port(Property.ofValue(TestUtils.port()))
            .username(Property.ofValue(TestUtils.username()))
            .password(Property.ofValue(TestUtils.password()))
            .database(Property.ofValue("postgres"))
            .pluginName(Property.ofValue(PostgresInterface.PluginName.PGOUTPUT))
            .snapshotMode(Property.ofValue(Capture.SnapshotMode.NEVER))
            .stateName(Property.ofValue("copy-realtime-" + IdUtils.create()))
            .slotName(Property.ofValue(SLOT))
            .publicationName(Property.ofValue(PUBLICATION))
            .includedTables(List.of("public." + TABLE))
            // Keep periodic offset flushing behind record delivery so shutdown must
            // persist the checkpoint, rather than relying on favorable timer timing.
            .properties(Property.ofValue(Map.of("offset.flush.interval.ms", "60000")))
            .maxWait(Property.ofValue(Duration.ofSeconds(20)))
            .build();
        var context = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var counts = new ConcurrentHashMap<Integer, Integer>();
        var received = new AtomicInteger();
        var failure = new AtomicReference<Throwable>();
        try {
            assertEquals(0, task.run(context).getSize());
            for (int batch = 1; batch <= 3; batch++) {
                final int currentBatch = batch;
                // The previous walsender can hold the slot for a short time after the engine stops.
                awaitSlot(false, failure, "Replication slot must be released before the next engine starts");
                RealtimeTrigger trigger = RealtimeTrigger.builder()
                    .id(task.getId())
                    .type(RealtimeTrigger.class.getName())
                    .offsetsCommitMode(Property.ofValue(AbstractDebeziumRealtimeTrigger.OffsetCommitMode.ON_EACH_BATCH))
                    .build();
                var terminated = new CountDownLatch(1);
                var subscription = Flux.from(trigger.publisher(task, context))
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe(output ->
                    {
                        int id = ((Number) output.getData().get("event_id")).intValue();
                        counts.merge(id, 1, Integer::sum);
                        int total = received.incrementAndGet();
                        if ((currentBatch == 1 && total >= 1000) || (currentBatch == 2 && total >= EXPECTED_ROWS)) {
                            trigger.stop();
                        }
                    }, error ->
                    {
                        failure.compareAndSet(null, error);
                        terminated.countDown();
                    }, terminated::countDown);
                try {
                    if (batch == 1) {
                        awaitSlot(true, failure, "Realtime pgoutput slot must be active before COPY");
                        StringBuilder csv = new StringBuilder();
                        for (int id = 1; id <= EXPECTED_ROWS; id++) {
                            csv.append(id).append(",bulk-").append(id).append('\n');
                        }
                        try (Connection connection = getConnection()) {
                            assertEquals(
                                EXPECTED_ROWS, connection.unwrap(PGConnection.class).getCopyAPI().copyIn(
                                    "COPY " + TABLE + " FROM STDIN WITH (FORMAT csv)", new StringReader(csv.toString())
                                )
                            );
                        }
                        try (
                            Connection connection = getConnection();
                            Statement statement = connection.createStatement();
                            var rows = statement.executeQuery("SELECT COUNT(*), COUNT(DISTINCT event_id) FROM " + TABLE)
                        ) {
                            assertTrue(rows.next());
                            assertEquals(EXPECTED_ROWS, rows.getLong(1));
                            assertEquals(EXPECTED_ROWS, rows.getLong(2));
                        }
                    }
                    if (batch == 3) {
                        // An empty restart is meaningful only after its engine actually
                        // reconnects; startup time must not count as the idle window.
                        awaitSlot(true, failure, "Realtime pgoutput slot must be active before observing an idle restart");
                        Thread.sleep(10000);
                        trigger.stop();
                    }
                    assertTrue(terminated.await(60, TimeUnit.SECONDS), "Realtime engine did not stop");
                    assertNull(failure.get(), "Realtime publisher failed: " + failure.get());
                    if (batch == 1) {
                        assertTrue(received.get() > 0 && received.get() < EXPECTED_ROWS, "Must restart while COPY events remain");
                    }
                } finally {
                    trigger.stop();
                    subscription.dispose();
                }
            }
            assertEquals(EXPECTED_ROWS, received.get());
            assertEquals(EXPECTED_ROWS, counts.size());
            for (int id = 1; id <= EXPECTED_ROWS; id++) {
                assertEquals(1, counts.getOrDefault(id, 0), "Realtime COPY ID " + id);
            }
        } finally {
            // Do not let an active slot in cleanup hide the original test failure.
            waitForSlotRelease();
            PostgresDebeziumTestHelper.cleanupTaskState(context, task);
            PostgresDebeziumTestHelper.dropReplicationArtifacts(this::getConnection, SLOT, PUBLICATION);
            try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + TABLE);
            }
        }
    }

    private void awaitSlot(boolean active, AtomicReference<Throwable> failure, String message) throws Exception {
        boolean reached = slotActive() == active;
        for (int attempt = 0; attempt < 300 && !reached && failure.get() == null; attempt++) {
            Thread.sleep(100);
            reached = slotActive() == active;
        }
        assertTrue(reached, message);
    }

    private void waitForSlotRelease() throws Exception {
        for (int attempt = 0; attempt < 300 && slotActive(); attempt++) {
            Thread.sleep(100);
        }
    }

    private boolean slotActive() throws Exception {
        try (
            Connection connection = getConnection();
            Statement statement = connection.createStatement();
            var slot = statement.executeQuery("SELECT active, plugin FROM pg_replication_slots WHERE slot_name = '" + SLOT + "'")
        ) {
            return slot.next() && slot.getBoolean(1) && slot.getString(2).equals("pgoutput");
        }
    }
}
