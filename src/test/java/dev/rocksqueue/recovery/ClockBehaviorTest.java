package dev.rocksqueue.recovery;

import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.core.RocksTimeQueue;
import dev.rocksqueue.ser.JsonSerializer;
import dev.rocksqueue.testing.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ClockBehaviorTest {

    static { RocksDB.loadLibrary(); }

    private Path tmp;

    @AfterEach
    void tearDown() throws Exception {
        if (tmp != null) {
            try {
                Files.walk(tmp)
                        .sorted((a,b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }

    }

    private void waitUntilNotEmpty(RocksTimeQueue<String> q, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (!q.isEmptyApproximate()) return;
            Thread.sleep(10);
        }
    }

    private String pollDequeueWithTimeout(RocksTimeQueue<String> q, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        String v;
        while (System.currentTimeMillis() < deadline) {
            v = q.dequeue();
            if (v != null) return v;
            Thread.sleep(10);
        }
        return null;
    }

    // Counter is managed by RocksTimeQueue

    private RocksTimeQueue<String> newQueue(MutableClock clock) throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-clock-");
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());
        return new RocksTimeQueue<>("g", String.class, new JsonSerializer<>(), cfg, clock);
    }

    @Test
    void readyAfterTimePasses() throws Exception {
        long base = 1_000_000L;
        MutableClock clock = new MutableClock(java.time.Instant.ofEpochMilli(base), ZoneId.of("UTC"));
        RocksTimeQueue<String> q = newQueue(clock);

        q.enqueue("a", base + 100);
        // ensure async enqueue has landed before assertions
        waitUntilNotEmpty(q, 1000);
        assertNull(q.peek());
        assertNull(q.dequeue());

        clock.advanceMillis(101);
        // Poll until available to avoid flakiness
        assertEquals("a", pollDequeueWithTimeout(q, 2000));
        assertNull(q.dequeue());
    }

    @Test
    void wallClockJumpsForward_makesItemReady() throws Exception {
        long base = 10_000L;
        MutableClock clock = new MutableClock(java.time.Instant.ofEpochMilli(base), ZoneId.of("UTC"));
        RocksTimeQueue<String> q = newQueue(clock);

        q.enqueue("x", base + 5000);
        clock.setMillis(base + 5000);
        String got = pollDequeueWithTimeout(q, 2000);
        assertEquals("x", got);
    }

    @Test
    void wallClockJumpsBackward_doesNotDeadlockBlockingDequeue() throws Exception {
        long base = 100_000L;
        MutableClock clock = new MutableClock(java.time.Instant.ofEpochMilli(base), ZoneId.of("UTC"));
        RocksTimeQueue<String> q = newQueue(clock);

        q.enqueue("y", base + 200);

        // Move backward first (simulating clock skew)
        clock.setMillis(base - 1000);
        Thread.sleep(50);
        // Now jump forward past ready time
        clock.setMillis(base + 210);
        String got = pollDequeueWithTimeout(q, 2000);
        assertEquals("y", got);
    }

    @Test
    void nonReadyHeadPreventsSkippingToLaterReadyItem() throws Exception {
        long base = 5_000L;
        MutableClock clock = new MutableClock(java.time.Instant.ofEpochMilli(base), ZoneId.of("UTC"));
        RocksTimeQueue<String> q = newQueue(clock);

        // Head not ready until base+1000, tail would be ready at base+500
        q.enqueue("head", base + 1000);
        q.enqueue("tail", base + 500);

        // Policy: FIFO within timestamps (time-ordered; FIFO among equal timestamps).
        // At base+600, tail is ready and has earlier timestamp, so it should be dequeued.
        clock.setMillis(base + 600);
        assertEquals("tail", q.dequeue());

        // Once past head's time, dequeue head; then nothing remains
        clock.setMillis(base + 1005);
        assertEquals("head", q.dequeue());
        assertNull(q.dequeue());
    }
}
