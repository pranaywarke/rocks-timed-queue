package dev.rocksqueue.recovery;

import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.core.RocksTimeQueue;
import dev.rocksqueue.ser.JsonSerializer;
import dev.rocksqueue.testing.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

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
    private RocksDB db;
    private ExecutorService writePool;
    private ExecutorService readPool;
    private ScheduledExecutorService maintenancePool;

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
        if (writePool != null) writePool.shutdownNow();
        if (readPool != null) readPool.shutdownNow();
        if (maintenancePool != null) maintenancePool.shutdownNow();
        if (tmp != null) {
            try {
                Files.walk(tmp)
                        .sorted((a,b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }
    }

    private RocksTimeQueue<String> newQueue(MutableClock clock) throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-clock-");
        Options opts = new Options().setCreateIfMissing(true);
        db = RocksDB.open(opts, tmp.toString());
        writePool = Executors.newFixedThreadPool(2, r -> new Thread(r, "tq-write-"+UUID.randomUUID()));
        readPool = Executors.newFixedThreadPool(2, r -> new Thread(r, "tq-read-"+UUID.randomUUID()));
        maintenancePool = Executors.newScheduledThreadPool(1, r -> new Thread(r, "tq-maint-"+UUID.randomUUID()));
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());
        return new RocksTimeQueue<>("g", db, new AtomicLong(0), String.class, new JsonSerializer<>(), writePool, readPool, maintenancePool, cfg, clock);
    }

    @Test
    void readyAfterTimePasses() throws Exception {
        long base = 1_000_000L;
        MutableClock clock = new MutableClock(java.time.Instant.ofEpochMilli(base), ZoneId.of("UTC"));
        RocksTimeQueue<String> q = newQueue(clock);

        q.enqueue("a", base + 100);
        assertNull(q.peek());
        assertNull(q.dequeue());

        clock.advanceMillis(101);
        assertEquals("a", q.peek());
        assertEquals("a", q.dequeue());
        assertNull(q.dequeue());
    }

    @Test
    void wallClockJumpsForward_makesItemReady() throws Exception {
        long base = 10_000L;
        MutableClock clock = new MutableClock(java.time.Instant.ofEpochMilli(base), ZoneId.of("UTC"));
        RocksTimeQueue<String> q = newQueue(clock);

        q.enqueue("x", base + 5000);
        clock.setMillis(base + 5000);
        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            Future<String> fut = es.submit(() -> {
                try { return q.dequeueBlocking(); } catch (InterruptedException e) { return null; }
            });
            String got = fut.get(2, TimeUnit.SECONDS);
            assertEquals("x", got);
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    void wallClockJumpsBackward_doesNotDeadlockBlockingDequeue() throws Exception {
        long base = 100_000L;
        MutableClock clock = new MutableClock(java.time.Instant.ofEpochMilli(base), ZoneId.of("UTC"));
        RocksTimeQueue<String> q = newQueue(clock);

        q.enqueue("y", base + 200);

        ExecutorService es = Executors.newSingleThreadExecutor();
        Future<String> fut = es.submit(() -> {
            try {
                return q.dequeueBlocking();
            } catch (InterruptedException e) { return null; }
        });

        // Move backward first (simulating clock skew)
        clock.setMillis(base - 1000);
        Thread.sleep(50);
        // Now jump forward past ready time
        clock.setMillis(base + 210);

        String got = fut.get(2, TimeUnit.SECONDS);
        es.shutdownNow();
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

        // At base+600, tail would be ready but head not; must not skip head
        clock.setMillis(base + 600);
        assertNull(q.dequeue());

        // Once past head's time, head then tail should come out
        clock.setMillis(base + 1005);
        assertEquals("head", q.dequeue());
        assertEquals("tail", q.dequeue());
        assertNull(q.dequeue());
    }
}
