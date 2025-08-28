package dev.rocksqueue.core;

import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RocksTimeQueueEdgeTest {

    static { RocksDB.loadLibrary(); }

    private Path tmp;
    private RocksDB db;

    static class InMemCounter implements Counter {
        private final java.util.concurrent.atomic.AtomicLong al = new java.util.concurrent.atomic.AtomicLong(0);
        @Override public long get() { return al.get(); }
        @Override public long incrementAndGet() { return al.incrementAndGet(); }
        @Override public void set(long value) { al.set(value); }
    }

    private RocksTimeQueue<String> newQueue() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-edge-");
        Options opts = new Options().setCreateIfMissing(true);
        db = RocksDB.open(opts, tmp.toString());
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());
        return new RocksTimeQueue<>("g", db, new InMemCounter(), String.class, new JsonSerializer<>(), cfg);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
        if (tmp != null) {
            try {
                Files.walk(tmp)
                        .sorted((a,b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }
    }

    @Test
    void enqueueRejectsNegativeTimestamp() throws Exception {
        RocksTimeQueue<String> q = newQueue();
        assertThrows(IllegalArgumentException.class, () -> q.enqueue("x", -1));
    }

    @Test
    void closedQueueBehavior() throws Exception {
        RocksTimeQueue<String> q = newQueue();
        q.close();
        assertNull(q.dequeue());
        assertNull(q.peek());
        assertThrows(IllegalStateException.class, () -> q.enqueue("x", System.currentTimeMillis()));
    }

    @Test
    void ignoresInvalidKeysInDb() throws Exception {
        RocksTimeQueue<String> q = newQueue();
        long now = System.currentTimeMillis();

        // Insert an invalid key (not 16 bytes), and then a valid ready item
        byte[] badKey = "badkey".getBytes(StandardCharsets.UTF_8); // length 6
        db.put(badKey, "bad".getBytes(StandardCharsets.UTF_8));

        byte[] validKey = BinaryKeyEncoder.encode(now, 1L);
        db.put(validKey, new JsonSerializer<String>().serialize("ok"));

        // Peek/Dequeue should skip invalid key and return the valid one
        assertEquals("ok", q.peek());
        assertEquals("ok", q.dequeue());
        assertNull(q.dequeue());
    }

    @Test
    void enqueueDelayed_zeroDelayReady() throws Exception {
        RocksTimeQueue<String> q = newQueue();
        q.enqueueDelayed("immediate", 0);
        assertEquals("immediate", q.dequeue());
        assertNull(q.dequeue());
    }
}
