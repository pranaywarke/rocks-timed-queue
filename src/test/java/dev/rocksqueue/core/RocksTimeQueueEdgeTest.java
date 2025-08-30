package dev.rocksqueue.core;

import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RocksTimeQueueEdgeTest {

    static { RocksDB.loadLibrary(); }

    private Path tmp;
    // DB is now managed by RocksTimeQueue

    static class InMemCounter implements Counter {
        private final java.util.concurrent.atomic.AtomicLong al = new java.util.concurrent.atomic.AtomicLong(0);
        @Override public long get() { return al.get(); }
        @Override public long incrementAndGet() { return al.incrementAndGet(); }
        @Override public void set(long value) { al.set(value); }
    }

    private RocksTimeQueue<String> newQueue() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-edge-");
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());
        return new RocksTimeQueue<>("g", String.class, new JsonSerializer<>(), cfg);
    }

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
        // Prepare DB with invalid and valid keys before constructing queue
        tmp = Files.createTempDirectory("rocksqueue-edge-");
        java.nio.file.Files.createDirectories(tmp.resolve("g"));
        try (Options opts = new Options().setCreateIfMissing(true);
             RocksDB raw = RocksDB.open(opts, tmp.resolve("g").toString())) {
            long now = System.currentTimeMillis();
            byte[] badKey = "badkey".getBytes(StandardCharsets.UTF_8); // length 6
            raw.put(badKey, "bad".getBytes(StandardCharsets.UTF_8));
            byte[] validKey = BinaryKeyEncoder.encode(now, 1L);
            raw.put(validKey, new JsonSerializer<String>().serialize("ok"));
        }

        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());
        RocksTimeQueue<String> q = new RocksTimeQueue<>("g", String.class, new JsonSerializer<>(), cfg);

        // Peek/Dequeue should skip invalid key and return the valid one
        assertEquals("ok", q.peek());
        assertEquals("ok", q.dequeue());
        assertNull(q.dequeue());
    }


}
