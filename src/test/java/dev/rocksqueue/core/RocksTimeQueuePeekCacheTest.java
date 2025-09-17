package dev.rocksqueue.core;

import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RocksTimeQueuePeekCacheTest {

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

    @Test
    void peek_returns_cached_item_when_ready_cache_has_entries() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-peek-cache-");
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());

        try (RocksTimeQueue<String> q = new RocksTimeQueue<>("g", String.class, new JsonSerializer<>(), cfg)) {
            long ts = System.currentTimeMillis();
            q.enqueue("a", ts);
            q.enqueue("b", ts);
            q.enqueue("c", ts);

            // Force a refill: dequeue will stage remaining ready items into the in-memory cache
            assertEquals("a", q.dequeue());

            // Now DB no longer has the staged entries; the next ready items live only in readyCache.
            // Current implementation of peek() ignores the in-memory cache, so this assertion
            // is expected to FAIL until peek() is fixed to consult readyCache first.
            assertEquals("b", q.peek());
        }
    }
}
