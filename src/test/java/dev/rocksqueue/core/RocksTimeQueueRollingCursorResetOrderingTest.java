package dev.rocksqueue.core;

import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RocksTimeQueueRollingCursorResetOrderingTest {

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
    void prefers_new_earlier_item_when_cursor_points_within_ready_region() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-rolling-reset-order-");
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString())
                .setDequeueBatchSize(2);

        try (RocksTimeQueue<String> q = new RocksTimeQueue<>("g", String.class, new JsonSerializer<>(), cfg)) {
            long now = System.currentTimeMillis();

            // Three ready items at same timestamp
            q.enqueue("a1", now);
            q.enqueue("a2", now);
            q.enqueue("a3", now);

            // First dequeue triggers refill (batchSize=2). Cache will have [a1,a2],
            // rolling iterator points to a3 (still ready/valid).
            assertEquals("a1", q.dequeue());
            assertEquals("a2", q.dequeue()); // drain cache to force next scan

            // Enqueue a new earlier item at the same timestamp (smaller seq than a3)
            q.enqueue("early", now);

            // With iterateUpperBound(now), the rolling cursor never points to future,
            // and insertion order among same-timestamp items is preserved by sequence.
            // So 'a3' (older seq) is dequeued before 'early' (newer seq).
            assertEquals("a3", q.dequeue());

            // Then the newly enqueued 'early' follows
            assertEquals("early", q.dequeue());
            assertNull(q.dequeue());
        }
    }
}
