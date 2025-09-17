package dev.rocksqueue.core;

import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RocksTimeQueueRollingCursorResetTest {

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
    void rolling_cursor_is_reset_for_new_earlier_key() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-rolling-reset-");
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());

        try (RocksTimeQueue<String> q = new RocksTimeQueue<>("g", String.class, new JsonSerializer<>(), cfg)) {
            long now = System.currentTimeMillis();

            // Stage two ready items and one future item so the rolling cursor points at the first future key
            q.enqueue("a", now);
            q.enqueue("b", now);
            q.enqueue("future", now + 10_000);

            // First dequeue triggers a refill and sets the rolling cursor to the future key
            assertEquals("a", q.dequeue());

            // Drain the cached remainder so the next call must scan RocksDB again
            assertEquals("b", q.dequeue());

            // Enqueue a new earlier (ready) item that sorts before the rolling cursor
            q.enqueue("early", now);

            // With the fix: iterator must reset to head and return the new earlier item
            // Without the fix: it would seek at the future key and erroneously return null here
            assertEquals("early", q.dequeue());

            // Nothing else ready; the future item remains in DB
            assertNull(q.dequeue());
        }
    }
}
