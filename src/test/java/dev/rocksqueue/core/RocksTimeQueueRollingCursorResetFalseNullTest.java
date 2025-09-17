package dev.rocksqueue.core;

import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RocksTimeQueueRollingCursorResetFalseNullTest {

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
    void returns_early_instead_of_null_when_cursor_points_to_future() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-rolling-reset-null-");
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());

        try (RocksTimeQueue<String> q = new RocksTimeQueue<>("g", String.class, new JsonSerializer<>(), cfg)) {
            long base = System.currentTimeMillis();

            // Stage two ready items and a far-future item
            q.enqueue("a", base);
            q.enqueue("b", base);
            q.enqueue("future", base + 30_000); // far future so UB remains well below

            // Trigger refill and move rolling cursor to the future key
            assertEquals("a", q.dequeue());
            assertEquals("b", q.dequeue());

            // New earlier ready arrival AFTER cursor advanced to future
            q.enqueue("early", base);

            // Without the reset in enqueue(), the next call may return null (false-null)
            // With the reset, it must return 'early'
            assertEquals("early", q.dequeue());
        }
    }
}
