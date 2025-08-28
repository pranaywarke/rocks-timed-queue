package dev.rocksqueue.recovery;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.client.QueueClient;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ShutdownRestoreTest {

    static { RocksDB.loadLibrary(); }

    private QueueClient client;
    private TimeQueue<String> queue;
    private Path tmp;

    @AfterEach
    void tearDown() throws Exception {
        try { if (queue != null) queue.close(); } catch (Exception ignored) {}
        try { if (client != null) client.close(); } catch (Exception ignored) {}
        if (tmp != null) {
            try {
                Files.walk(tmp)
                        .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }
    }

    @Test
    void shutdown_persists_ready_cache_and_recovers_on_restart() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-shutdown-");
        String group = "g-cache";

        // Create client/queue
        QueueConfig cfg = new QueueConfig()
                .setBasePath(tmp.toString())
                .setDisableWAL(false)   // default durable WAL settings
                .setSyncWrites(false)
                .setDequeueBatchSize(2000);
        client = new QueueClient(cfg);
        queue = client.getQueue(group, String.class, new JsonSerializer<>());

        long ts = System.currentTimeMillis();
        int n = 10;
        for (int i = 0; i < n; i++) {
            queue.enqueue("m" + i, ts);
        }

        // Trigger a refill: this will batch-collect all ready items into the in-memory cache,
        // delete them from DB, and return the first item. The rest remain in cache.
        String first = queue.dequeue();
        assertEquals("m0", first);

        // Clean shutdown: queue.close() must persist cached entries back to RocksDB
        queue.close();
        queue = null;
        client.close();
        client = null;

        // Restart fresh client/queue
        client = new QueueClient(new QueueConfig().setBasePath(tmp.toString()));
        queue = client.getQueue(group, String.class, new JsonSerializer<>());

        // Dequeue remaining; should see m1..m9 in order
        List<String> got = new ArrayList<>();
        String v;
        while ((v = queue.dequeue()) != null) {
            got.add(v);
        }
        assertEquals(n - 1, got.size(), "should recover all remaining cached items after restart");
        for (int i = 1; i < n; i++) {
            assertEquals("m" + i, got.get(i - 1));
        }
    }
}
