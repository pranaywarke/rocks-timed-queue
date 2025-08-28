package dev.rocksqueue.integration;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.client.QueueClient;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that multiple consumer threads within the same JVM do not produce duplicates
 * thanks to per-group dequeue serialization. This test does not validate cross-process behavior.
 */
public class QueueConcurrencyTest {

    private Path tmpDir;
    private QueueClient client;

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) client.close();
        if (tmpDir != null) {
            try {
                Files.walk(tmpDir)
                        .sorted((a,b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }
    }

    private void initClient() throws IOException {
        tmpDir = Files.createTempDirectory("rocksqueue-concurrency-");
        QueueConfig cfg = new QueueConfig().setBasePath(tmpDir.toString());
        client = new QueueClient(cfg);
    }

    @Test
    void multiThreadedDequeue_hasNoDuplicates_andGetsAll() throws Exception {
        initClient();
        String group = "concurrency-group";
        TimeQueue<String> q = client.getQueue(group, String.class, new JsonSerializer<>());

        int total = 2000;
        long now = System.currentTimeMillis();
        for (int i = 0; i < total; i++) {
            // unique payloads to detect duplicates
            q.enqueue("item-" + i, now);
        }

        int threads = 6;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        ConcurrentSkipListSet<String> seen = new ConcurrentSkipListSet<>();
        AtomicInteger received = new AtomicInteger(0);

        Runnable consumer = () -> {
            long deadline = System.currentTimeMillis() + 10_000; // 10s max per thread
            while (System.currentTimeMillis() < deadline && received.get() < total) {
                String v = q.dequeue();
                if (v == null) {
                    try { Thread.sleep(1); } catch (InterruptedException ignored) {}
                    continue;
                }
                boolean first = seen.add(v);
                assertTrue(first, "Duplicate detected: " + v);
                received.incrementAndGet();
            }
        };

        for (int i = 0; i < threads; i++) pool.submit(consumer);
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS), "Consumers did not finish in time");

        assertEquals(total, received.get(), "Did not receive all items");
        assertEquals(total, seen.size(), "Set size mismatch");
        assertNull(q.dequeue(), "Queue should be empty now");
        q.close();
    }
}
