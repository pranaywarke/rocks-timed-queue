package dev.rocksqueue.client;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class QueueClientTest {

    static { RocksDB.loadLibrary(); }

    private QueueClient client;
    private Path tmp;

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) client.close();
        if (tmp != null) {
            try {
                Files.walk(tmp)
                        .sorted((a,b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }
    }

    @Test
    void separateDbPerGroup_andIsolation() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-client-");
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());
        client = new QueueClient(cfg);

        TimeQueue<String> q1 = client.getQueue("g1", String.class, new JsonSerializer<>());
        TimeQueue<String> q2 = client.getQueue("g2", String.class, new JsonSerializer<>());

        long ts = System.currentTimeMillis();
        q1.enqueue("a", ts);
        q2.enqueue("b", ts);

        assertEquals("a", q1.dequeue());
        assertNull(q1.dequeue());
        assertEquals("b", q2.dequeue());
        assertNull(q2.dequeue());

        // Both group directories should exist under base path
        assertTrue(Files.isDirectory(tmp.resolve("g1")));
        assertTrue(Files.isDirectory(tmp.resolve("g2")));
    }

    @Test
    void sanitizeGroupNames_createsDirectories_andQueueWorks() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-client-");
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());
        client = new QueueClient(cfg);

        String ugly = "my/group?name*";
        TimeQueue<String> q = client.getQueue(ugly, String.class, new JsonSerializer<>());
        long ts = System.currentTimeMillis();
        q.enqueue("x", ts);
        assertEquals("x", q.dequeue());

        // Directory should be sanitized (non-alnum replaced with '_')
        String expected = ugly.replaceAll("[^a-zA-Z0-9._-]", "_");
        assertTrue(Files.isDirectory(tmp.resolve(expected)));
    }

    @Test
    void sameGroupQueues_shareDequeueLock_noDuplicates() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-client-");
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());
        client = new QueueClient(cfg);

        String group = "concurrent";
        TimeQueue<String> q1 = client.getQueue(group, String.class, new JsonSerializer<>());
        TimeQueue<String> q2 = client.getQueue(group, String.class, new JsonSerializer<>());

        int total = 500;
        long now = System.currentTimeMillis();
        for (int i = 0; i < total; i++) q1.enqueue("v-" + i, now);

        int threads = 4;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        ConcurrentSkipListSet<String> seen = new ConcurrentSkipListSet<>();
        AtomicInteger received = new AtomicInteger(0);

        Runnable consumer = () -> {
            long deadline = System.currentTimeMillis() + 8000;
            while (System.currentTimeMillis() < deadline && received.get() < total) {
                String v = (ThreadLocalRandom.current().nextBoolean() ? q1 : q2).dequeue();
                if (v == null) { try { Thread.sleep(1); } catch (InterruptedException ignored) {} ; continue; }
                boolean first = seen.add(v);
                assertTrue(first, "Duplicate: " + v);
                received.incrementAndGet();
            }
        };
        for (int i = 0; i < threads; i++) pool.submit(consumer);
        pool.shutdown();
        assertTrue(pool.awaitTermination(12, TimeUnit.SECONDS));

        assertEquals(total, received.get());
        assertEquals(total, seen.size());
    }

    @Test
    void persistCounterOnClose_writesMetaKey() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-client-");
        String group = "gmeta";
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());
        client = new QueueClient(cfg);
        TimeQueue<String> q = client.getQueue(group, String.class, new JsonSerializer<>());

        long ts = System.currentTimeMillis();
        q.enqueue("a", ts);
        q.enqueue("b", ts);
        client.close();
        client = null;

        // Open DB directly and verify meta key exists
        try (Options opts = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, tmp.resolve(group.replaceAll("[^a-zA-Z0-9._-]", "_")).toString())) {
            byte[] val = db.get("meta:insertion_counter".getBytes());
            assertNotNull(val, "meta:insertion_counter should exist after close()");
        } catch (RocksDBException e) {
            fail(e);
        }
    }
}
