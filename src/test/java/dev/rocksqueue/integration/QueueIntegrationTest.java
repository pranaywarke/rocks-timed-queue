package dev.rocksqueue.integration;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.client.QueueClient;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class QueueIntegrationTest {

    private QueueClient client;
    private Path tmp;

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) client.close();
        if (tmp != null) {
            // best effort cleanup
            try {
                Files.walk(tmp)
                        .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> {
                            try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                        });
            } catch (Exception ignored) {}
        }
    }

    @Test
    void fifoWithinSameTimestampAndGroupIsolation() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-test-");
        QueueConfig cfg = new QueueConfig()
                .setBasePath(tmp.toString())
                .setSyncWrites(false)
                .setDisableWAL(false);

        client = new QueueClient(cfg);
        client.registerQueue("g1", String.class, new JsonSerializer<>());
        client.registerQueue("g2", String.class, new JsonSerializer<>());
        TimeQueue<String> q1 = client.getQueue("g1");
        TimeQueue<String> q2 = client.getQueue("g2");

        long ts = System.currentTimeMillis();
        q1.enqueue("a", ts);
        q1.enqueue("b", ts);
        q1.enqueue("c", ts);

        // Other group should not see q1's items
        assertNull(q2.dequeue());

        // FIFO within same timestamp
        assertEquals("a", q1.dequeue());
        assertEquals("b", q1.dequeue());
        assertEquals("c", q1.dequeue());
        assertNull(q1.dequeue());
    }

    @Test
    void delayAndPeekBehavior() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-test-");
        QueueConfig cfg = new QueueConfig().setBasePath(tmp.toString());
        client = new QueueClient(cfg);
        client.registerQueue("g", String.class, new JsonSerializer<>());
        TimeQueue<String> q = client.getQueue("g");

        long future = System.currentTimeMillis() + 200;
        q.enqueue("x", future);

        // Not ready yet
        assertNull(q.peek());
        assertNull(q.dequeue());

        Thread.sleep(250);

        // Now ready
        assertEquals("x", q.peek());
        assertEquals("x", q.dequeue());
        assertNull(q.dequeue());
    }
}
