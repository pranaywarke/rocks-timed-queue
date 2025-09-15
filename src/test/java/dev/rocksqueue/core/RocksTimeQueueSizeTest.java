package dev.rocksqueue.core;

import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.Utf8StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for sizeApproximate() method to ensure accurate size reporting
 * across all queue states including cache operations.
 */
class RocksTimeQueueSizeTest {

    private RocksTimeQueue<String> queue;
    private Path tempDir;
    private long now;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("rocksqueue-size-test-");
        QueueConfig config = new QueueConfig()
                .setBasePath(tempDir.toString())
                .setDequeueBatchSize(5); // Small batch size to trigger cache operations easily
        
        queue = new RocksTimeQueue<>("test-group", String.class, new Utf8StringSerializer(), config);
        now = System.currentTimeMillis();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (queue != null) {
            queue.close();
        }
        if (tempDir != null) {
            Files.walk(tempDir)
                    .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (Exception ignored) {}
                    });
        }
    }


    @Test
    void sizeApproximate_multipleItems_returnsCorrectCount() {
        // Enqueue multiple items
        for (int i = 1; i <= 10; i++) {
            queue.enqueue("item" + i, now);
            assertEquals(i, queue.sizeApproximate());
        }
    }

    @Test
    void sizeApproximate_afterDequeue_decreasesCorrectly() {
        // Enqueue items
        queue.enqueue("item1", now);
        queue.enqueue("item2", now);
        queue.enqueue("item3", now);
        assertEquals(3, queue.sizeApproximate());

        // Dequeue and verify size decreases
        assertNotNull(queue.dequeue());
        assertEquals(2, queue.sizeApproximate());

        assertNotNull(queue.dequeue());
        assertEquals(1, queue.sizeApproximate());

        assertNotNull(queue.dequeue());
        assertEquals(0, queue.sizeApproximate());
    }

    @Test
    void sizeApproximate_withCacheRefill_maintainsAccuracy() {
        // Enqueue more items than batch size to trigger cache operations
        int itemCount = 15; // Batch size is 5, so this will require multiple cache refills
        
        for (int i = 1; i <= itemCount; i++) {
            queue.enqueue("item" + i, now);
        }
        
        long sizeBeforeDequeue = queue.sizeApproximate();
        assertEquals(itemCount, sizeBeforeDequeue);

        // Dequeue items one by one and verify size accuracy
        for (int i = 0; i < itemCount; i++) {
            long expectedSize = itemCount - i;
            assertEquals(expectedSize, queue.sizeApproximate(), 
                       "Size should be " + expectedSize + " before dequeue " + (i + 1));
            
            String item = queue.dequeue();
            assertNotNull(item, "Should have item at dequeue " + (i + 1));
            
            long newSize = queue.sizeApproximate();
            assertEquals(expectedSize - 1, newSize,
                       "Size should be " + (expectedSize - 1) + " after dequeue " + (i + 1));
        }

        assertEquals(0, queue.sizeApproximate());
    }

    @Test
    void sizeApproximate_largeBatch_handlesCorrectly() {
        // Test with a larger number of items to stress test cache accounting
        int itemCount = 100;
        
        // Enqueue all items
        for (int i = 1; i <= itemCount; i++) {
            queue.enqueue("item" + i, now);
        }
        
        assertEquals(itemCount, queue.sizeApproximate());

        // Dequeue in chunks to trigger multiple cache refills
        int dequeued = 0;
        while (dequeued < itemCount) {
            String item = queue.dequeue();
            if (item != null) {
                dequeued++;
                long expectedSize = itemCount - dequeued;
                assertEquals(expectedSize, queue.sizeApproximate(),
                           "Size mismatch after dequeuing " + dequeued + " items");
            } else {
                fail("Unexpected null item when " + dequeued + " items dequeued out of " + itemCount);
            }
        }

        assertEquals(0, queue.sizeApproximate());
    }

    @Test
    void sizeApproximate_mixedEnqueueDequeue_maintainsAccuracy() {
        // Test interleaved enqueue/dequeue operations
        queue.enqueue("item1", now);
        assertEquals(1, queue.sizeApproximate());

        queue.enqueue("item2", now);
        assertEquals(2, queue.sizeApproximate());

        assertNotNull(queue.dequeue()); // Remove item1
        assertEquals(1, queue.sizeApproximate());

        queue.enqueue("item3", now);
        assertEquals(2, queue.sizeApproximate());

        queue.enqueue("item4", now);
        assertEquals(3, queue.sizeApproximate());

        assertNotNull(queue.dequeue()); // Remove item2
        assertEquals(2, queue.sizeApproximate());

        assertNotNull(queue.dequeue()); // Remove item3
        assertEquals(1, queue.sizeApproximate());

        assertNotNull(queue.dequeue()); // Remove item4
        assertEquals(0, queue.sizeApproximate());
    }

    @Test
    void sizeApproximate_futureItems_countsCorrectly() {
        long futureTime = now + 10000; // 10 seconds in future
        
        // Enqueue items for future execution
        queue.enqueue("future1", futureTime);
        queue.enqueue("future2", futureTime);
        assertEquals(2, queue.sizeApproximate());

        // These items shouldn't be ready for dequeue yet
        assertNull(queue.dequeue());
        assertEquals(2, queue.sizeApproximate()); // Size should remain the same

        // Add immediate items
        queue.enqueue("immediate1", now);
        assertEquals(3, queue.sizeApproximate());

        // Should be able to dequeue immediate item
        assertEquals("immediate1", queue.dequeue());
        assertEquals(2, queue.sizeApproximate()); // Future items still counted
    }

    @Test
    void sizeApproximate_closedQueue_returnsMinusOne() {
        queue.enqueue("item1", now);
        assertEquals(1, queue.sizeApproximate());

        queue.close();
        assertEquals(-1, queue.sizeApproximate());
    }

    @Test
    void sizeApproximate_cacheTransitions_maintainsConsistency() {
        // This test specifically targets the cache vs RocksDB accounting bug
        int batchSize = 5; // Matches our config
        
        // Enqueue exactly batch size items
        for (int i = 1; i <= batchSize; i++) {
            queue.enqueue("batch" + i, now);
        }
        assertEquals(batchSize, queue.sizeApproximate());

        // First dequeue should trigger cache refill (moves items from RocksDB to cache)
        String first = queue.dequeue();
        assertEquals("batch1", first);
        
        // Critical test: size should still be accurate after cache refill
        assertEquals(batchSize - 1, queue.sizeApproximate());

        // Continue dequeuing from cache
        for (int i = 2; i <= batchSize; i++) {
            assertEquals(batchSize - i + 1, queue.sizeApproximate());
            String item = queue.dequeue();
            assertEquals("batch" + i, item);
        }

        assertEquals(0, queue.sizeApproximate());
    }
}
