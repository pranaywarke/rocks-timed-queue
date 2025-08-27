package dev.rocksqueue.recovery;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.client.QueueClient;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CrashRecoveryTest {

    static { RocksDB.loadLibrary(); }

    private QueueClient client;
    private Path tmp;

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) client.close();
        if (tmp != null) {
            try {
                Files.walk(tmp)
                        .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }
    }

    @Test
    void itemsPersistWithWAL_andCounterRecoversFromData() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-crash-");
        String group = "g1";
        long ts = System.currentTimeMillis();

        // First run: enqueue N items with WAL enabled; sync not required for WAL durability
        QueueConfig cfg1 = new QueueConfig()
                .setBasePath(tmp.toString())
                .setDisableWAL(false)
                .setSyncWrites(false);
        client = new QueueClient(cfg1);
        TimeQueue<String> q = client.getQueue(group, String.class, new JsonSerializer<>());
        q.enqueue("a", ts);
        q.enqueue("b", ts);
        q.enqueue("c", ts);

        // Simulate crash effect on counter by forcing a stale meta counter (lower than actual)
        client.close();
        client = null;
        lowerMetaCounter(tmp.resolve(group));

        // Restart: client should recover counter from last key (data) not the stale meta
        QueueConfig cfg2 = new QueueConfig().setBasePath(tmp.toString());
        client = new QueueClient(cfg2);
        TimeQueue<String> q2 = client.getQueue(group, String.class, new JsonSerializer<>());

        // Enqueue one more item at same timestamp; if counter recovery failed, this would come first
        q2.enqueue("d", ts);

        // Dequeue order must remain FIFO within same timestamp: a,b,c,d
        assertEquals("a", q2.dequeue());
        assertEquals("b", q2.dequeue());
        assertEquals("c", q2.dequeue());
        assertEquals("d", q2.dequeue());
        assertNull(q2.dequeue());
    }

    @Test
    void missingMetaCounter_recoversFromDataOrStartsAtZero() throws Exception {
        tmp = Files.createTempDirectory("rocksqueue-crash-");
        String group = "g2";
        long ts = System.currentTimeMillis();

        // First run
        QueueConfig cfg1 = new QueueConfig().setBasePath(tmp.toString());
        client = new QueueClient(cfg1);
        TimeQueue<String> q = client.getQueue(group, String.class, new JsonSerializer<>());
        q.enqueue("x", ts);
        q.enqueue("y", ts);
        client.close();
        client = null;

        // Remove meta key entirely
        deleteMetaCounter(tmp.resolve(group));

        // Restart
        QueueConfig cfg2 = new QueueConfig().setBasePath(tmp.toString());
        client = new QueueClient(cfg2);
        TimeQueue<String> q2 = client.getQueue(group, String.class, new JsonSerializer<>());
        q2.enqueue("z", ts);

        // Expect x,y,z order
        assertEquals("x", q2.dequeue());
        assertEquals("y", q2.dequeue());
        assertEquals("z", q2.dequeue());
        assertNull(q2.dequeue());
    }

    private static void lowerMetaCounter(Path groupPath) throws RocksDBException {
        try (Options opts = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, groupPath.toString())) {
            // Set meta counter to 0 (stale) regardless of actual data
            byte[] key = "meta:insertion_counter".getBytes();
            ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0L);
            db.put(key, buf.array());
        }
    }

    private static void deleteMetaCounter(Path groupPath) throws RocksDBException {
        try (Options opts = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, groupPath.toString())) {
            byte[] key = "meta:insertion_counter".getBytes();
            db.delete(key);
        }
    }
}
