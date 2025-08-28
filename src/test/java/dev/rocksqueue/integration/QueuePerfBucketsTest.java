package dev.rocksqueue.integration;

import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.core.Counter;
import dev.rocksqueue.core.RocksTimeQueue;
import dev.rocksqueue.ser.JsonSerializer;
import dev.rocksqueue.testing.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bucketed performance/behavior integration test.
 * - Enqueues items into 5-minute buckets (configurable)
 * - Advances time bucket-by-bucket using a test clock
 * - Dequeues per bucket, verifies FIFO within timestamp (bucket) and counts items
 * - Prints throughput metrics
 *
 * Configurable via system properties (defaults chosen for CI):
 *   - q.totalMinutes (default: 30)
 *   - q.bucketMinutes (default: 5)
 *   - q.itemsPerBucket (default: 100)
 *   - q.valueSizeBytes (default: 16)
 *   - q.syncWrites (default: false)
 *   - q.disableWAL (default: false)
 */
class QueuePerfBucketsTest {

    static { RocksDB.loadLibrary(); }

    private Path tmp;
    private RocksDB db;

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
        if (tmp != null) {
            try {
                Files.walk(tmp)
                        .sorted((a,b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }
    }

    static class InMemCounter implements Counter {
        private final AtomicLong al = new AtomicLong(0);
        @Override public long get() { return al.get(); }
        @Override public long incrementAndGet() { return al.incrementAndGet(); }
        @Override public void set(long value) { al.set(value); }
    }

    private static int getIntProp(String key, int def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return def; }
    }
    private static boolean getBoolProp(String key, boolean def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        return Boolean.parseBoolean(v);
    }

    @Test
    @Tag("perf")
    void bucketed_enqueue_dequeue_throughput_and_fifo() throws Exception {
        // Params
        int totalMinutes = getIntProp("q.totalMinutes", 30);
        int bucketMinutes = getIntProp("q.bucketMinutes", 5);
        int itemsPerBucket = getIntProp("q.itemsPerBucket", 100);
        int valueSizeBytes = getIntProp("q.valueSizeBytes", 16);
        boolean syncWrites = getBoolProp("q.syncWrites", false);
        boolean disableWAL = getBoolProp("q.disableWAL", false);

        int buckets = Math.max(1, totalMinutes / bucketMinutes);
        long bucketMillis = TimeUnit.MINUTES.toMillis(bucketMinutes);

        tmp = Files.createTempDirectory("rocksqueue-perf-");
        Options opts = new Options().setCreateIfMissing(true);
        db = RocksDB.open(opts, tmp.toString());

        long base = 1_000_000L;
        MutableClock clock = new MutableClock(java.time.Instant.ofEpochMilli(base), ZoneId.of("UTC"));
        QueueConfig cfg = new QueueConfig()
                .setBasePath(tmp.toString())
                .setSyncWrites(syncWrites)
                .setDisableWAL(disableWAL);

        RocksTimeQueue<String> q = new RocksTimeQueue<>(
                "g", db, new InMemCounter(), String.class, new JsonSerializer<>(), cfg, clock);

        // Prepare payload template to fixed size
        String prefix = "item-";
        String padUnit = "x";
        StringBuilder sb = new StringBuilder();
        while (sb.length() < valueSizeBytes) sb.append(padUnit);
        String pad = sb.substring(0, valueSizeBytes);

        // Enqueue all buckets
        long enqStart = System.nanoTime();
        long totalItems = 0;
        for (int b = 0; b < buckets; b++) {
            long ts = base + b * bucketMillis;
            for (int i = 0; i < itemsPerBucket; i++) {
                String v = prefix + b + ":" + i + ":" + pad; // encodes bucket and in-bucket sequence
                q.enqueue(v, ts);
                totalItems++;
            }
        }
        long enqEnd = System.nanoTime();

        double enqSec = (enqEnd - enqStart) / 1_000_000_000.0;
        double enqThroughput = totalItems / Math.max(1e-6, enqSec);

        // Dequeue per bucket and verify FIFO within bucket
        long deqStart = System.nanoTime();
        long seen = 0;
        for (int b = 0; b < buckets; b++) {
            clock.setMillis(base + b * bucketMillis);
            List<String> got = new ArrayList<>(itemsPerBucket);
            while (true) {
                String v = q.dequeue();
                if (v == null) break; // head not ready or empty
                long thisBucket = parseBucket(v);
                if (thisBucket != b) {
                    // Not ready for this item yet; since ordering is by timestamp then FIFO within same timestamp,
                    // we should not encounter a future bucket here. If we do, it means head wasn't ready and dequeue returned null earlier.
                    // So reaching here would be unexpected.
                    fail("Dequeued item from future bucket: expected=" + b + " got=" + thisBucket);
                }
                got.add(v);
            }
            assertEquals(itemsPerBucket, got.size(), "bucket " + b + " count");
            // Verify FIFO within bucket by sequence number
            for (int i = 0; i < got.size(); i++) {
                int seq = parseSeq(got.get(i));
                assertEquals(i, seq, "fifo order in bucket " + b);
            }
            seen += got.size();
        }
        long deqEnd = System.nanoTime();

        assertEquals(totalItems, seen, "total items dequeued");

        double deqSec = (deqEnd - deqStart) / 1_000_000_000.0;
        double deqThroughput = totalItems / Math.max(1e-6, deqSec);

        System.out.println("QueuePerfBucketsTest summary:");
        System.out.println("  buckets=" + buckets + " bucketMinutes=" + bucketMinutes + " itemsPerBucket=" + itemsPerBucket +
                " totalItems=" + totalItems);
        System.out.println("  enqueue: " + String.format("%.2f", enqThroughput) + " items/sec in " + String.format("%.2f", enqSec) + "s");
        System.out.println("  dequeue: " + String.format("%.2f", deqThroughput) + " items/sec in " + String.format("%.2f", deqSec) + "s");
    }

    private static long parseBucket(String v) {
        // format: item-<bucket>:<seq>:<pad>
        int p1 = v.indexOf('-');
        int p2 = v.indexOf(':', p1 + 1);
        return Long.parseLong(v.substring(p1 + 1, p2));
    }
    private static int parseSeq(String v) {
        int p1 = v.indexOf('-');
        int p2 = v.indexOf(':', p1 + 1);
        int p3 = v.indexOf(':', p2 + 1);
        return Integer.parseInt(v.substring(p2 + 1, p3));
    }
}
