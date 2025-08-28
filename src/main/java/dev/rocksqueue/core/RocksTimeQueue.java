package dev.rocksqueue.core;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.Serializer;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.ReadOptions;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RocksDB-backed time-based FIFO queue with binary-encoded keys.
 * Per-queue-group instance (separate RocksDB per group).
 */
public class RocksTimeQueue<T> implements TimeQueue<T>, AutoCloseable {
    private final String group;
    private final RocksDB db;
    private final Counter insertionCounter;
    private final Class<T> type;
    private final Serializer<T> serializer;
    private final QueueConfig config;
    private final Clock clock;
    private final Object dequeueLock; // shared per-group lock when provided by client

    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Dequeue fast-path state (accessed only under dequeueLock)
    // We store key+value to be able to restore the cache on clean shutdown without losing items.
    private static final class CacheEntry<E> {
        final byte[] key;    // 16-byte encoded key (ts, seq)
        final byte[] value;  // serialized payload
        final E item;        // deserialized payload for fast return
        CacheEntry(byte[] key, byte[] value, E item) { this.key = key; this.value = value; this.item = item; }
    }
    private final Deque<CacheEntry<T>> readyCache = new ArrayDeque<>();
    // Rolling cursor to avoid expensive seekToFirst each refill; updated to successor of last processed key
    private byte[] scanStartKey = null;

    // Resume point removed: we always start scans from head with a fresh iterator

    public RocksTimeQueue(
            String group,
            RocksDB db,
            Counter insertionCounter,
            Class<T> type,
            Serializer<T> serializer,
            QueueConfig config
    ) {
        this(group, db, insertionCounter, type, serializer, config, Clock.systemUTC(), null);
    }

    // Alternate constructor allowing custom Clock injection (used in tests)
    public RocksTimeQueue(
            String group,
            RocksDB db,
            Counter insertionCounter,
            Class<T> type,
            Serializer<T> serializer,
            QueueConfig config,
            Clock clock
    ) {
        this(group, db, insertionCounter, type, serializer, config, clock, null);
    }

    // Constructor used by QueueClient to provide a shared per-group dequeue lock
    public RocksTimeQueue(
            String group,
            RocksDB db,
            Counter insertionCounter,
            Class<T> type,
            Serializer<T> serializer,
            QueueConfig config,
            Object dequeueLock
    ) {
        this(group, db, insertionCounter, type, serializer, config, Clock.systemUTC(), dequeueLock);
    }

    // Internal canonical constructor
    private RocksTimeQueue(
            String group,
            RocksDB db,
            Counter insertionCounter,
            Class<T> type,
            Serializer<T> serializer,
            QueueConfig config,
            Clock clock,
            Object dequeueLock
    ) {
        this.group = Objects.requireNonNull(group, "group");
        this.db = Objects.requireNonNull(db, "db");
        this.insertionCounter = Objects.requireNonNull(insertionCounter, "insertionCounter");
        this.type = Objects.requireNonNull(type, "type");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.dequeueLock = (dequeueLock != null) ? dequeueLock : new Object();
    }

    @Override
    public void enqueue(T item, long executeAtMillis) {
        if (closed.get()) throw new IllegalStateException("Queue is closed");
        if (executeAtMillis < 0) throw new IllegalArgumentException("executeAtMillis must be >= 0");
        long seq = insertionCounter.incrementAndGet();
        byte[] key = BinaryKeyEncoder.encode(executeAtMillis, seq);
        byte[] value = serializer.serialize(item);
        try (WriteOptions wo = new WriteOptions().setSync(config.isSyncWrites()).setDisableWAL(config.isDisableWAL())) {
            db.put(wo, key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException("enqueue failed", e);
        }
    }

    @Override
    public T dequeue() {
        if (closed.get()) return null;
        synchronized (dequeueLock) {
            // Serve from cache if available
            CacheEntry<T> cached = readyCache.pollFirst();
            if (cached != null) return cached.item;

            // Refill cache from DB up to now
            refillReadyCache(Math.max(1, config.getDequeueBatchSize()));
            CacheEntry<T> next = readyCache.pollFirst();
            return next == null ? null : next.item;
        }
    }

    @Override
    public T peek() {
        if (closed.get()) return null;
        long now = clock.millis();
        try (RocksIterator it = db.newIterator()) {
            it.seekToFirst();
            while (it.isValid()) {
                byte[] key = it.key();
                if (key == null || key.length != 16) { it.next(); continue; }
                long ts = BinaryKeyEncoder.decodeTimestamp(key);
                if (ts > now) {
                    return null; // head not ready
                }
                return serializer.deserialize(it.value(), type);
            }
        }
        return null;
    }

    @Override
    public long sizeApproximate() {
        try {
            // estimate-num-keys is per DB; since each group is a separate DB, it's fine.
            String prop = db.getProperty("rocksdb.estimate-num-keys");
            return prop == null ? -1 : Long.parseLong(prop.trim());
        } catch (Exception e) {
            return -1; // unknown
        }
    }

    @Override
    public boolean isEmptyApproximate() {
        long s = sizeApproximate();
        if (s >= 0) return s == 0;
        // Fallback: quick iterator check
        try (RocksIterator it = db.newIterator()) {
            it.seekToFirst();
            while (it.isValid()) {
                byte[] key = it.key();
                if (key != null && key.length == 16) return false;
                it.next();
            }
            return true;
        }
    }

    @Override
    public void close() {
        closed.set(true);
        synchronized (dequeueLock) {
            // On clean shutdown, persist cached entries back into RocksDB using their original keys
            if (!readyCache.isEmpty()) {
                // Force durability for restore: always WAL-enabled and fsync to ensure items survive power loss
                try (WriteOptions wo = new WriteOptions().setSync(true).setDisableWAL(false);
                     WriteBatch wb = new WriteBatch()) {
                    for (CacheEntry<T> e : readyCache) {
                        wb.put(e.key, e.value);
                    }
                    db.write(wo, wb);
                } catch (RocksDBException e) {
                    // Best-effort restore; if it fails we still clear the cache to avoid memory retention
                }
            }
            readyCache.clear();
        }
    }

    private void refillReadyCache(int batchSize) {
        final long now = clock.millis();
        byte[] ub = BinaryKeyEncoder.encode(now, Long.MAX_VALUE);

        try (Slice ubSlice = new Slice(ub);
             ReadOptions ro = new ReadOptions()
                 .setVerifyChecksums(false)
                 .setFillCache(false)
                 .setReadaheadSize(config.getReadaheadSizeBytes())
                 .setIterateUpperBound(ubSlice);
             RocksIterator it = db.newIterator(ro);
             WriteOptions wo = new WriteOptions().setSync(config.isSyncWrites()).setDisableWAL(config.isDisableWAL());
             WriteBatch wb = new WriteBatch()) {
            long tSeekAccum = 0L;
            long tScanStart = System.nanoTime();

            int collected = 0;
            byte[] lastKeyProcessed = null;

            // Phase 1: seek to rolling cursor (if present) and scan to ub
            long tSeekStart = System.nanoTime();
            if (scanStartKey != null) {
                it.seek(scanStartKey);
            } else {
                it.seekToFirst();
            }
            long tSeekEnd = System.nanoTime();
            tSeekAccum += (tSeekEnd - tSeekStart);

            while (it.isValid() && collected < batchSize) {
                byte[] k = it.key();
                if (k == null || k.length != 16) { it.next(); continue; }

                long ts = BinaryKeyEncoder.decodeTimestamp(k);
                if (ts > now) break; // safety; upper bound applied via iterateUpperBound

                byte[] keyCopy = java.util.Arrays.copyOf(k, k.length);
                byte[] valCopy = java.util.Arrays.copyOf(it.value(), it.value().length);
                T item = serializer.deserialize(valCopy, type);
                readyCache.addLast(new CacheEntry<>(keyCopy, valCopy, item));
                wb.singleDelete(keyCopy);
                lastKeyProcessed = keyCopy;
                collected++;
                it.next();
            }

            // Phase 2: wrap to the beginning and scan up to scanStartKey (exclusive) if batch not filled
            if (collected < batchSize && scanStartKey != null) {
                tSeekStart = System.nanoTime();
                it.seekToFirst();
                tSeekEnd = System.nanoTime();
                tSeekAccum += (tSeekEnd - tSeekStart);

                while (it.isValid() && collected < batchSize) {
                    byte[] k = it.key();
                    if (k == null || k.length != 16) { it.next(); continue; }
                    // Stop before reaching the old scanStartKey to avoid reprocessing
                    if (compareBytes(k, scanStartKey) >= 0) break;

                    long ts = BinaryKeyEncoder.decodeTimestamp(k);
                    if (ts > now) break;

                    byte[] keyCopy = java.util.Arrays.copyOf(k, k.length);
                    byte[] valCopy = java.util.Arrays.copyOf(it.value(), it.value().length);
                    T item = serializer.deserialize(valCopy, type);
                    readyCache.addLast(new CacheEntry<>(keyCopy, valCopy, item));
                    wb.singleDelete(keyCopy);
                    lastKeyProcessed = keyCopy;
                    collected++;
                    it.next();
                }
            }

            long tScanEnd = System.nanoTime();

            if (collected == 0) {
                // Nothing ready
                return;
            }

            long tWriteStart = System.nanoTime();
            db.write(wo, wb);
            long tWriteEnd = System.nanoTime();

            // Debug timings: seek, scan+deserialize, delete(commit)
            long seekMs = tSeekAccum / 1_000_000;
            long scanMs = (tScanEnd - tScanStart) / 1_000_000;
            long delMs  = (tWriteEnd - tWriteStart) / 1_000_000;
//            System.out.println("[rocksqueue] group=" + group +
//                    " batchItems=" + collected +
//                    " seekMs=" + seekMs +
//                    " scanMs=" + scanMs +
//                    " deleteMs=" + delMs);

            // Advance rolling cursor to successor of the last processed key
            if (lastKeyProcessed != null) {
                scanStartKey = lexicographicSuccessor(lastKeyProcessed);
            }
        } catch (RocksDBException e) {
            // On failure, clear cache to avoid duplicates and do NOT advance lastKey
            readyCache.clear();
            throw new RuntimeException("dequeue batch delete failed", e);
        }
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int ai = a[i] & 0xFF;
            int bi = b[i] & 0xFF;
            if (ai != bi) return ai - bi;
        }
        return a.length - b.length;
    }

    // Compute lexicographic successor for a fixed-length big-endian key
    private static byte[] lexicographicSuccessor(byte[] key) {
        if (key == null) return null;
        byte[] out = java.util.Arrays.copyOf(key, key.length);
        for (int i = out.length - 1; i >= 0; i--) {
            int b = out[i] & 0xFF;
            if (b != 0xFF) {
                out[i] = (byte) (b + 1);
                for (int j = i + 1; j < out.length; j++) out[j] = 0;
                return out;
            }
        }
        // overflow, no successor
        return null;
    }

}
