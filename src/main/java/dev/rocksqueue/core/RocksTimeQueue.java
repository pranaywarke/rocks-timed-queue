package dev.rocksqueue.core;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.Serializer;
import org.rocksdb.*;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
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
    private final Object dequeueLock; // per-group lock internal to this instance

    private final AtomicBoolean closed = new AtomicBoolean(false);
    // Reusable options
    private final org.rocksdb.WriteOptions writeOpts;
    private final org.rocksdb.ReadOptions readOptsNoCache;
    private long lastClock = 0;
    private AtomicBoolean resetIteratorAt = new AtomicBoolean(false);
    private final Deque<CacheEntry<T>> readyCache = new ArrayDeque<>();
    // Rolling cursor to avoid expensive seekToFirst each refill; updated to successor of last processed key
    private byte[] scanStartKey = null;

    // Resume point removed: we always start scans from head with a fresh iterator

    // Dequeue fast-path state (accessed only under dequeueLock)
    // We store key+value to be able to restore the cache on clean shutdown without losing items.
    private static final class CacheEntry<E> {
        final byte[] key;    // 16-byte encoded key (ts, seq)
        final byte[] value;  // serialized payload
        final E item;        // deserialized payload for fast return

        CacheEntry(byte[] key, byte[] value, E item) {
            this.key = key;
            this.value = value;
            this.item = item;
        }
    }

    private static String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private long recoverCounterFromData(RocksDB db) {
        try (RocksIterator it = db.newIterator()) {
            it.seekToLast();
            while (it.isValid()) {
                byte[] key = it.key();
                if (key != null && key.length == 16) {
                    java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(key).order(java.nio.ByteOrder.BIG_ENDIAN);
                    return buf.getLong(8);
                }
                it.prev();
            }
            return 0L;
        } catch (Exception e) {
            return 0L;
        }
    }


    // Public constructors: RocksTimeQueue manages its own DB, counter and lock per group
    public RocksTimeQueue(
            String group,
            Class<T> type,
            Serializer<T> serializer,
            QueueConfig config
    ) {
        this(group, type, serializer, config, null);
    }

    public RocksTimeQueue(
            String group,
            Class<T> type,
            Serializer<T> serializer,
            QueueConfig config,
            Clock clock
    ) {
        this.group = Objects.requireNonNull(group, "group");
        this.type = Objects.requireNonNull(type, "type");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = (clock != null) ? clock : Clock.systemUTC();
        this.dequeueLock = new Object();

        // Open per-group RocksDB
        try {
            String path = config.getBasePath() + java.io.File.separator + sanitize(group);
            new java.io.File(path).mkdirs();
            org.rocksdb.Options options = new org.rocksdb.Options()
                    .setCreateIfMissing(true)
                    .setCompressionType(config.getCompressionType())
                    .setWriteBufferSize((long) config.getWriteBufferSizeMB() * 1024 * 1024)
                    .setMaxWriteBufferNumber(config.getMaxWriteBufferNumber());
            this.db = org.rocksdb.RocksDB.open(options, path);
        } catch (Exception e) {
            throw new RuntimeException("Failed to open RocksDB for group=" + group, e);
        }

        // Initialize reusable options
        this.writeOpts = new org.rocksdb.WriteOptions()
                .setSync(config.isSyncWrites())
                .setDisableWAL(config.isDisableWAL());
        this.readOptsNoCache = new org.rocksdb.ReadOptions()
                .setVerifyChecksums(false)
                .setFillCache(false)
                .setReadaheadSize(config.getReadaheadSizeBytes());

        // Open mapped counter and recover from data if needed
        String counterPath = config.getBasePath() + java.io.File.separator + sanitize(group) + java.io.File.separator + "insertion.counter";
        MappedLongCounter mapped = MappedLongCounter.open(counterPath);
        long fromData = recoverCounterFromData(db);
        if (fromData > mapped.get()) mapped.set(fromData);
        this.insertionCounter = mapped;
    }


    AtomicLong temp = new AtomicLong();

    @Override
    public void enqueue(T item, long executeAtMillis) {
        if (closed.get()) throw new IllegalStateException("Queue is closed");
        long seq = insertionCounter.incrementAndGet();
        long now = clock.millis();
        if (executeAtMillis < now) {
            executeAtMillis = now;
        }
        if (now < lastClock) {
            resetIteratorAt.set(true);
        }

        lastClock = now;
        byte[] key = BinaryKeyEncoder.encode(executeAtMillis, seq);
        byte[] value = serializer.serialize(item);
        try {
            db.put(writeOpts, key, value);
        } catch (org.rocksdb.RocksDBException e) {
            throw new RuntimeException("enqueue failed", e);
        }
    }

    @Override
    public T dequeue() {
        if (closed.get()) return null;
        synchronized (dequeueLock) {
            CacheEntry<T> cached = readyCache.pollFirst();
            if (cached != null) {
                return cached.item;
            }
            // Refill cache from DB up to now
            refillReadyCache(Math.max(1, config.getDequeueBatchSize()));
            CacheEntry<T> next = readyCache.pollFirst();
            if (next == null) return null;
            return next.item;
        }
    }

    @Override
    public T peek() {
        if (closed.get()) return null;
        long now = clock.millis();
        try (RocksIterator it = db.newIterator(readOptsNoCache)) {
            it.seekToFirst();
            while (it.isValid()) {
                byte[] key = it.key();
                if (key == null || key.length != 16) {
                    it.next();
                    continue;
                }
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
        try (RocksIterator it = db.newIterator(readOptsNoCache)) {
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
            // best-effort persist counter to a meta key
            try {
                java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.BIG_ENDIAN).putLong(insertionCounter.get());
                db.put(writeOpts, "meta:insertion_counter".getBytes(), buf.array());
            } catch (org.rocksdb.RocksDBException ignored) {
            }
            try {
                ((AutoCloseable) insertionCounter).close();
            } catch (Exception ignored) {
            }
            if (!readyCache.isEmpty()) {
                try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
                    for (CacheEntry<T> e : readyCache) {
                        batch.put(e.key, e.value);
                    }
                    db.write(writeOpts, batch);
                } catch (org.rocksdb.RocksDBException ignored) {
                }
            }
            try {
                readOptsNoCache.close();
            } catch (Exception ignored) {
            }
            try {
                writeOpts.close();
            } catch (Exception ignored) {
            }
            readyCache.clear();
            db.close();
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
             RocksIterator it = db.newIterator(ro)) {

            int collected = 0;
            // Seek using rolling cursor only if it's within the current upper bound; otherwise fallback to head
            if (scanStartKey != null && !resetIteratorAt.get()) {
                it.seek(scanStartKey);
            } else {
                it.seekToFirst();
                resetIteratorAt.set(false);
            }

            java.util.ArrayList<CacheEntry<T>> staged = new java.util.ArrayList<>(batchSize);

            while (it.isValid() && collected < batchSize) {
                byte[] k = it.key();
                if (k == null || k.length != 16) {
                    it.next();
                    continue;
                }

                long ts = BinaryKeyEncoder.decodeTimestamp(k);
                if (ts > now) break; // safety; upper bound applied via iterateUpperBound

                byte[] keyCopy = java.util.Arrays.copyOf(k, k.length);
                byte[] valCopy = java.util.Arrays.copyOf(it.value(), it.value().length);
                T item = serializer.deserialize(valCopy, type);
                staged.add(new CacheEntry<>(keyCopy, valCopy, item));
                collected++;
                it.next();
            }
            if (it.isValid()) {
                scanStartKey = it.key();
            }else{
                scanStartKey = null;
            }


            if (staged.isEmpty()) {
                // Nothing ready; force next scan to seekToFirst() to avoid any missed-window starvation
                // scanStartKey = null;
                return; // nothing ready
            }

            // Bulk delete staged keys before exposing them via readyCache
            try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
                for (CacheEntry<T> e : staged) {
                    batch.delete(e.key);
                }
                db.write(writeOpts, batch);
            } catch (org.rocksdb.RocksDBException e) {
                // On delete failure, do not populate cache to prevent duplicates
                return;
            }

            for (CacheEntry<T> e : staged) {
                readyCache.addLast(e);
            }
            //   scanStartKey = lastKeyProcessed;
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
