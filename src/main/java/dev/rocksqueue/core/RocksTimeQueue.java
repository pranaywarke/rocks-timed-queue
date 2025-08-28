package dev.rocksqueue.core;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.Serializer;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RocksDB-backed time-based FIFO queue with binary-encoded keys.
 * Per-queue-group instance (separate RocksDB per group).
 */
public class RocksTimeQueue<T> implements TimeQueue<T> {
    private final String group;
    private final RocksDB db;
    private final Counter insertionCounter;
    private final Class<T> type;
    private final Serializer<T> serializer;
    private final QueueConfig config;
    private final Clock clock;
    private final Object dequeueLock; // shared per-group lock when provided by client

    private final AtomicBoolean closed = new AtomicBoolean(false);

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
            long now = clock.millis();
            try (RocksIterator it = db.newIterator()) {
                it.seekToFirst();
                while (it.isValid()) {
                    byte[] key = it.key();
                    if (key == null || key.length != 16) { it.next(); continue; }
                    long ts = BinaryKeyEncoder.decodeTimestamp(key);
                    if (ts > now) {
                        return null; // head not ready yet
                    }
                    // Found ready item
                    byte[] value = it.value();
                    T item = serializer.deserialize(value, type);
                    try (WriteOptions wo = new WriteOptions().setSync(config.isSyncWrites()).setDisableWAL(config.isDisableWAL())) {
                        try {
                            db.delete(wo, key);
                        } catch (RocksDBException e) {
                            throw new RuntimeException("dequeue delete failed", e);
                        }
                    }
                    return item;
                }
            }
            return null;
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
    }
}
