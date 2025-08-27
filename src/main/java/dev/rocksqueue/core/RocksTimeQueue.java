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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * RocksDB-backed time-based FIFO queue with binary-encoded keys.
 * Per-queue-group instance (separate RocksDB per group).
 */
public class RocksTimeQueue<T> implements TimeQueue<T> {
    private static final byte[] META_COUNTER_KEY = "meta:insertion_counter".getBytes();

    private final String group;
    private final RocksDB db;
    private final AtomicLong insertionCounter;
    private final Class<T> type;
    private final Serializer<T> serializer;
    private final ExecutorService writePool;
    private final ExecutorService readPool;
    private final ScheduledExecutorService maintenancePool;
    private final QueueConfig config;
    private final Clock clock;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ReentrantLock waitLock = new ReentrantLock();
    private final Condition notEmptyOrReady = waitLock.newCondition();

    public RocksTimeQueue(
            String group,
            RocksDB db,
            AtomicLong insertionCounter,
            Class<T> type,
            Serializer<T> serializer,
            ExecutorService writePool,
            ExecutorService readPool,
            ScheduledExecutorService maintenancePool,
            QueueConfig config
    ) {
        this.group = Objects.requireNonNull(group, "group");
        this.db = Objects.requireNonNull(db, "db");
        this.insertionCounter = Objects.requireNonNull(insertionCounter, "insertionCounter");
        this.type = Objects.requireNonNull(type, "type");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.writePool = Objects.requireNonNull(writePool, "writePool");
        this.readPool = Objects.requireNonNull(readPool, "readPool");
        this.maintenancePool = Objects.requireNonNull(maintenancePool, "maintenancePool");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Clock.systemUTC();

        // Periodically persist the counter to reduce loss window on crash
        this.maintenancePool.scheduleAtFixedRate(this::persistCounterSafe, 5, 5, TimeUnit.SECONDS);
    }

    // Alternate constructor allowing custom Clock injection (used in tests)
    public RocksTimeQueue(
            String group,
            RocksDB db,
            AtomicLong insertionCounter,
            Class<T> type,
            Serializer<T> serializer,
            ExecutorService writePool,
            ExecutorService readPool,
            ScheduledExecutorService maintenancePool,
            QueueConfig config,
            Clock clock
    ) {
        this.group = Objects.requireNonNull(group, "group");
        this.db = Objects.requireNonNull(db, "db");
        this.insertionCounter = Objects.requireNonNull(insertionCounter, "insertionCounter");
        this.type = Objects.requireNonNull(type, "type");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.writePool = Objects.requireNonNull(writePool, "writePool");
        this.readPool = Objects.requireNonNull(readPool, "readPool");
        this.maintenancePool = Objects.requireNonNull(maintenancePool, "maintenancePool");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");

        this.maintenancePool.scheduleAtFixedRate(this::persistCounterSafe, 5, 5, TimeUnit.SECONDS);
    }

    @Override
    public void enqueue(T item, long executeAtMillis) {
        if (closed.get()) throw new IllegalStateException("Queue is closed");
        if (executeAtMillis < 0) throw new IllegalArgumentException("executeAtMillis must be >= 0");
        long seq = insertionCounter.incrementAndGet();
        byte[] key = BinaryKeyEncoder.encode(executeAtMillis, seq);
        byte[] value = serializer.serialize(item);

        Runnable task = () -> {
            try (WriteOptions wo = new WriteOptions().setSync(config.isSyncWrites()).setDisableWAL(config.isDisableWAL())) {
                db.put(wo, key, value);
            } catch (RocksDBException e) {
                throw new RuntimeException("enqueue failed", e);
            }
        };
        // Writes can be async for throughput
        writePool.execute(task);
        // Wake up any waiting consumers to re-evaluate readiness
        signalWaiters();
    }

    @Override
    public T dequeue() {
        if (closed.get()) return null;
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

    @Override
    public T dequeueBlocking() throws InterruptedException {
        while (!closed.get()) {
            // Try fast path first
            T v = dequeue();
            if (v != null) return v;

            // Determine how long to wait based on the timestamp of the head key, if any
            long now = clock.millis();
            Long nextTs = null;
            try (RocksIterator it = db.newIterator()) {
                it.seekToFirst();
                while (it.isValid()) {
                    byte[] k = it.key();
                    if (k == null || k.length != 16) { it.next(); continue; }
                    nextTs = BinaryKeyEncoder.decodeTimestamp(k); break;
                }
            }

            waitLock.lock();
            try {
                if (closed.get()) return null;
                if (nextTs == null) {
                    // Nothing in queue: await a bounded time and retry on signal or timeout
                    notEmptyOrReady.await(200, TimeUnit.MILLISECONDS);
                } else {
                    long delay = Math.max(0, nextTs - now);
                    if (delay == 0) {
                        // Should be ready; loop will retry immediately
                    } else {
                        notEmptyOrReady.await(Math.min(delay, 500), TimeUnit.MILLISECONDS);
                    }
                }
            } finally {
                waitLock.unlock();
            }
        }
        return null;
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
        if (closed.compareAndSet(false, true)) {
            persistCounterSafe();
            signalWaiters();
        }
    }

    private void persistCounterSafe() {
        try (WriteOptions wo = new WriteOptions().setSync(true)) { // force durable counter
            long c = insertionCounter.get();
            byte[] buf = java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.BIG_ENDIAN).putLong(c).array();
            db.put(wo, META_COUNTER_KEY, buf);
        } catch (Exception ignored) {
        }
    }

    private void signalWaiters() {
        waitLock.lock();
        try {
            notEmptyOrReady.signalAll();
        } finally {
            waitLock.unlock();
        }
    }
}
