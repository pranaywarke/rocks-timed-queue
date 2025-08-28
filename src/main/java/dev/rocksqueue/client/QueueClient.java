package dev.rocksqueue.client;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.core.RocksTimeQueue;
import dev.rocksqueue.core.Counter;
import dev.rocksqueue.core.MappedLongCounter;
import dev.rocksqueue.ser.Serializer;
import org.rocksdb.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QueueClient manages per-group RocksDB instances and exposes a simple API to obtain queues.
 * Each queue group is a separate RocksDB instance (directory under basePath). This avoids
 * key prefixes and improves isolation.
 */
public class QueueClient implements AutoCloseable {
    private final QueueConfig config;

    private final Map<String, RocksDB> dbs = new ConcurrentHashMap<>();
    private final Map<String, Counter> groupCounters = new ConcurrentHashMap<>();
    private final Map<String, Object> groupLocks = new ConcurrentHashMap<>();

    static { RocksDB.loadLibrary(); }

    public QueueClient(QueueConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public <T> TimeQueue<T> getQueue(String group, Class<T> type, Serializer<T> serializer) {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(serializer, "serializer");
        RocksDB db = dbs.computeIfAbsent(group, this::openDBForGroup);
        Counter counter = groupCounters.computeIfAbsent(group, g -> openMappedCounterForGroup(g, db));
        Object lock = groupLocks.computeIfAbsent(group, g -> new Object());
        return new RocksTimeQueue<>(group, db, counter, type, serializer, config, lock);
    }

    private RocksDB openDBForGroup(String group) {
        try {
            String path = config.getBasePath() + File.separator + sanitize(group);
            Files.createDirectories(new File(path).toPath());
            Options options = new Options()
                    .setCreateIfMissing(true)
                    .setCompressionType(config.getCompressionType())
                    .setWriteBufferSize((long) config.getWriteBufferSizeMB() * 1024 * 1024)
                    .setMaxWriteBufferNumber(config.getMaxWriteBufferNumber());
            return RocksDB.open(options, path);
        } catch (Exception e) {
            throw new RuntimeException("Failed to open RocksDB for group=" + group, e);
        }
    }

    private Counter openMappedCounterForGroup(String group, RocksDB db) {
        String path = config.getBasePath() + File.separator + sanitize(group) + File.separator + "insertion.counter";
        MappedLongCounter mapped = MappedLongCounter.open(path);
        // compute max of mapped and data-derived to ensure monotonicity after recovery
        long fromData = recoverCounterFromData(db);
        if (fromData > mapped.get()) {
            mapped.set(fromData);
        }
        return mapped;
    }

    private static String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private long recoverCounterFromData(RocksDB db) {
        // Find the last data key (16 bytes: [ts][seq]) and extract sequence
        try (RocksIterator it = db.newIterator()) {
            it.seekToLast();
            while (it.isValid()) {
                byte[] key = it.key();
                if (key != null && key.length == 16) {
                    ByteBuffer buf = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
                    return buf.getLong(8);
                }
                it.prev();
            }
            return 0L;
        } catch (Exception e) {
            return 0L;
        }
    }

    public void persistCounter(RocksDB db, long counter) {
        try (WriteOptions wo = new WriteOptions().setSync(config.isSyncWrites()).setDisableWAL(config.isDisableWAL())) {
            ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(counter);
            db.put(wo, "meta:insertion_counter".getBytes(), buf.array());
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to persist counter", e);
        }
    }

    @Override
    public void close() {
        // best-effort persist counters
        dbs.forEach((group, db) -> {
            Counter c = groupCounters.get(group);
            if (c != null) {
                try { persistCounter(db, c.get()); } catch (Exception ignored) { }
                try { ((AutoCloseable) c).close(); } catch (Exception ignored) { }
            }
        });

        dbs.values().forEach(RocksDB::close);
        dbs.clear();
        groupCounters.clear();
    }
}
