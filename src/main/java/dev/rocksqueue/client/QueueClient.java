package dev.rocksqueue.client;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.core.RocksTimeQueue;
import dev.rocksqueue.ser.Serializer;
import org.rocksdb.RocksDB;

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

    private final Map<String, TimeQueue<?>> groupQueues = new ConcurrentHashMap<>();

    static { RocksDB.loadLibrary(); }

    public QueueClient(QueueConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public <T> TimeQueue<T> getQueue(String group, Class<T> type, Serializer<T> serializer) {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(serializer, "serializer");

        @SuppressWarnings("unchecked")
        TimeQueue<T> q = (TimeQueue<T>) groupQueues.computeIfAbsent(group,
                g -> new RocksTimeQueue<>(g, type, serializer, config));
        return q;
    }

    @Override
    public void close() {
        groupQueues.values().forEach(q -> { try { ((AutoCloseable) q).close(); } catch (Exception ignored) {} });
        groupQueues.clear();
    }
}
