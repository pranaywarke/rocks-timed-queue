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
 * QueueClient manages per-group RocksDB instances using a Register + Get pattern.
 * Each queue group is a separate RocksDB instance (directory under basePath) for optimal isolation.
 * 
 * <p><strong>Usage Pattern:</strong>
 * <pre>{@code
 * // 1. Register queues with their types and serializers
 * client.registerQueue("user-tasks", String.class, stringSerializer);
 * client.registerQueue("events", Event.class, eventSerializer);
 * 
 * // 2. Get type-safe queue instances
 * TimeQueue<String> userTasks = client.getQueue("user-tasks");
 * TimeQueue<Event> events = client.getQueue("events");
 * }</pre>
 * 
 * <p><strong>Benefits:</strong>
 * <ul>
 *   <li>Compile-time type safety</li>
 *   <li>Clear separation of registration and usage</li>
 *   <li>Automatic handling of closed queues (recreates when needed)</li>
 *   <li>Simple, intuitive API</li>
 * </ul>
 */
public class QueueClient implements AutoCloseable {
    private final QueueConfig config;
    private final Map<String, QueueRegistration<?>> registrations = new ConcurrentHashMap<>();
    private final Map<String, TimeQueue<?>> activeQueues = new ConcurrentHashMap<>();

    static { RocksDB.loadLibrary(); }

    /**
     * Internal registration tracking queue metadata for type-safe recreation.
     */
    private static class QueueRegistration<T> {
        final Class<T> type;
        final Serializer<T> serializer;
        
        QueueRegistration(Class<T> type, Serializer<T> serializer) {
            this.type = Objects.requireNonNull(type, "type");
            this.serializer = Objects.requireNonNull(serializer, "serializer");
        }
    }

    public QueueClient(QueueConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * Registers a queue group with its type and serializer.
     * This must be called before {@link #getQueue(String)}.
     * 
     * @param group the queue group name
     * @param type the class type of items stored in the queue
     * @param serializer the serializer for converting items to/from bytes
     * @param <T> the type of items stored in the queue
     * @throws IllegalArgumentException if group is null/empty or type/serializer is null
     * @throws IllegalStateException if group is already registered with different type/serializer
     */
    public <T> void registerQueue(String group, Class<T> type, Serializer<T> serializer) {
        Objects.requireNonNull(group, "group cannot be null");
        Objects.requireNonNull(type, "type cannot be null");
        Objects.requireNonNull(serializer, "serializer cannot be null");
        
        if (group.trim().isEmpty()) {
            throw new IllegalArgumentException("group cannot be empty or whitespace");
        }

        QueueRegistration<T> newRegistration = new QueueRegistration<>(type, serializer);
        
        QueueRegistration<?> existing = registrations.putIfAbsent(group, newRegistration);
        if (existing != null) {
            // Check if registration is compatible
            if (!existing.type.equals(type) || !existing.serializer.equals(serializer)) {
                throw new IllegalStateException(
                    String.format("Queue group '%s' already registered with different type/serializer. " +
                                "Existing: %s, Requested: %s", 
                                group, existing.type.getName(), type.getName()));
            }
            // Compatible registration already exists, no-op
        }
    }

    /**
     * Gets a type-safe queue instance for the specified group.
     * The group must be registered first using {@link #registerQueue(String, Class, Serializer)}.
     * 
     * <p>If the queue instance is closed, this method will automatically create a new one.
     * 
     * @param group the queue group name
     * @param <T> the type of items stored in the queue (inferred from registration)
     * @return a type-safe queue instance
     * @throws IllegalArgumentException if group is null/empty
     * @throws IllegalStateException if group is not registered
     */
    @SuppressWarnings("unchecked")
    public <T> TimeQueue<T> getQueue(String group) {
        Objects.requireNonNull(group, "group cannot be null");
        
        if (group.trim().isEmpty()) {
            throw new IllegalArgumentException("group cannot be empty or whitespace");
        }

        QueueRegistration<T> registration = (QueueRegistration<T>) registrations.get(group);
        if (registration == null) {
            throw new IllegalStateException(
                String.format("Queue group '%s' not registered. Call registerQueue() first.", group));
        }

        return (TimeQueue<T>) activeQueues.compute(group, (g, existing) -> {
            // Check if existing queue is still healthy
            if (existing != null && !isQueueClosed(existing)) {
                return existing; // Reuse healthy queue
            }
            
            // Create new queue (first time or replacing closed one)
            return new RocksTimeQueue<>(g, registration.type, registration.serializer, config);
        });
    }

    /**
     * Checks if a queue instance is closed.
     */
    private boolean isQueueClosed(TimeQueue<?> queue) {
        return queue instanceof RocksTimeQueue && ((RocksTimeQueue<?>) queue).isClosed();
    }

    /**
     * Returns whether a queue group is registered.
     * 
     * @param group the queue group name
     * @return true if registered, false otherwise
     */
    public boolean isRegistered(String group) {
        return registrations.containsKey(group);
    }

    /**
     * Returns the number of registered queue groups.
     * 
     * @return number of registered groups
     */
    public int getRegisteredGroupCount() {
        return registrations.size();
    }

    @Override
    public void close() {
        // Close all active queues
        activeQueues.values().forEach(queue -> { 
            try { 
                ((AutoCloseable) queue).close(); 
            } catch (Exception ignored) {} 
        });
        
        // Clear all state
        activeQueues.clear();
        registrations.clear();
    }
}
