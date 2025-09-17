package dev.rocksqueue.core;

import java.util.Objects;

import static dev.rocksqueue.core.RocksTimeQueue.BINARY_KEY_LENGTH;

/**
 * Internal cache entry holding both serialized and deserialized forms of queue items.
 *
 * @param key   16-byte encoded key (timestamp + sequence number)
 * @param value serialized payload for persistence
 * @param item  deserialized payload for fast return
 * @param <E>   the type of the cached item
 */
public record CacheEntry<E>(byte[] key, byte[] value, E item) {
    /**
     * Creates a cache entry with validation of inputs.
     */
    public CacheEntry {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        Objects.requireNonNull(item, "item cannot be null");
        if (key.length != BINARY_KEY_LENGTH) {
            throw new IllegalArgumentException("Key must be exactly " + BINARY_KEY_LENGTH + " bytes, got " + key.length);
        }
    }
}