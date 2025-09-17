package dev.rocksqueue.core;

import java.util.Objects;

import static dev.rocksqueue.core.RocksTimeQueue.BINARY_KEY_LENGTH;

/**
 * Raw cache entry holding serialized data before deserialization.
 * Used to minimize time spent in synchronized blocks during cache refill.
 *
 * @param key       16-byte encoded key (timestamp + sequence number)
 * @param value     serialized payload
 * @param timestamp decoded timestamp for logging/debugging
 */
public record RawCacheEntry(byte[] key, byte[] value, long timestamp) {
    public RawCacheEntry {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        if (key.length != BINARY_KEY_LENGTH) {
            throw new IllegalArgumentException("Key must be exactly " + BINARY_KEY_LENGTH + " bytes, got " + key.length);
        }
    }
}