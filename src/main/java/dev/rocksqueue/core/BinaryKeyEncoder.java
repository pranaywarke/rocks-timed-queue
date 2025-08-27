package dev.rocksqueue.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Encodes queue keys as fixed-length binary for correct lexicographic ordering in RocksDB.
 * Layout: [8 bytes big-endian executeAtMillis][8 bytes big-endian insertionSequence]
 */
public final class BinaryKeyEncoder {
    private BinaryKeyEncoder() {}

    public static final int KEY_LENGTH = 16;

    public static byte[] encode(long executeAtMillis, long insertionSequence) {
        if (executeAtMillis < 0 || insertionSequence < 0) {
            throw new IllegalArgumentException("executeAtMillis and insertionSequence must be non-negative");
        }
        ByteBuffer buf = ByteBuffer.allocate(KEY_LENGTH).order(ByteOrder.BIG_ENDIAN);
        buf.putLong(executeAtMillis);
        buf.putLong(insertionSequence);
        return buf.array();
    }

    public static long decodeTimestamp(byte[] key) {
        if (key == null || key.length != KEY_LENGTH) {
            throw new IllegalArgumentException("Invalid key length: expected " + KEY_LENGTH);
        }
        ByteBuffer buf = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        return buf.getLong(0);
    }

    public static long decodeSequence(byte[] key) {
        if (key == null || key.length != KEY_LENGTH) {
            throw new IllegalArgumentException("Invalid key length: expected " + KEY_LENGTH);
        }
        ByteBuffer buf = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        return buf.getLong(8);
    }
}
