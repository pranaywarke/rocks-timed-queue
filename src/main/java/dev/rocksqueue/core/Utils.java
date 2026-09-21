package dev.rocksqueue.core;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import static dev.rocksqueue.core.RocksTimeQueue.BINARY_KEY_LENGTH;
import static dev.rocksqueue.core.RocksTimeQueue.META_INSERTION_COUNTER_KEY;

public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    private static final Pattern UNSAFE_CHARACTERS = Pattern.compile("[^a-zA-Z0-9._-]");

    /**
     * Reduces a queue group name to a single safe path segment.
     *
     * <p>Every separator is already replaced, but "." and ".." survive the character filter
     * and are resolved by the filesystem rather than treated as names, so a group called
     * ".." would place its directory outside the configured base path. Both are rejected,
     * as are blank names, because a group directory must be a name and not a traversal.
     *
     * @param name the group name to reduce
     * @return the sanitized single path segment
     * @throws IllegalArgumentException if the name is blank or resolves to a path traversal
     */
    public static String sanitize(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Queue group name must not be blank");
        }
        String sanitized = UNSAFE_CHARACTERS.matcher(name).replaceAll("_");
        if (sanitized.equals(".") || sanitized.equals("..")) {
            throw new IllegalArgumentException(
                    "Queue group name must not resolve to a path traversal: " + name);
        }
        return sanitized;
    }

    /**
     * The number of items one refill may move from RocksDB into the ready cache.
     * A caller may ask for any size; the result is at least 1 and never more than
     * {@code max}, so a misconfigured batch cannot outgrow the cache it fills.
     */
    public static int clampBatchSize(int requested, int max) {
        if (requested <= 0) {
            return 1;
        }
        return Math.min(requested, max);
    }




    /**
     * Recovers the insertion counter value, preferring persisted metadata over data scanning.
     * This optimization tries O(1) metadata read before falling back to O(n) data scan.
     *
     * @param db the RocksDB instance to read from
     * @return the recovered counter value, or 0 if no valid data exists
     */
    public static long recoverCounterValue(RocksDB db,String group) {
        // First try: Read persisted counter metadata (O(1) - fast)
        try {
            byte[] metaBytes = db.get(META_INSERTION_COUNTER_KEY.getBytes(StandardCharsets.UTF_8));
            if (metaBytes != null && metaBytes.length == 8) {
                long metaValue = ByteBuffer.wrap(metaBytes).order(ByteOrder.BIG_ENDIAN).getLong();
                logger.debug("Recovered insertion counter {} from metadata for group '{}'", metaValue, group);

                // Verify metadata is reasonable by checking if any data exists beyond this counter
                if (hasDataBeyondCounter(db,group, metaValue)) {
                    logger.warn("Metadata counter {} appears stale, falling back to data scan for group '{}'",
                            metaValue, group);
                    return recoverCounterFromData(db,group);
                }

                return metaValue;
            }
        } catch (Exception e) {
            logger.debug("Failed to read counter metadata for group '{}', falling back to data scan: {}",
                    group, e.getMessage());
        }

        // Fallback: Scan data to recover counter (O(n) - slower but reliable)
        return recoverCounterFromData(db,group);
    }

    /**
     * Checks if there's any data with sequence numbers beyond the given counter value.
     * This helps detect stale metadata counters.
     */
    private static boolean hasDataBeyondCounter(RocksDB db, String group, long counterValue) {
        try (RocksIterator it = db.newIterator()) {
            it.seekToLast();
            if (it.isValid()) {
                byte[] key = it.key();
                if (key != null && key.length == BINARY_KEY_LENGTH) {
                    ByteBuffer buf = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
                    long lastSeq = buf.getLong(8);
                    return lastSeq > counterValue;
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to check data beyond counter for group '{}': {}", group, e.getMessage());
        }
        return false;
    }

    /**
     * Recovers the insertion counter value from existing data in RocksDB.
     * This is used as a fallback when metadata is unavailable or stale.
     *
     * @param db the RocksDB instance to scan
     * @return the highest sequence number found, or 0 if no valid keys exist
     */
    private static long recoverCounterFromData(RocksDB db,String group) {
        logger.debug("Recovering insertion counter from existing data for group '{}'", group);
        try (RocksIterator it = db.newIterator()) {
            it.seekToLast();
            while (it.isValid()) {
                byte[] key = it.key();
                if (key != null && key.length == BINARY_KEY_LENGTH) {
                    ByteBuffer buf = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
                    long recoveredSeq = buf.getLong(8);
                    logger.info("Recovered insertion counter value {} from existing data for group '{}'",
                            recoveredSeq, group);
                    return recoveredSeq;
                }
                it.prev();
            }
            logger.debug("No existing data found for counter recovery in group '{}'", group);
            return 0L;
        } catch (Exception e) {
            logger.warn("Failed to recover counter from data for group '{}', starting from 0", group, e);
            return 0L;
        }
    }

}
