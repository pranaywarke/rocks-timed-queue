package dev.rocksqueue.core;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.Serializer;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static dev.rocksqueue.core.Utils.sanitize;

/**
 * A high-performance, persistent time-based FIFO queue implementation backed by RocksDB.
 *
 * <p>This implementation provides:
 * <ul>
 *   <li>Time-based scheduling with millisecond precision</li>
 *   <li>FIFO ordering for items with identical execution times</li>
 *   <li>Persistent storage with crash recovery</li>
 *   <li>Thread-safe concurrent operations</li>
 *   <li>Optimized batch processing for high throughput</li>
 * </ul>
 *
 * <p>Each queue group maintains its own RocksDB instance for optimal isolation and performance.
 * Items are stored with binary-encoded keys containing both timestamp and sequence number,
 * ensuring proper ordering and uniqueness.
 *
 * <p><strong>Thread Safety:</strong> This class is thread-safe for concurrent enqueue/dequeue
 * operations. Dequeue operations are synchronized to maintain consistency.
 *
 * <p><strong>Resource Management:</strong> This class implements {@link AutoCloseable} and
 * should be properly closed to release RocksDB resources and persist any cached data.
 *
 * <p><strong>Usage Example:</strong>
 * <pre>{@code
 * QueueConfig config = new QueueConfig().setBasePath("/tmp/queues");
 * try (RocksTimeQueue<String> queue = new RocksTimeQueue<>("myGroup", String.class,
 *                                                          new StringSerializer(), config)) {
 *     // Schedule item for immediate execution
 *     queue.enqueue("task1", System.currentTimeMillis());
 *
 *     // Schedule item for future execution
 *     queue.enqueue("task2", System.currentTimeMillis() + 5000);
 *
 *     // Dequeue ready items
 *     String item = queue.dequeue(); // Returns "task1" immediately, null for "task2"
 * }
 * }</pre>
 *
 * @param <T> the type of items stored in the queue
 * @author RocksQueue Team
 * @since 1.0.0
 */
public class RocksTimeQueue<T> implements TimeQueue<T>, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RocksTimeQueue.class);

    // Constants for better maintainability and clarity
    static final int BINARY_KEY_LENGTH = 16;
    static final String META_INSERTION_COUNTER_KEY = "meta:insertion_counter";
    private static final String COUNTER_FILE_NAME = "insertion.counter";

    private final String group;
    private final RocksDB db;
    private final Counter insertionCounter;
    private final Class<T> type;
    private final Serializer<T> serializer;
    private final QueueConfig config;
    private final Clock clock;
    private final Object dequeueLock;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final WriteOptions writeOpts;
    private final ReadOptions readOptsNoCache;
    private volatile long lastClock = 0;
    private final AtomicBoolean resetRollingIterator = new AtomicBoolean(false);
    private final Deque<RawCacheEntry> readyCache = new ArrayDeque<>();
    private volatile byte[] rollingIteratorStartTimeStamp = null;





    /**
     * Creates a new RocksTimeQueue with the system UTC clock.
     *
     * @param group      the queue group name (used for RocksDB path isolation)
     * @param type       the class type of items stored in the queue
     * @param serializer the serializer for converting items to/from bytes
     * @param config     the queue configuration
     * @throws IllegalArgumentException if any parameter is null or invalid
     * @throws RuntimeException         if RocksDB initialization fails
     */
    public RocksTimeQueue(
            String group,
            Class<T> type,
            Serializer<T> serializer,
            QueueConfig config
    ) {
        this(group, type, serializer, config, null);
    }

    /**
     * Creates a new RocksTimeQueue with the specified clock.
     *
     * @param group      the queue group name (used for RocksDB path isolation)
     * @param type       the class type of items stored in the queue
     * @param serializer the serializer for converting items to/from bytes
     * @param config     the queue configuration
     * @param clock      the clock to use for time operations, null for system UTC
     * @throws IllegalArgumentException if any required parameter is null or invalid
     * @throws RuntimeException         if RocksDB initialization fails
     */
    public RocksTimeQueue(
            String group,
            Class<T> type,
            Serializer<T> serializer,
            QueueConfig config,
            Clock clock
    ) {
        // Validate all required parameters
        this.group = Objects.requireNonNull(group, "group cannot be null");
        this.type = Objects.requireNonNull(type, "type cannot be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");

        if (group.trim().isEmpty()) {
            throw new IllegalArgumentException("group cannot be empty or whitespace");
        }

        this.clock = (clock != null) ? clock : Clock.systemUTC();
        this.dequeueLock = new Object();

        // Set up MDC for structured logging
        MDC.put("queueGroup", group);
        MDC.put("queueType", type.getSimpleName());

        logger.info("Initializing RocksTimeQueue for group '{}' with type '{}'", group, type.getSimpleName());

        // Initialize RocksDB with proper error handling
        try {
            String path = config.getBasePath() + File.separator + sanitize(group);
            File dbDir = new File(path);

            if (!dbDir.exists() && !dbDir.mkdirs()) {
                throw new RuntimeException("Failed to create directory: " + path);
            }

            logger.debug("Opening RocksDB at path: {}", path);

            // Use try-with-resources to ensure Options is properly closed
            try (Options options = new Options()
                    .setCreateIfMissing(true)
                    .setCompressionType(config.getCompressionType())
                    .setWriteBufferSize((long) config.getWriteBufferSizeMB() * 1024 * 1024)
                    .setMaxWriteBufferNumber(config.getMaxWriteBufferNumber())) {

                this.db = RocksDB.open(options, path);
            }

            logger.info("Successfully opened RocksDB for group '{}' at path: {}", group, path);
        } catch (Exception e) {
            logger.error("Failed to open RocksDB for group '{}': {}", group, e.getMessage(), e);
            throw new RuntimeException("Failed to open RocksDB for group=" + group, e);
        }

        // Initialize reusable options
        try {
            this.writeOpts = new WriteOptions()
                    .setSync(config.isSyncWrites())
                    .setDisableWAL(config.isDisableWAL());
            this.readOptsNoCache = new ReadOptions()
                    .setVerifyChecksums(false)
                    .setFillCache(false)
                    .setReadaheadSize(config.getReadaheadSizeBytes());

            logger.debug("Initialized RocksDB options - sync: {}, WAL disabled: {}, readahead: {} bytes",
                    config.isSyncWrites(), config.isDisableWAL(), config.getReadaheadSizeBytes());
        } catch (Exception e) {
            logger.error("Failed to initialize RocksDB options for group '{}': {}", group, e.getMessage(), e);
            try {
                db.close();
            } catch (Exception ignored) {
            }
            throw new RuntimeException("Failed to initialize RocksDB options for group=" + group, e);
        }

        // Initialize insertion counter with recovery
        try {
            String counterPath = config.getBasePath() + File.separator + sanitize(group) + File.separator + COUNTER_FILE_NAME;
            MappedLongCounter mapped = MappedLongCounter.open(counterPath);
            long fromData = Utils.recoverCounterValue(db,group);

            if (fromData > mapped.get()) {
                logger.info("Counter recovery: updating from {} to {} for group '{}'", mapped.get(), fromData, group);
                mapped.set(fromData);
            }

            this.insertionCounter = mapped;
            logger.info("Initialized insertion counter at {} for group '{}'", mapped.get(), group);
        } catch (Exception e) {
            logger.error("Failed to initialize insertion counter for group '{}': {}", group, e.getMessage(), e);
            try {
                readOptsNoCache.close();
            } catch (Exception ignored) {
            }
            try {
                writeOpts.close();
            } catch (Exception ignored) {
            }
            try {
                db.close();
            } catch (Exception ignored) {
            }
            throw new RuntimeException("Failed to initialize insertion counter for group=" + group, e);
        } finally {
            MDC.clear();
        }

        logger.info("RocksTimeQueue initialization completed for group '{}'", group);
    }


    /**
     * Enqueues an item to be executed at the specified time.
     *
     * <p>If the execution time is in the past, it will be adjusted to the current time
     * to ensure immediate availability for dequeue operations.
     *
     * @param item            the item to enqueue (must not be null)
     * @param executeAtMillis the timestamp when the item should become available for dequeue
     * @throws IllegalStateException    if the queue is closed
     * @throws IllegalArgumentException if item is null
     * @throws RuntimeException         if serialization or RocksDB write fails
     */
    @Override
    public void enqueue(T item, long executeAtMillis) {
        if (closed.get()) {
            throw new IllegalStateException("Queue is closed for group: " + group);
        }

        Objects.requireNonNull(item, "item cannot be null");

        long seq = insertionCounter.incrementAndGet();
        long now = clock.millis();
        long originalExecuteAt = executeAtMillis;

        // Adjust past timestamps to current time
        if (executeAtMillis < now) {
            executeAtMillis = now;
            if (logger.isDebugEnabled()) {
                logger.debug("Adjusted past execution time from {} to {} for item in group '{}'",
                        Instant.ofEpochMilli(originalExecuteAt), Instant.ofEpochMilli(executeAtMillis), group);
            }
        }

        if(rollingIteratorStartTimeStamp!=null &&
                (executeAtMillis <= BinaryKeyEncoder.decodeTimestamp(rollingIteratorStartTimeStamp))){
            resetRollingIterator.set(true);
        }
        // Handle clock regression
        if (now < lastClock) {
            logger.warn("Clock regression detected: now={}, lastClock={} for group '{}', resetting iterator",
                    now, lastClock, group);
            resetRollingIterator.set(true);
        }

        lastClock = now;

        try {
            byte[] key = BinaryKeyEncoder.encode(executeAtMillis, seq);
            byte[] value = serializer.serialize(item);

            db.put(writeOpts, key, value);

        } catch (RocksDBException e) {
            logger.error("Failed to enqueue item in group '{}': {}", group, e.getMessage(), e);
            throw new RuntimeException("Failed to enqueue item for group=" + group, e);
        } catch (Exception e) {
            logger.error("Serialization failed for item in group '{}': {}", group, e.getMessage(), e);
            throw new RuntimeException("Failed to serialize item for group=" + group, e);
        }
    }

    /**
     * Dequeues the next available item from the queue.
     *
     * <p>This method returns items that are ready for execution (executeAtMillis &lt;= current time).
     * If no items are ready, returns null. The method uses an internal cache for performance
     * and will automatically refill from RocksDB when needed.
     *
     * @return the next available item, or null if no items are ready
     * @throws IllegalStateException if called after the queue is closed (returns null instead)
     */
    @Override
    public T dequeue() {
        return dequeueInternal(false);
    }

    private T dequeueInternal(boolean isPeek) {
        if (closed.get()) {
            logger.debug("Dequeue called on closed queue for group '{}'", group);
            return null;
        }

        RawCacheEntry entry;
        boolean wasCacheHit;

        // Minimal critical section: poll from cache; if empty, refill and poll once
        synchronized (dequeueLock) {
            entry = isPeek?readyCache.peekFirst():readyCache.pollFirst();
            wasCacheHit = (entry != null);

            if (!wasCacheHit) {

                java.util.List<RawCacheEntry> rawEntries = collectRawReadyEntries(Math.max(1, config.getDequeueBatchSize()));
                if (rawEntries.isEmpty()) {
                    return null;
                }
                for (RawCacheEntry e : rawEntries) {
                    readyCache.addLast(e);
                }
                entry = isPeek?readyCache.peekFirst():readyCache.pollFirst();
            }
        }

        // Deserialize outside the lock. On failure, throw instead of skipping to next.
        if (entry == null) {
            logger.trace("No items available for group '{}'", group);
            return null;
        }
        try {
            return serializer.deserialize(entry.value(), type);

        } catch (Exception e) {
            logger.error("Deserialization failed {} for group '{}': {}",
                    wasCacheHit ? "from cache" : "after refill", group, e.getMessage(), e);
            throw new RuntimeException("Failed to deserialize item for group=" + group, e);
        }
    }



    /**
     * Peeks at the next available item without removing it from the queue.
     *
     * <p>This method returns the item that would be returned by the next call to {@link #dequeue()},
     * but leaves it in the queue. Only items ready for execution (executeAtMillis &lt;= current time)
     * are considered.
     *
     * @return the next available item, or null if no items are ready
     */
    @Override
    public T peek() {
        return dequeueInternal(true);
    }

    /**
     * Returns an approximate count of items in the queue.
     *
     * <p>This method uses RocksDB's internal statistics and may not be perfectly accurate,
     * especially during concurrent operations. It's intended for monitoring and debugging purposes.
     *
     * @return approximate number of items in the queue, or -1 if unknown
     */
    @Override
    public long sizeApproximate() {
        if (closed.get()) {
            logger.debug("Size check called on closed queue for group '{}'", group);
            return -1;
        }

        try {
            // Get RocksDB estimate (includes meta keys, but that's minimal error)
            String prop = db.getProperty("rocksdb.estimate-num-keys");
            long rocksDbSize = prop == null ? 0 : Long.parseLong(prop.trim());

            // Add items currently in the ready cache
            int cacheSize;
            synchronized (dequeueLock) {
                cacheSize = readyCache.size();
            }

            long totalSize = rocksDbSize + cacheSize;

            logger.trace("Approximate size for group '{}': {} (RocksDB: {}, Cache: {})",
                    group, totalSize, rocksDbSize, cacheSize);

            return totalSize;
        } catch (Exception e) {
            logger.warn("Failed to get approximate size for group '{}': {}", group, e.getMessage(), e);
            return -1;
        }
    }


    /**
     * Closes this queue and releases all associated resources.
     *
     * <p>This method performs the following cleanup operations:
     * <ul>
     *   <li>Marks the queue as closed to prevent new operations</li>
     *   <li>Persists the current insertion counter value</li>
     *   <li>Restores any cached items back to RocksDB</li>
     *   <li>Closes all RocksDB resources</li>
     * </ul>
     *
     * <p>This method is idempotent and can be called multiple times safely.
     * After calling this method, all queue operations will fail or return null.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            logger.debug("Close called on already closed queue for group '{}'", group);
            return; // Already closed
        }

        logger.info("Closing RocksTimeQueue for group '{}'", group);

        // Set up MDC for structured logging during cleanup
        MDC.put("queueGroup", group);

        try {
            synchronized (dequeueLock) {
                // Persist insertion counter to metadata
                try {
                    long counterValue = insertionCounter.get();
                    ByteBuffer buf = ByteBuffer.allocate(8)
                            .order(ByteOrder.BIG_ENDIAN)
                            .putLong(counterValue);
                    db.put(writeOpts, META_INSERTION_COUNTER_KEY.getBytes(StandardCharsets.UTF_8), buf.array());

                    logger.debug("Persisted insertion counter value {} for group '{}'", counterValue, group);
                } catch (RocksDBException e) {
                    logger.warn("Failed to persist insertion counter for group '{}': {}", group, e.getMessage(), e);
                }

                // Close insertion counter
                try {
                    ((AutoCloseable) insertionCounter).close();
                    logger.debug("Closed insertion counter for group '{}'", group);
                } catch (Exception e) {
                    logger.warn("Failed to close insertion counter for group '{}': {}", group, e.getMessage(), e);
                }

                // Restore cached items back to RocksDB to prevent data loss
                if (!readyCache.isEmpty()) {
                    int cacheSize = readyCache.size();
                    logger.info("Restoring {} cached items back to RocksDB for group '{}'", cacheSize, group);

                    try (WriteBatch batch = new WriteBatch()) {
                        for (RawCacheEntry e : readyCache) {
                            batch.put(e.key(), e.value());
                        }
                        db.write(writeOpts, batch);

                        logger.debug("Successfully restored {} cached items for group '{}'", cacheSize, group);
                    } catch (RocksDBException e) {
                        logger.error("Failed to restore cached items for group '{}': {}", group, e.getMessage(), e);
                    }
                }

                // Close RocksDB options
                try {
                    readOptsNoCache.close();
                    logger.debug("Closed read options for group '{}'", group);
                } catch (Exception e) {
                    logger.warn("Failed to close read options for group '{}': {}", group, e.getMessage(), e);
                }

                try {
                    writeOpts.close();
                    logger.debug("Closed write options for group '{}'", group);
                } catch (Exception e) {
                    logger.warn("Failed to close write options for group '{}': {}", group, e.getMessage(), e);
                }

                // Clear cache and close RocksDB
                readyCache.clear();

                try {
                    db.close();
                    logger.info("Successfully closed RocksDB for group '{}'", group);
                } catch (Exception e) {
                    logger.error("Failed to close RocksDB for group '{}': {}", group, e.getMessage(), e);
                }

            }
        } finally {
            MDC.clear();
        }
    }



    /**
     * Returns whether this queue instance is closed.
     *
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Collects raw ready entries without deserialization to minimize synchronized block time.
     * This method performs the database operations and raw data collection efficiently.
     *
     * @param batchSize maximum number of items to collect
     * @return list of raw cache entries ready for deserialization
     */
    private java.util.List<RawCacheEntry> collectRawReadyEntries(int batchSize) {
        final long now = clock.millis();
        byte[] ub = BinaryKeyEncoder.encode(now, Long.MAX_VALUE);


        java.util.List<RawCacheEntry> rawEntries = new java.util.ArrayList<>(batchSize);

        try (Slice ubSlice = new Slice(ub);
             ReadOptions ro = new ReadOptions()
                     .setVerifyChecksums(false)
                     .setFillCache(false)
                     .setReadaheadSize(config.getReadaheadSizeBytes())
                     .setIterateUpperBound(ubSlice);
             RocksIterator it = db.newIterator(ro)) {

            int collected = 0;

            if (rollingIteratorStartTimeStamp != null && !resetRollingIterator.get()) {
                it.seek(rollingIteratorStartTimeStamp);
            } else {
                it.seekToFirst();
                resetRollingIterator.set(false);
            }

            java.util.ArrayList<RawCacheEntry> staged = new java.util.ArrayList<>(batchSize);

            while (it.isValid() && collected < batchSize) {
                byte[] k = it.key();
                if (k == null || k.length != BINARY_KEY_LENGTH) {
                    it.next();
                    continue;
                }

                long ts = BinaryKeyEncoder.decodeTimestamp(k);
                if (ts > now) {

                    break; // safety; upper bound applied via iterateUpperBound
                }

                // Copy raw bytes without deserialization (fast, no lock contention)
                byte[] keyCopy = Arrays.copyOf(k, k.length);
                byte[] valCopy = Arrays.copyOf(it.value(), it.value().length);
                staged.add(new RawCacheEntry(keyCopy, valCopy, ts));
                collected++;

                it.next();
            }

            if (it.isValid()) {
                rollingIteratorStartTimeStamp = Arrays.copyOf(it.key(), it.key().length);
            } else {
                rollingIteratorStartTimeStamp = null;
            }

            if (staged.isEmpty()) {

                return rawEntries; // empty list
            }

            // Bulk delete staged keys before returning raw entries
            try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
                for (RawCacheEntry e : staged) {
                    batch.delete(e.key());
                }
                db.write(writeOpts, batch);
            } catch (RocksDBException e) {
                logger.error("Failed to delete staged keys during raw entry collection for group '{}': {}",
                        group, e.getMessage(), e);
                // On delete failure, return empty list to prevent duplicates
                return rawEntries; // empty list
            }

            rawEntries.addAll(staged);


        } catch (Exception e) {
            logger.error("Unexpected error during raw entry collection for group '{}': {}", group, e.getMessage(), e);
            rollingIteratorStartTimeStamp = null;
        }

        return rawEntries;
    }

}
