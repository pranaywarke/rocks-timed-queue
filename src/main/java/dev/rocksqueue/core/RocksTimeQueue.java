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
import java.util.concurrent.atomic.AtomicLong;

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
    private static final int BINARY_KEY_LENGTH = 16;
    private static final String META_INSERTION_COUNTER_KEY = "meta:insertion_counter";
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
    private final AtomicBoolean resetIteratorAt = new AtomicBoolean(false);
    private final Deque<CacheEntry<T>> readyCache = new ArrayDeque<>();
    private volatile byte[] scanStartKey = null;
    
    // Metrics for monitoring and observability
    private final AtomicLong totalEnqueued = new AtomicLong(0);
    private final AtomicLong totalDequeued = new AtomicLong(0);
    private final AtomicLong cacheHits = new AtomicLong(0);
    private final AtomicLong cacheMisses = new AtomicLong(0);
    private final AtomicLong batchRefills = new AtomicLong(0);

    /**
     * Internal cache entry holding both serialized and deserialized forms of queue items.
     * 
     * @param key   16-byte encoded key (timestamp + sequence number)
     * @param value serialized payload for persistence
     * @param item  deserialized payload for fast return
     * @param <E>   the type of the cached item
     */
    private record CacheEntry<E>(byte[] key, byte[] value, E item) {
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

    /**
     * Raw cache entry holding serialized data before deserialization.
     * Used to minimize time spent in synchronized blocks during cache refill.
     * 
     * @param key       16-byte encoded key (timestamp + sequence number)
     * @param value     serialized payload
     * @param timestamp decoded timestamp for logging/debugging
     */
    private record RawCacheEntry(byte[] key, byte[] value, long timestamp) {
        public RawCacheEntry {
            Objects.requireNonNull(key, "key cannot be null");
            Objects.requireNonNull(value, "value cannot be null");
            if (key.length != BINARY_KEY_LENGTH) {
                throw new IllegalArgumentException("Key must be exactly " + BINARY_KEY_LENGTH + " bytes, got " + key.length);
            }
        }
    }

    private static String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    /**
     * Recovers the insertion counter value, preferring persisted metadata over data scanning.
     * This optimization tries O(1) metadata read before falling back to O(n) data scan.
     * 
     * @param db the RocksDB instance to read from
     * @return the recovered counter value, or 0 if no valid data exists
     */
    private long recoverCounterValue(RocksDB db) {
        // First try: Read persisted counter metadata (O(1) - fast)
        try {
            byte[] metaBytes = db.get(META_INSERTION_COUNTER_KEY.getBytes(StandardCharsets.UTF_8));
            if (metaBytes != null && metaBytes.length == 8) {
                long metaValue = ByteBuffer.wrap(metaBytes).order(ByteOrder.BIG_ENDIAN).getLong();
                logger.debug("Recovered insertion counter {} from metadata for group '{}'", metaValue, group);
                
                // Verify metadata is reasonable by checking if any data exists beyond this counter
                if (hasDataBeyondCounter(db, metaValue)) {
                    logger.warn("Metadata counter {} appears stale, falling back to data scan for group '{}'", 
                              metaValue, group);
                    return recoverCounterFromData(db);
                }
                
                return metaValue;
            }
        } catch (Exception e) {
            logger.debug("Failed to read counter metadata for group '{}', falling back to data scan: {}", 
                       group, e.getMessage());
        }
        
        // Fallback: Scan data to recover counter (O(n) - slower but reliable)
        return recoverCounterFromData(db);
    }
    
    /**
     * Checks if there's any data with sequence numbers beyond the given counter value.
     * This helps detect stale metadata counters.
     */
    private boolean hasDataBeyondCounter(RocksDB db, long counterValue) {
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
    private long recoverCounterFromData(RocksDB db) {
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


    /**
     * Creates a new RocksTimeQueue with the system UTC clock.
     * 
     * @param group      the queue group name (used for RocksDB path isolation)
     * @param type       the class type of items stored in the queue
     * @param serializer the serializer for converting items to/from bytes
     * @param config     the queue configuration
     * @throws IllegalArgumentException if any parameter is null or invalid
     * @throws RuntimeException if RocksDB initialization fails
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
     * @throws RuntimeException if RocksDB initialization fails
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
            try { db.close(); } catch (Exception ignored) {}
            throw new RuntimeException("Failed to initialize RocksDB options for group=" + group, e);
        }

        // Initialize insertion counter with recovery
        try {
            String counterPath = config.getBasePath() + File.separator + sanitize(group) + File.separator + COUNTER_FILE_NAME;
            MappedLongCounter mapped = MappedLongCounter.open(counterPath);
            long fromData = recoverCounterValue(db);
            
            if (fromData > mapped.get()) {
                logger.info("Counter recovery: updating from {} to {} for group '{}'", mapped.get(), fromData, group);
                mapped.set(fromData);
            }
            
            this.insertionCounter = mapped;
            logger.info("Initialized insertion counter at {} for group '{}'", mapped.get(), group);
        } catch (Exception e) {
            logger.error("Failed to initialize insertion counter for group '{}': {}", group, e.getMessage(), e);
            try { readOptsNoCache.close(); } catch (Exception ignored) {}
            try { writeOpts.close(); } catch (Exception ignored) {}
            try { db.close(); } catch (Exception ignored) {}
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
     * @param item the item to enqueue (must not be null)
     * @param executeAtMillis the timestamp when the item should become available for dequeue
     * @throws IllegalStateException if the queue is closed
     * @throws IllegalArgumentException if item is null
     * @throws RuntimeException if serialization or RocksDB write fails
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
        
        // Handle clock regression
        if (now < lastClock) {
            logger.warn("Clock regression detected: now={}, lastClock={} for group '{}', resetting iterator", 
                       now, lastClock, group);
            resetIteratorAt.set(true);
        }

        lastClock = now;
        
        try {
            byte[] key = BinaryKeyEncoder.encode(executeAtMillis, seq);
            byte[] value = serializer.serialize(item);
            
            db.put(writeOpts, key, value);
            
            long enqueued = totalEnqueued.incrementAndGet();
            
            if (logger.isDebugEnabled()) {
                logger.debug("Enqueued item #{} for execution at {} (seq={}) in group '{}'",
                           enqueued, Instant.ofEpochMilli(executeAtMillis), seq, group);
            }
            
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
        if (closed.get()) {
            logger.debug("Dequeue called on closed queue for group '{}'", group);
            return null;
        }
        
        // First, try to get from existing cache (fast path)
        synchronized (dequeueLock) {
            CacheEntry<T> cached = readyCache.pollFirst();
            if (cached != null) {
                long hits = cacheHits.incrementAndGet();
                long dequeued = totalDequeued.incrementAndGet();
                
                if (logger.isDebugEnabled()) {
                    logger.debug("Cache hit: dequeued item #{} from cache for group '{}' (cache hits: {})",
                               dequeued, group, hits);
                }
                
                return cached.item;
            }
        }
        
        // Cache miss - need to refill, but do deserialization outside sync block
        long misses = cacheMisses.incrementAndGet();
        
        if (logger.isDebugEnabled()) {
            logger.debug("Cache miss #{}: refilling cache for group '{}'", misses, group);
        }
        
        // Collect raw entries (inside sync block - minimal time)
        java.util.List<RawCacheEntry> rawEntries;
        synchronized (dequeueLock) {
            rawEntries = collectRawReadyEntries(Math.max(1, config.getDequeueBatchSize()));
            if (rawEntries.isEmpty()) {
                logger.trace("No items available after cache refill for group '{}'", group);
                return null;
            }
        }
        
        // Deserialize outside sync block (reduces contention)
        java.util.List<CacheEntry<T>> deserializedEntries = new java.util.ArrayList<>(rawEntries.size());
        for (RawCacheEntry rawEntry : rawEntries) {
            try {
                T item = serializer.deserialize(rawEntry.value(), type);
                deserializedEntries.add(new CacheEntry<>(rawEntry.key(), rawEntry.value(), item));
            } catch (Exception e) {
                logger.warn("Failed to deserialize item during cache refill for group '{}': {}", 
                          group, e.getMessage(), e);
                // Skip this item and continue with others
            }
        }
        
        // Add deserialized entries to cache and return first item
        synchronized (dequeueLock) {
            for (CacheEntry<T> entry : deserializedEntries) {
                readyCache.addLast(entry);
            }
            
            CacheEntry<T> next = readyCache.pollFirst();
            if (next == null) {
                logger.trace("No items available after deserialization for group '{}'", group);
                return null;
            }
            
            long dequeued = totalDequeued.incrementAndGet();
            
            if (logger.isDebugEnabled()) {
                logger.debug("Dequeued item #{} after cache refill for group '{}'", dequeued, group);
            }
            
            return next.item;
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
        if (closed.get()) {
            logger.debug("Peek called on closed queue for group '{}'", group);
            return null;
        }
        
        long now = clock.millis();
        
        try (RocksIterator it = db.newIterator(readOptsNoCache)) {
            it.seekToFirst();
            
            while (it.isValid()) {
                byte[] key = it.key();
                if (key == null || key.length != BINARY_KEY_LENGTH) {
                    logger.trace("Skipping invalid key of length {} in group '{}'", 
                               key != null ? key.length : 0, group);
                    it.next();
                    continue;
                }
                
                long ts = BinaryKeyEncoder.decodeTimestamp(key);
                if (ts > now) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Next item not ready until {} (now={}) for group '{}'", 
                                   Instant.ofEpochMilli(ts), Instant.ofEpochMilli(now), group);
                    }
                    return null; // head not ready
                }
                
                try {
                    T item = serializer.deserialize(it.value(), type);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Peeked at ready item with timestamp {} for group '{}'", 
                                   Instant.ofEpochMilli(ts), group);
                    }
                    return item;
                } catch (Exception e) {
                    logger.warn("Failed to deserialize item during peek for group '{}': {}", 
                              group, e.getMessage(), e);
                    // Continue to next item
                    it.next();
                }
            }
        } catch (Exception e) {
            logger.error("Error during peek operation for group '{}': {}", group, e.getMessage(), e);
        }
        
        return null;
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
                        for (CacheEntry<T> e : readyCache) {
                            batch.put(e.key, e.value);
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
                
                // Log final metrics
                double hitRatio = getCacheHitRatio();
                logger.info("Queue closure complete for group '{}' - Final metrics: " +
                          "enqueued={}, dequeued={}, cache hits={}, cache misses={}, hit ratio={}%, refills={}", 
                          group, totalEnqueued.get(), totalDequeued.get(), cacheHits.get(), 
                          cacheMisses.get(), String.format("%.2f", hitRatio), batchRefills.get());
            }
        } finally {
            MDC.clear();
        }
    }

    // ========== Monitoring and Metrics Methods ==========
    
    /**
     * Returns the total number of items enqueued since queue creation.
     * 
     * @return total enqueued count
     */
    public long getTotalEnqueued() {
        return totalEnqueued.get();
    }
    
    /**
     * Returns the total number of items dequeued since queue creation.
     * 
     * @return total dequeued count
     */
    public long getTotalDequeued() {
        return totalDequeued.get();
    }
    
    /**
     * Returns the number of cache hits during dequeue operations.
     * 
     * @return cache hit count
     */
    public long getCacheHits() {
        return cacheHits.get();
    }
    
    /**
     * Returns the number of cache misses during dequeue operations.
     * 
     * @return cache miss count
     */
    public long getCacheMisses() {
        return cacheMisses.get();
    }
    
    /**
     * Returns the number of batch refill operations performed.
     * 
     * @return batch refill count
     */
    public long getBatchRefills() {
        return batchRefills.get();
    }
    
    /**
     * Returns the cache hit ratio as a percentage (0.0 to 100.0).
     * 
     * @return cache hit ratio percentage, or 0.0 if no cache operations have occurred
     */
    public double getCacheHitRatio() {
        long hits = cacheHits.get();
        long misses = cacheMisses.get();
        long total = hits + misses;
        return total == 0 ? 0.0 : (hits * 100.0) / total;
    }
    
    /**
     * Returns the current size of the internal ready cache.
     * 
     * @return number of items currently cached
     */
    public int getReadyCacheSize() {
        synchronized (dequeueLock) {
            return readyCache.size();
        }
    }
    
    /**
     * Returns the queue group name.
     * 
     * @return the queue group name
     */
    public String getGroup() {
        return group;
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
        
        long refillCount = batchRefills.incrementAndGet();
        
        if (logger.isDebugEnabled()) {
            logger.debug("Starting raw entry collection #{} with batchSize={} for group '{}' (now={})", 
                       refillCount, batchSize, group, Instant.ofEpochMilli(now));
        }

        java.util.List<RawCacheEntry> rawEntries = new java.util.ArrayList<>(batchSize);

        try (Slice ubSlice = new Slice(ub);
             ReadOptions ro = new ReadOptions()
                     .setVerifyChecksums(false)
                     .setFillCache(false)
                     .setReadaheadSize(config.getReadaheadSizeBytes())
                     .setIterateUpperBound(ubSlice);
             RocksIterator it = db.newIterator(ro)) {

            int collected = 0;
            boolean usedRollingCursor = false;
            
            // Seek using rolling cursor optimization or fallback to head
            if (scanStartKey != null && !resetIteratorAt.get()) {
                it.seek(scanStartKey);
                usedRollingCursor = true;
                
                logger.trace("Using rolling cursor for raw entry collection in group '{}', iterator valid: {}", 
                           group, it.isValid());
            } else {
                it.seekToFirst();
                resetIteratorAt.set(false);
                
                logger.trace("Using full scan for raw entry collection in group '{}', iterator valid: {}", 
                           group, it.isValid());
            }

            java.util.ArrayList<RawCacheEntry> staged = new java.util.ArrayList<>(batchSize);

            while (it.isValid() && collected < batchSize) {
                byte[] k = it.key();
                if (k == null || k.length != BINARY_KEY_LENGTH) {
                    logger.trace("Skipping invalid key of length {} during raw entry collection for group '{}'", 
                               k != null ? k.length : 0, group);
                    it.next();
                    continue;
                }

                long ts = BinaryKeyEncoder.decodeTimestamp(k);
                if (ts > now) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Reached future item at {} during raw entry collection for group '{}', stopping collection", 
                                   Instant.ofEpochMilli(ts), group);
                    }
                    break; // safety; upper bound applied via iterateUpperBound
                }

                // Copy raw bytes without deserialization (fast, no lock contention)
                byte[] keyCopy = Arrays.copyOf(k, k.length);
                byte[] valCopy = Arrays.copyOf(it.value(), it.value().length);
                staged.add(new RawCacheEntry(keyCopy, valCopy, ts));
                collected++;
                
                if (logger.isTraceEnabled()) {
                    logger.trace("Collected raw entry #{} with timestamp {} for group '{}'", 
                               collected, Instant.ofEpochMilli(ts), group);
                }
                
                it.next();
            }
            
            // Update rolling cursor based on iterator state
            if (it.isValid()) {
                scanStartKey = Arrays.copyOf(it.key(), it.key().length);
                logger.trace("Updated rolling cursor to next position for group '{}'", group);
            } else {
                scanStartKey = null;
                logger.trace("Reset rolling cursor (iterator exhausted) for group '{}'", group);
            }

            if (staged.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Raw entry collection #{} found no ready items for group '{}' (used rolling cursor: {})", 
                               refillCount, group, usedRollingCursor);
                }
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

            // Successfully deleted from DB, return raw entries for deserialization
            rawEntries.addAll(staged);
            
            if (logger.isDebugEnabled()) {
                logger.debug("Raw entry collection #{} completed: collected {} items for group '{}' (used rolling cursor: {})", 
                           refillCount, staged.size(), group, usedRollingCursor);
            }
            
        } catch (Exception e) {
            logger.error("Unexpected error during raw entry collection for group '{}': {}", group, e.getMessage(), e);
            // Reset rolling cursor on unexpected errors to ensure recovery
            scanStartKey = null;
        }
        
        return rawEntries;
    }

    /**
     * Refills the ready cache with items that are eligible for dequeue.
     * This method is the core of the queue's performance optimization, using a rolling cursor
     * to avoid expensive full scans and batch processing for efficiency.
     * 
     * @param batchSize maximum number of items to load into cache
     * @deprecated Use collectRawReadyEntries for better performance with reduced lock contention
     */
    @Deprecated
    private void refillReadyCache(int batchSize) {
        final long now = clock.millis();
        byte[] ub = BinaryKeyEncoder.encode(now, Long.MAX_VALUE);
        
        long refillCount = batchRefills.incrementAndGet();
        
        if (logger.isDebugEnabled()) {
            logger.debug("Starting cache refill #{} with batchSize={} for group '{}' (now={})", 
                       refillCount, batchSize, group, Instant.ofEpochMilli(now));
        }

        try (Slice ubSlice = new Slice(ub);
             ReadOptions ro = new ReadOptions()
                     .setVerifyChecksums(false)
                     .setFillCache(false)
                     .setReadaheadSize(config.getReadaheadSizeBytes())
                     .setIterateUpperBound(ubSlice);
             RocksIterator it = db.newIterator(ro)) {

            int collected = 0;
            boolean usedRollingCursor = false;
            
            // Seek using rolling cursor optimization or fallback to head
            if (scanStartKey != null && !resetIteratorAt.get()) {
                it.seek(scanStartKey);
                usedRollingCursor = true;
                
                logger.trace("Using rolling cursor for cache refill in group '{}', iterator valid: {}", 
                           group, it.isValid());
            } else {
                it.seekToFirst();
                resetIteratorAt.set(false);
                
                logger.trace("Using full scan for cache refill in group '{}', iterator valid: {}", 
                           group, it.isValid());
            }

            java.util.ArrayList<RawCacheEntry> staged = new java.util.ArrayList<>(batchSize);

            while (it.isValid() && collected < batchSize) {
                byte[] k = it.key();
                if (k == null || k.length != BINARY_KEY_LENGTH) {
                    logger.trace("Skipping invalid key of length {} during cache refill for group '{}'", 
                               k != null ? k.length : 0, group);
                    it.next();
                    continue;
                }

                long ts = BinaryKeyEncoder.decodeTimestamp(k);
                if (ts > now) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Reached future item at {} during cache refill for group '{}', stopping collection", 
                                   Instant.ofEpochMilli(ts), group);
                    }
                    break; // safety; upper bound applied via iterateUpperBound
                }

                // Copy raw bytes without deserialization (fast, no lock contention)
                byte[] keyCopy = Arrays.copyOf(k, k.length);
                byte[] valCopy = Arrays.copyOf(it.value(), it.value().length);
                staged.add(new RawCacheEntry(keyCopy, valCopy, ts));
                collected++;
                
                if (logger.isTraceEnabled()) {
                    logger.trace("Collected item #{} with timestamp {} for group '{}'", 
                               collected, Instant.ofEpochMilli(ts), group);
                }
                
                it.next();
            }
            
            // Update rolling cursor based on iterator state
            if (it.isValid()) {
                scanStartKey = Arrays.copyOf(it.key(), it.key().length);
                logger.trace("Updated rolling cursor to next position for group '{}'", group);
            } else {
                scanStartKey = null;
                logger.trace("Reset rolling cursor (iterator exhausted) for group '{}'", group);
            }

            if (staged.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Cache refill #{} found no ready items for group '{}' (used rolling cursor: {})", 
                               refillCount, group, usedRollingCursor);
                }
                return; // nothing ready
            }

            // Bulk delete staged keys before exposing them via readyCache
            try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
                for (RawCacheEntry e : staged) {
                    batch.delete(e.key());
                }
                db.write(writeOpts, batch);
            } catch (RocksDBException e) {
                logger.error("Failed to delete staged keys during cache refill for group '{}': {}", 
                           group, e.getMessage(), e);
                // On delete failure, do not populate cache to prevent duplicates
                return;
            }

            // Successfully deleted from DB, now deserialize and populate cache outside synchronized block
            // This reduces lock contention by moving expensive deserialization out of critical section
            for (RawCacheEntry rawEntry : staged) {
                try {
                    T item = serializer.deserialize(rawEntry.value(), type);
                    CacheEntry<T> cacheEntry = new CacheEntry<>(rawEntry.key(), rawEntry.value(), item);
                    readyCache.addLast(cacheEntry);
                } catch (Exception e) {
                    logger.warn("Failed to deserialize item during cache population for group '{}': {}", 
                              group, e.getMessage(), e);
                    // Skip this item and continue with others
                }
            }
            
            if (logger.isDebugEnabled()) {
                logger.debug("Cache refill #{} completed: loaded {} items for group '{}' (used rolling cursor: {})", 
                           refillCount, staged.size(), group, usedRollingCursor);
            }
            
        } catch (Exception e) {
            logger.error("Unexpected error during cache refill for group '{}': {}", group, e.getMessage(), e);
            // Reset rolling cursor on unexpected errors to ensure recovery
            scanStartKey = null;
        }
    }

}
