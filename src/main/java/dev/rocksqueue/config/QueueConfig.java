package dev.rocksqueue.config;

import org.rocksdb.CompressionType;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class QueueConfig {
    // System property helpers for test configurability (safe fallbacks)
    private static String prop(String key, String def) {
        String v = System.getProperty(key);
        return v == null ? def : v;
    }
    private static boolean boolProp(String key, boolean def) {
        String v = System.getProperty(key);
        return v == null ? def : Boolean.parseBoolean(v);
    }
    private static int intProp(String key, int def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return def; }
    }
    private static CacheType cacheTypeProp() {
        try { return CacheType.valueOf(prop("rq.cacheType", "IN_MEMORY")); }
        catch (Exception ignored) { return CacheType.IN_MEMORY; }
    }
    // Paths
    private String basePath = "./data/rocksqueue";

    // RocksDB
    private boolean syncWrites = false;       // WAL fsync on each write
    private boolean disableWAL = false;       // keep WAL by default
    private int writeBufferSizeMB = 64;       // per memtable
    private int maxWriteBufferNumber = 3;
    private CompressionType compressionType = CompressionType.LZ4_COMPRESSION;

    // Queue behavior
    private int dequeueBatchSize = 2000;       // number of ready items to collect per iterator scan
    private long readaheadSizeBytes = 16L * 1024 * 1024; // 4MB readahead for iterator scans

    // Ready cache
    private CacheType cacheType = cacheTypeProp(); // IN_MEMORY or MEMORY_MAPPED
    private int cacheFileSizeMB = intProp("rq.cacheFileSizeMB", 16); // fixed-size circular file capacity (MB)
    private boolean cacheStrictCrc = boolProp("rq.cacheStrictCrc", true); // validate CRCs on load and read
    private boolean cacheForceOnWrite = boolProp("rq.cacheForceOnWrite", true); // fsync header and entries on each append
    private int cacheMinFileSizeMB = intProp("rq.cacheMinFileSizeMB", 4); // enforce a safe minimum capacity
    private int cacheMaxRecordBytes = intProp("rq.cacheMaxRecordBytes", 5 * 1024 * 1024); // per-record payload limit (5MB)
    private CacheSizeChangePolicy cacheSizeChangePolicy = CacheSizeChangePolicy.FORBID; // policy on capacity mismatch at reopen
}
