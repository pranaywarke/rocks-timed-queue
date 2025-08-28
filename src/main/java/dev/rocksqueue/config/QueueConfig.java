package dev.rocksqueue.config;

import org.rocksdb.CompressionType;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class QueueConfig {
    // Paths
    private String basePath = "./data/rocksqueue";

    // RocksDB
    private boolean syncWrites = false;       // WAL fsync on each write
    private boolean disableWAL = false;       // keep WAL by default
    private int writeBufferSizeMB = 64;       // per memtable
    private int maxWriteBufferNumber = 3;
    private CompressionType compressionType = CompressionType.LZ4_COMPRESSION;
}
