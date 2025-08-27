package dev.rocksqueue.config;

import org.rocksdb.CompressionType;

public class QueueConfig {
    // Paths
    private String basePath = "./data/rocksqueue";

    // Threading
    private int writeCoreThreads = 2;
    private int writeMaxThreads = 4;
    private int readCoreThreads = 2;
    private int readMaxThreads = 4;
    private int maintenanceThreads = 1;

    // RocksDB
    private boolean syncWrites = false;       // WAL fsync on each write
    private boolean disableWAL = false;       // keep WAL by default
    private int writeBufferSizeMB = 64;       // per memtable
    private int maxWriteBufferNumber = 3;
    private CompressionType compressionType = CompressionType.LZ4_COMPRESSION;

    // Counter persistence
    private int counterPersistEvery = 1000;

    public String getBasePath() { return basePath; }
    public QueueConfig setBasePath(String basePath) { this.basePath = basePath; return this; }

    public int getWriteCoreThreads() { return writeCoreThreads; }
    public QueueConfig setWriteCoreThreads(int writeCoreThreads) { this.writeCoreThreads = writeCoreThreads; return this; }

    public int getWriteMaxThreads() { return writeMaxThreads; }
    public QueueConfig setWriteMaxThreads(int writeMaxThreads) { this.writeMaxThreads = writeMaxThreads; return this; }

    public int getReadCoreThreads() { return readCoreThreads; }
    public QueueConfig setReadCoreThreads(int readCoreThreads) { this.readCoreThreads = readCoreThreads; return this; }

    public int getReadMaxThreads() { return readMaxThreads; }
    public QueueConfig setReadMaxThreads(int readMaxThreads) { this.readMaxThreads = readMaxThreads; return this; }

    public int getMaintenanceThreads() { return maintenanceThreads; }
    public QueueConfig setMaintenanceThreads(int maintenanceThreads) { this.maintenanceThreads = maintenanceThreads; return this; }

    public boolean isSyncWrites() { return syncWrites; }
    public QueueConfig setSyncWrites(boolean syncWrites) { this.syncWrites = syncWrites; return this; }

    public boolean isDisableWAL() { return disableWAL; }
    public QueueConfig setDisableWAL(boolean disableWAL) { this.disableWAL = disableWAL; return this; }

    public int getWriteBufferSizeMB() { return writeBufferSizeMB; }
    public QueueConfig setWriteBufferSizeMB(int writeBufferSizeMB) { this.writeBufferSizeMB = writeBufferSizeMB; return this; }

    public int getMaxWriteBufferNumber() { return maxWriteBufferNumber; }
    public QueueConfig setMaxWriteBufferNumber(int maxWriteBufferNumber) { this.maxWriteBufferNumber = maxWriteBufferNumber; return this; }

    public CompressionType getCompressionType() { return compressionType; }
    public QueueConfig setCompressionType(CompressionType compressionType) { this.compressionType = compressionType; return this; }

    public int getCounterPersistEvery() { return counterPersistEvery; }
    public QueueConfig setCounterPersistEvery(int counterPersistEvery) { this.counterPersistEvery = counterPersistEvery; return this; }
}
