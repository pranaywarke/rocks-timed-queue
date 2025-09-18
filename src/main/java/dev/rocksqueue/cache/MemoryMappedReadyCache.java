package dev.rocksqueue.cache;

import dev.rocksqueue.core.RawCacheEntry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.zip.CRC32;

/**
 * Durable ready cache backed by a memory-mapped append-only log.
 *
 * Fixed-size circular file. When nearing end and record doesn't fit, a sentinel (-1 keyLen)
 * is written and the write pointer wraps to HEADER_SIZE.
 *
 * Header (64 bytes):
 *   [0..7]   MAGIC "RQCACHE1"
 *   [8..11]  version (int)
 *   [12..15] flags (bit0=strictCrc, bit1=forceOnWrite)
 *   [16..23] readOffset/head (long)
 *   [24..31] writeOffset/tail (long)
 *   [32..39] entryCount (long)
 *   [40..47] capacity/fileSize (long)
 *   [48..51] headerCRC32 (int) over bytes 0..47
 *   [52..63] reserved
 *
 * Entry:
 *   int keyLen
 *   int valLen
 *   long timestamp
 *   int crc32  (over timestamp+key+value)
 *   byte[keyLen] key
 *   byte[valLen] value
 *
 * On startup, the file is scanned from head to tail (with wrap) to rebuild the in-memory index.
 */
public final class MemoryMappedReadyCache implements ReadyCache {
    private static final byte[] MAGIC = new byte[]{'R','Q','C','A','C','H','E','1'};
    private static final int HEADER_SIZE = 64;
    private static final int VERSION = 1;
    private static final int SENTINEL = -1; // keyLen sentinel marking wrap

    private final String path;
    private final boolean strictCrc;
    private final boolean forceOnWrite;
    private final long fileSize;

    private RandomAccessFile raf;
    private FileChannel channel;
    private MappedByteBuffer mapped;

    private long readOffset;
    private long writeOffset;
    private long entryCount;

    // In-memory index for fast operations (drop-in for ArrayDeque semantics)
    private final Deque<RawCacheEntry> dq = new ArrayDeque<>();

    public MemoryMappedReadyCache(String filePath, long fileSizeBytes, boolean strictCrc, boolean forceOnWrite) {
        this.path = filePath;
        this.fileSize = Math.max(fileSizeBytes, HEADER_SIZE); // allow tiny files; header-only is valid
        this.strictCrc = strictCrc;
        this.forceOnWrite = forceOnWrite;
        openAndLoad();
    }

    private void openAndLoad() {
        try {
            File f = new File(path);
            File parent = f.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();

            this.raf = new RandomAccessFile(f, "rw");
            raf.setLength(fileSize);
            this.channel = raf.getChannel();
            this.mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            this.mapped.order(ByteOrder.BIG_ENDIAN);

            // Initialize or validate header
            boolean fresh = !validateMagic();
            if (fresh) {
                initializeHeader();
            } else if (!validateAndLoadHeader()) {
                // Corrupt header - reset
                initializeHeader();
            }

            // Rebuild in-memory index by scanning from head to tail.
            dq.clear();
            long start = readOffset;
            long pos = start;
            boolean equalPointers = (readOffset == writeOffset);
            long remaining = entryCount; // when pointers equal, trust header entryCount as upper bound
            boolean first = true;
            // If pointers are equal and count is zero, there is nothing to scan
            boolean doScan = !(equalPointers && remaining == 0);
            while (doScan) {
                // Early exit conditions to avoid reading past valid region
                if (!equalPointers && pos == writeOffset) {
                    break;
                }
                if (equalPointers && remaining <= 0) {
                    break;
                }
                if (pos < HEADER_SIZE) {
                    // Invalid position, reset
                    initializeHeader();
                    break;
                }
                // Not enough bytes left in this segment to read keyLen -> wrap
                if (pos + 4L > fileSize) {
                    pos = HEADER_SIZE;
                    continue;
                }
                int keyLen = getInt(pos);
                if (keyLen == SENTINEL) {
                    pos = HEADER_SIZE; // wrap
                    continue;
                }
                if (pos + 20L > fileSize) { // need keyLen,valLen,ts,crc header
                    // Crossed end of file -> wrap unconditionally
                    pos = HEADER_SIZE;
                    continue;
                }
                int valLen = getInt(pos + 4);
                long ts = getLong(pos + 8);
                int crc = getInt(pos + 16);
                long end = pos + 20L + keyLen + valLen;
                if (keyLen <= 0 || valLen < 0) {
                    writeOffset = pos;
                    writeHeaderPointers(true);
                    break;
                }
                if (end > fileSize) {
                    // Entry crosses end-of-file; wrap to header and continue
                    pos = HEADER_SIZE;
                    continue;
                }
                byte[] key = new byte[keyLen];
                byte[] val = new byte[valLen];
                mapped.get((int) (pos + 20), key);
                mapped.get((int) (pos + 20 + keyLen), val);
                if (strictCrc) {
                    int calc = crc(timestampToBytes(ts), key, val);
                    if (calc != crc) {
                        // Truncate at corruption point
                        writeOffset = pos;
                        writeHeaderPointers(true);
                        break;
                    }
                }
                dq.addLast(new RawCacheEntry(key, val, ts));
                long next = (end == fileSize) ? HEADER_SIZE : end;
                pos = next;
                first = false;

                // Exit conditions
                if (equalPointers) {
                    if (remaining > 0) remaining--;
                    if (remaining <= 0) break;
                    if (pos == start) break; // safety net if header count was too large
                } else {
                    if (pos == writeOffset) break;
                }
            }
            // Fix entryCount to actual
            entryCount = dq.size();
            writeHeaderPointers(false);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open memory-mapped cache at " + path, e);
        }
    }

    private void writeMagic() {
        mapped.put(0, MAGIC);
    }

    private boolean validateMagic() {
        byte[] magic = new byte[8];
        mapped.get(0, magic);
        for (int i = 0; i < MAGIC.length; i++) {
            if (magic[i] != MAGIC[i]) return false;
        }
        return true;
    }

    private boolean validateAndLoadHeader() {
        int ver = getInt(8);
        if (ver != VERSION) return false;
        int hdrCrc = getInt(48);
        int calc = headerCrc();
        if (strictCrc && hdrCrc != calc) {
            return false;
        }
        int flags = getInt(12);
        // flags are informational only on load; we keep current settings
        readOffset = getLong(16);
        writeOffset = getLong(24);
        entryCount = getLong(32);
        long cap = getLong(40);
        if (cap != fileSize) {
            // if file size differs, trust actual file size but reset pointers
            readOffset = writeOffset = HEADER_SIZE;
            entryCount = 0;
            writeHeaderPointers(true);
        }
        // bounds sanity
        if (readOffset < HEADER_SIZE || readOffset >= fileSize) readOffset = HEADER_SIZE;
        if (writeOffset < HEADER_SIZE || writeOffset >= fileSize) writeOffset = HEADER_SIZE;
        return true;
    }

    private void initializeHeader() {
        writeMagic();
        putInt(8, VERSION);
        int flags = (strictCrc ? 1 : 0) | (forceOnWrite ? 2 : 0);
        putInt(12, flags);
        readOffset = writeOffset = HEADER_SIZE;
        entryCount = 0;
        putLong(40, fileSize);
        writeHeaderPointers(true);
    }

    private int headerCrc() {
        byte[] buf = new byte[48];
        mapped.get(0, buf, 0, 48);
        CRC32 c = new CRC32();
        c.update(buf, 0, 48);
        return (int) c.getValue();
    }

    private void writeHeaderPointers() { writeHeaderPointers(forceOnWrite); }

    private void writeHeaderPointers(boolean force) {
        putLong(16, readOffset);
        putLong(24, writeOffset);
        putLong(32, entryCount);
        putLong(40, fileSize);
        putInt(48, headerCrc());
        if (force) {
            try { mapped.force(); } catch (Exception ignored) {}
        }
    }

    private void putInt(long index, int value) {
        mapped.putInt((int) index, value);
    }

    private int getInt(long index) {
        return mapped.getInt((int) index);
    }

    private void putLong(long index, long value) {
        mapped.putLong((int) index, value);
    }

    private long getLong(long index) {
        return mapped.getLong((int) index);
    }

    private static byte[] timestampToBytes(long ts) {
        // big-endian 8 bytes
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) { b[i] = (byte) (ts & 0xFF); ts >>= 8; }
        return b;
    }

    private static int crc(byte[] tsBytes, byte[] key, byte[] val) {
        CRC32 c = new CRC32();
        c.update(tsBytes, 0, 8);
        c.update(key, 0, key.length);
        c.update(val, 0, val.length);
        return (int) c.getValue();
    }

    private long dataCapacity() { return fileSize - HEADER_SIZE; }

    private long usedBytes() {
        if (writeOffset == readOffset) {
            // Ambiguity breaker: if pointers equal, either empty or full.
            // Use entryCount to disambiguate.
            return (entryCount == 0) ? 0 : dataCapacity();
        }
        if (writeOffset > readOffset) return writeOffset - readOffset;
        return (fileSize - readOffset) + (writeOffset - HEADER_SIZE);
    }

    private long freeBytes() { return dataCapacity() - usedBytes(); }

    @Override
    public RawCacheEntry peekFirst() {
        return dq.peekFirst();
    }

    @Override
    public RawCacheEntry pollFirst() {
        RawCacheEntry e = dq.pollFirst();
        if (e == null) return null;
        try {
            // Advance readOffset by reading the record length at current readOffset
            long pos = readOffset;
            // If less than 4 bytes remain at EOF, wrap before reading keyLen
            if (pos + 4L > fileSize) {
                pos = HEADER_SIZE;
            }
            int keyLen = getInt(pos);
            if (keyLen == SENTINEL) {
                pos = HEADER_SIZE;
                keyLen = getInt(pos);
            }
            int valLen = getInt(pos + 4);
            long recLen = 20L + keyLen + valLen; // includes crc
            long next = pos + recLen;
            long newRead = (next == fileSize) ? HEADER_SIZE : next;
            readOffset = newRead;
            if (readOffset == writeOffset) {
                // Empty after polling
                readOffset = writeOffset = HEADER_SIZE;
            }
            entryCount = Math.max(0, entryCount - 1);
            writeHeaderPointers();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to advance read pointer in cache", ex);
        }
        return e;
    }

    @Override
    public void addLast(RawCacheEntry e) {
        int keyLen = e.key().length;
        int valLen = e.value().length;
        long recLen = 20L + keyLen + valLen; // includes crc

        long remaining = fileSize - writeOffset;
        boolean needWrap = remaining < recLen;
        long sentinelExtra = (needWrap && remaining >= 4) ? 4 : 0;

        if (recLen + sentinelExtra > freeBytes()) {
            throw new RuntimeException("MemoryMappedReadyCache is full; cannot addLast");
        }

        if (needWrap) {
            if (remaining >= 4) {
                putInt(writeOffset, SENTINEL);
            }
            long newWrite = HEADER_SIZE;
            // Allow exact fit up to readOffset, but do not cross it.
            if (newWrite + recLen > readOffset) {
                throw new RuntimeException("MemoryMappedReadyCache overlap risk (full)");
            }
            writeOffset = newWrite;
        } else {
            // Allow exact fit up to readOffset, but do not cross it when write < read.
            if (writeOffset < readOffset && writeOffset + recLen > readOffset) {
                throw new RuntimeException("MemoryMappedReadyCache overlap risk (full)");
            }
        }

        // Write record
        putInt(writeOffset, keyLen);
        putInt(writeOffset + 4, valLen);
        putLong(writeOffset + 8, e.timestamp());
        int crc = crc(timestampToBytes(e.timestamp()), e.key(), e.value());
        putInt(writeOffset + 16, crc);
        mapped.put((int) (writeOffset + 20), e.key());
        mapped.put((int) (writeOffset + 20 + keyLen), e.value());
        long next = writeOffset + recLen;
        writeOffset = (next == fileSize) ? HEADER_SIZE : next;
        entryCount++;
        writeHeaderPointers();
        if (forceOnWrite) {
            try { mapped.force(); } catch (Exception ignored) {}
        }
        dq.addLast(e);
    }

    @Override
    public boolean offerLast(RawCacheEntry e) {
        int keyLen = e.key().length;
        int valLen = e.value().length;
        long recLen = 20L + keyLen + valLen;
        long remaining = fileSize - writeOffset;
        boolean needWrap = remaining < recLen;
        long sentinelExtra = (needWrap && remaining >= 4) ? 4 : 0;
        if (recLen + sentinelExtra > freeBytes()) {
            return false;
        }
        if (needWrap) {
            if (remaining >= 4) {
                putInt(writeOffset, SENTINEL);
            }
            long newWrite = HEADER_SIZE;
            if (newWrite + recLen > readOffset) {
                return false;
            }
            writeOffset = newWrite;
        } else {
            if (writeOffset < readOffset && writeOffset + recLen > readOffset) {
                return false;
            }
        }

        // write the record
        putInt(writeOffset, keyLen);
        putInt(writeOffset + 4, valLen);
        putLong(writeOffset + 8, e.timestamp());
        int crc = crc(timestampToBytes(e.timestamp()), e.key(), e.value());
        putInt(writeOffset + 16, crc);
        mapped.put((int) (writeOffset + 20), e.key());
        mapped.put((int) (writeOffset + 20 + keyLen), e.value());
        long next = writeOffset + recLen;
        writeOffset = (next == fileSize) ? HEADER_SIZE : next;
        entryCount++;
        writeHeaderPointers();
        if (forceOnWrite) { try { mapped.force(); } catch (Exception ignored) {} }
        dq.addLast(e);
        return true;
    }

    @Override
    public boolean canFit(RawCacheEntry e) {
        int keyLen = e.key().length;
        int valLen = e.value().length;
        long recLen = 20L + keyLen + valLen;
        long remaining = fileSize - writeOffset;
        boolean needWrap = remaining < recLen;
        long sentinelExtra = (needWrap && remaining >= 4) ? 4 : 0;
        if (recLen + sentinelExtra > freeBytes()) {
            return false;
        }
        if (needWrap) {
            long newWrite = HEADER_SIZE;
            // allow exact fit up to readOffset
            if (newWrite + recLen > readOffset) return false;
            return true;
        } else {
            if (writeOffset < readOffset && writeOffset + recLen > readOffset) return false;
            return true;
        }
    }

    /**
     * Approximate free bytes available for new records. Intended for conservative
     * pre-staging in RocksTimeQueue to avoid over-deleting from RocksDB.
     */
    public long approximateFreeBytes() {
        return freeBytes();
    }

    @Override
    public int size() {
        return dq.size();
    }

    @Override
    public boolean isEmpty() {
        return dq.isEmpty();
    }

    @Override
    public void clear() {
        dq.clear();
        readOffset = writeOffset = HEADER_SIZE;
        entryCount = 0;
        writeHeaderPointers();
    }

    @Override
    public Iterator<RawCacheEntry> iterator() {
        return dq.iterator();
    }

    @Override
    public void close() {
        try {
            if (mapped != null) {
                mapped.force();
            }
        } catch (Exception ignored) {}
        try { if (channel != null) channel.close(); } catch (Exception ignored) {}
        try { if (raf != null) raf.close(); } catch (Exception ignored) {}
    }
}
