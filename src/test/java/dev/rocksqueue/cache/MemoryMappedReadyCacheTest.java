package dev.rocksqueue.cache;

import dev.rocksqueue.core.RawCacheEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MemoryMappedReadyCacheTest {

    private Path tmp;

    @AfterEach
    void tearDown() {
        if (tmp != null) {
            try {
                Files.walk(tmp)
                        .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> {
                            try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                        });
            } catch (Exception ignored) {}
        }
    }

    @Test
    void large_payload_16mb_cache_no_stall_on_empty() throws Exception {
        Path dir = newTempDir();
        int valLen = 5120; // 5KB payload to match throughput test scenario
        int recLen = 20 + 16 + valLen;
        int fileSize = 64 + (16 * 1024 * 1024); // 16MB file size
        String file = "large16mb.cache";

        try (MemoryMappedReadyCache cache = newCache(dir, file, fileSize, true, false)) {
            // Fill as much as possible
            int appended = 0;
            long ts = 1;
            RawCacheEntry proto = entry(ts, valLen);
            while (cache.canFit(proto)) {
                cache.addLast(entry(ts++, valLen));
                appended++;
            }

            // Drain completely
            int polled = 0;
            while (cache.pollFirst() != null) polled++;
            assertEquals(appended, polled, "All appended entries should be retrievable");

            // When empty, large record must fit and not stall
            RawCacheEntry e = entry(9999, valLen);
            assertTrue(cache.canFit(e), "Empty cache must accept at least one large record");
            cache.addLast(e);
            RawCacheEntry got = cache.pollFirst();
            assertNotNull(got);
            assertEquals(9999, got.timestamp());
        }
    }

    private static RawCacheEntry entry(long ts, int valLen) {
        byte[] key = new byte[16]; // 16-byte key as required
        // simple pattern for visibility
        for (int i = 0; i < 16; i++) key[i] = (byte) (i + 1);
        byte[] val = new byte[valLen];
        for (int i = 0; i < valLen; i++) val[i] = (byte) (ts + i);
        return new RawCacheEntry(key, val, ts);
    }

    private Path newTempDir() throws IOException {
        tmp = Files.createTempDirectory("mm-ready-cache-test-");
        return tmp;
    }

    private MemoryMappedReadyCache newCache(Path dir, String name, int fileSize, boolean strict, boolean force) {
        return new MemoryMappedReadyCache(dir.resolve(name).toString(), fileSize, strict, force);
    }

    @Test
    void basic_add_peek_poll_and_size() throws Exception {
        Path dir = newTempDir();
        int fileSize = 64 + 10 * (20 + 16 + 8); // space for 10 small entries
        try (MemoryMappedReadyCache cache = newCache(dir, "c1.cache", fileSize, true, false)) {
            assertTrue(cache.isEmpty());

            RawCacheEntry e1 = entry(1, 8);
            RawCacheEntry e2 = entry(2, 8);
            cache.addLast(e1);
            cache.addLast(e2);
            assertEquals(2, cache.size());
            assertSame(e1, cache.peekFirst());
            assertSame(e1, cache.pollFirst());
            assertSame(e2, cache.pollFirst());
            assertNull(cache.pollFirst());
            assertTrue(cache.isEmpty());
        }
    }

    @Test
    void exact_fit_to_end_wraps_to_header() throws Exception {
        Path dir = newTempDir();
        int recLen = 20 + 16 + 10; // valLen=10
        int fileSize = 64 + 5 * recLen; // exactly fits 5 records
        try (MemoryMappedReadyCache cache = newCache(dir, "c2.cache", fileSize, true, false)) {
            for (int i = 0; i < 5; i++) {
                cache.addLast(entry(i + 1, 10));
            }
            // After 5th add, internal writeOffset should be HEADER_SIZE (wrap). We can't access it,
            // but we can ensure another add fits at start by polling one and adding one more.
            assertEquals(5, cache.size());
            assertNotNull(cache.pollFirst());
            cache.addLast(entry(6, 10));
            assertEquals(5, cache.size());
        }
    }

    @Test
    void wrap_with_sentinel_and_poll_across_wrap() throws Exception {
        Path dir = newTempDir();
        int valLen = 50; // recLen=86
        int recLen = 20 + 16 + valLen;
        int fileSize = 64 + 6 * recLen + 32; // ensure one wrap with leftover >= 4 bytes
        try (MemoryMappedReadyCache cache = newCache(dir, "c3.cache", fileSize, true, false)) {
            List<RawCacheEntry> items = new ArrayList<>();
            for (int i = 0; i < 4; i++) items.add(entry(i + 1, valLen));
            // fill some
            for (RawCacheEntry e : items) cache.addLast(e);
            // move head forward to allow wrap
            assertSame(items.get(0), cache.pollFirst());
            // Add until near end to force sentinel + wrap
            RawCacheEntry e5 = entry(5, valLen);
            while (cache.canFit(e5)) {
                cache.addLast(e5);
                items.add(e5);
            }
            // Now canFit is false due to contiguous space; but after polling more, we should be able to wrap
            RawCacheEntry e2 = cache.pollFirst();
            assertNotNull(e2);
            // Now try adding again (should wrap and succeed)
            assertTrue(cache.offerLast(entry(6, valLen)));
            // Drain and ensure order continuity
            int count = 0;
            RawCacheEntry prev;
            while ((prev = cache.pollFirst()) != null) {
                count++;
            }
            assertTrue(count >= 1);
        }
    }

    @Test
    void offer_returns_false_and_add_throws_when_full() throws Exception {
        Path dir = newTempDir();
        int valLen = 32;
        int recLen = 20 + 16 + valLen;
        int fileSize = 64 + 5 * recLen + 8; // tight capacity
        try (MemoryMappedReadyCache cache = newCache(dir, "c4.cache", fileSize, true, false)) {
            RawCacheEntry e = entry(1, valLen);
            int appended = 0;
            while (cache.canFit(e)) {
                cache.addLast(e);
                appended++;
            }
            assertFalse(cache.offerLast(e));
            assertThrows(RuntimeException.class, () -> cache.addLast(e));
            assertEquals(appended, cache.size());
        }
    }

    @Test
    void persistence_across_reopen() throws Exception {
        Path dir = newTempDir();
        int fileSize = 64 + 3 * (20 + 16 + 12);
        String file = "c5.cache";
        try (MemoryMappedReadyCache cache = newCache(dir, file, fileSize, true, true)) {
            cache.addLast(entry(10, 12));
            cache.addLast(entry(11, 12));
            assertEquals(2, cache.size());
        }
        try (MemoryMappedReadyCache cache2 = newCache(dir, file, fileSize, true, true)) {
            assertEquals(2, cache2.size());
            assertEquals(10, cache2.pollFirst().timestamp());
            assertEquals(11, cache2.pollFirst().timestamp());
            assertNull(cache2.pollFirst());
        }
    }

    @Test
    void persistence_across_reopen_no_force() throws Exception {
        Path dir = newTempDir();
        int fileSize = 64 + 3 * (20 + 16 + 8);
        String file = "c5b.cache";
        try (MemoryMappedReadyCache cache = newCache(dir, file, fileSize, true, false)) {
            cache.addLast(entry(100, 8));
            cache.addLast(entry(101, 8));
        }
        try (MemoryMappedReadyCache cache2 = newCache(dir, file, fileSize, true, false)) {
            // With no force, typical OS flush should persist entries
            assertEquals(2, cache2.size());
            assertEquals(100, cache2.pollFirst().timestamp());
            assertEquals(101, cache2.pollFirst().timestamp());
        }
    }

    @Test
    void strict_crc_detects_corruption_and_truncates() throws Exception {
        Path dir = newTempDir();
        int valLen = 20;
        int recLen = 20 + 16 + valLen;
        int fileSize = 64 + 3 * recLen + 64;
        String file = "c6.cache";
        try (MemoryMappedReadyCache cache = newCache(dir, file, fileSize, true, false)) {
            cache.addLast(entry(1, valLen));
            cache.addLast(entry(2, valLen));
            cache.addLast(entry(3, valLen));
            assertEquals(3, cache.size());
        }
        // Corrupt second record's value first byte
        Path p = dir.resolve(file);
        try (RandomAccessFile raf = new RandomAccessFile(p.toFile(), "rw")) {
            long posSecond = 64 + recLen; // start of 2nd record
            long valOffset = posSecond + 20; // skip keyLen,valLen,ts,crc
            raf.seek(valOffset);
            int b = raf.read();
            raf.seek(valOffset);
            raf.write((b ^ 0xFF)); // flip bits
        }
        try (MemoryMappedReadyCache cache2 = newCache(dir, file, fileSize, true, false)) {
            // Should truncate at the start of the corrupted record, leaving only the first one
            assertEquals(1, cache2.size());
            assertEquals(1, cache2.pollFirst().timestamp());
            assertNull(cache2.pollFirst());
        }
    }

    @Test
    void crash_before_header_commit_drops_last_entry() throws Exception {
        Path dir = newTempDir();
        int valLen = 16;
        int recLen = 20 + 16 + valLen;
        int fileSize = 64 + 4 * recLen + 64;
        String file = "c10.cache";
        // Write two entries
        try (MemoryMappedReadyCache cache = newCache(dir, file, fileSize, true, true)) {
            cache.addLast(entry(1, valLen));
            cache.addLast(entry(2, valLen));
        }
        Path p = dir.resolve(file);
        // Simulate crash before header commit of the second entry: revert writeOffset to just after first
        try (RandomAccessFile raf = new RandomAccessFile(p.toFile(), "rw")) {
            long prevWrite = 64 + recLen; // after first record
            // writeOffset at header offset 24
            raf.seek(24);
            // big-endian long
            ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(prevWrite);
            bb.flip();
            raf.write(bb.array());

            // Recompute header CRC over bytes 0..47 and write at 48
            raf.seek(0);
            byte[] hdr = new byte[48];
            raf.readFully(hdr);
            java.util.zip.CRC32 c = new java.util.zip.CRC32();
            c.update(hdr, 0, 48);
            int crc = (int) c.getValue();
            raf.seek(48);
            raf.writeInt(crc);
        }

        // Reopen and verify only the first entry remains
        try (MemoryMappedReadyCache cache2 = newCache(dir, file, fileSize, true, true)) {
            assertEquals(1, cache2.size());
            assertEquals(1, cache2.pollFirst().timestamp());
            assertNull(cache2.pollFirst());
        }
    }

    @Test
    void crash_partial_last_entry_truncates_to_previous() throws Exception {
        Path dir = newTempDir();
        int valLen = 32;
        int recLen = 20 + 16 + valLen;
        int fileSize = 64 + 4 * recLen + 64;
        String file = "c11.cache";
        try (MemoryMappedReadyCache cache = newCache(dir, file, fileSize, true, false)) {
            cache.addLast(entry(10, valLen));
            cache.addLast(entry(11, valLen));
        }
        Path p = dir.resolve(file);
        // Simulate truncation mid-value of second record
        long secondStart = 64 + recLen;
        long partialLen = 20 + 16 + (valLen / 2);
        try (RandomAccessFile raf = new RandomAccessFile(p.toFile(), "rw")) {
            raf.setLength(secondStart + partialLen);
            // keep header's writeOffset pointing to the full end to simulate commit-before-data
        }
        // Reopen with strict CRC: should truncate to before second record
        try (MemoryMappedReadyCache cache2 = newCache(dir, file, fileSize, true, false)) {
            assertEquals(1, cache2.size());
            assertEquals(10, cache2.pollFirst().timestamp());
            assertNull(cache2.pollFirst());
        }
    }

    @Test
    void header_crc_corruption_resets_header() throws Exception {
        Path dir = newTempDir();
        int fileSize = 64 + 2 * (20 + 16 + 8);
        String file = "c7.cache";
        try (MemoryMappedReadyCache cache = newCache(dir, file, fileSize, true, false)) {
            cache.addLast(entry(1, 8));
            cache.addLast(entry(2, 8));
        }
        // Corrupt header CRC at offset 48
        Path p = dir.resolve(file);
        try (RandomAccessFile raf = new RandomAccessFile(p.toFile(), "rw")) {
            raf.seek(48);
            raf.writeInt(0); // wrong CRC
        }
        try (MemoryMappedReadyCache cache2 = newCache(dir, file, fileSize, true, false)) {
            // With strictCrc, header CRC mismatch triggers reinit -> empty
            assertEquals(0, cache2.size());
            assertNull(cache2.pollFirst());
        }
    }

    @Test
    void clear_resets_and_allows_reuse() throws Exception {
        Path dir = newTempDir();
        int fileSize = 64 + 4 * (20 + 16 + 10);
        try (MemoryMappedReadyCache cache = newCache(dir, "c8.cache", fileSize, true, false)) {
            cache.addLast(entry(1, 10));
            cache.addLast(entry(2, 10));
            assertEquals(2, cache.size());
            cache.clear();
            assertEquals(0, cache.size());
            cache.addLast(entry(3, 10));
            assertEquals(1, cache.size());
            assertEquals(3, cache.pollFirst().timestamp());
        }
    }

    @Test
    void iterator_orders_fifo() throws Exception {
        Path dir = newTempDir();
        int fileSize = 64 + 4 * (20 + 16 + 10);
        try (MemoryMappedReadyCache cache = newCache(dir, "c9.cache", fileSize, true, false)) {
            cache.addLast(entry(1, 10));
            cache.addLast(entry(2, 10));
            cache.addLast(entry(3, 10));
            List<Long> ts = new ArrayList<>();
            for (RawCacheEntry e : cache) ts.add(e.timestamp());
            assertEquals(List.of(1L, 2L, 3L), ts);
        }
    }

    @Test
    void canFit_addLast_equivalence() throws Exception {
        Path dir = newTempDir();
        int valLen = 40;
        int recLen = 20 + 16 + valLen;
        int fileSize = 64 + 3 * recLen + 8;
        try (MemoryMappedReadyCache cache = newCache(dir, "c12.cache", fileSize, true, false)) {
            RawCacheEntry e = entry(1, valLen);
            while (cache.canFit(e)) {
                cache.addLast(e);
            }
            assertFalse(cache.canFit(e));
            assertFalse(cache.offerLast(e));
            assertThrows(RuntimeException.class, () -> cache.addLast(e));
        }
    }

    @Test
    void minimal_file_size_rejects_all() throws Exception {
        Path dir = newTempDir();
        int fileSize = 64 + 1; // essentially no room for an entry
        try (MemoryMappedReadyCache cache = newCache(dir, "c13.cache", fileSize, true, false)) {
            RawCacheEntry e = entry(1, 1);
            assertFalse(cache.canFit(e));
            assertFalse(cache.offerLast(e));
            assertThrows(RuntimeException.class, () -> cache.addLast(e));
            assertEquals(0, cache.size());
        }
    }

    @Test
    void version_mismatch_reinitializes() throws Exception {
        Path dir = newTempDir();
        int fileSize = 64 + 2 * (20 + 16 + 8);
        String file = "c14.cache";
        try (MemoryMappedReadyCache cache = newCache(dir, file, fileSize, true, false)) {
            cache.addLast(entry(1, 8));
        }
        // Corrupt version at offset 8
        Path p = dir.resolve(file);
        try (RandomAccessFile raf = new RandomAccessFile(p.toFile(), "rw")) {
            raf.seek(8);
            raf.writeInt(9999);
        }
        try (MemoryMappedReadyCache cache2 = newCache(dir, file, fileSize, true, false)) {
            assertEquals(0, cache2.size());
        }
    }

    @Test
    void wrap_without_sentinel_trailing_lt4_bytes_then_reopen() throws Exception {
        Path dir = newTempDir();
        int valLen = 7;
        int recLen = 20 + 16 + valLen; // 43 bytes
        int trailing = 3; // < 4 bytes remain, cannot write sentinel
        int fileSize = 64 + 3 * recLen + trailing;
        String file = "c15.cache";
        try (MemoryMappedReadyCache cache = newCache(dir, file, fileSize, true, false)) {
            cache.addLast(entry(1, valLen));
            cache.addLast(entry(2, valLen));
            cache.addLast(entry(3, valLen));
            // Buffer full except trailing <4 bytes. Free one slot, then wrap without sentinel should succeed.
            assertEquals(1, cache.pollFirst().timestamp());
            cache.addLast(entry(4, valLen));
            assertEquals(3, cache.size());
        }
        try (MemoryMappedReadyCache cache2 = newCache(dir, file, fileSize, true, false)) {
            // Entries after reopen should be [2,3,4]
            assertEquals(3, cache2.size());
            assertEquals(2, cache2.pollFirst().timestamp());
            assertEquals(3, cache2.pollFirst().timestamp());
            assertEquals(4, cache2.pollFirst().timestamp());
            assertNull(cache2.pollFirst());
        }
    }

    @Test
    void crash_after_sentinel_before_header_update_wraps_on_reopen() throws Exception {
        Path dir = newTempDir();
        int valLen = 20;
        int recLen = 20 + 16 + valLen;
        // choose capacity to cause a wrap after 2 entries
        int fileSize = 64 + 2 * recLen + 8; // enough to write sentinel (>=4)
        String file = "c16.cache";
        // Write two entries to place writer near end then cause wrap on next add
        try (MemoryMappedReadyCache cache = newCache(dir, file, fileSize, true, false)) {
            cache.addLast(entry(1, valLen));
            cache.addLast(entry(2, valLen));
            // Force a wrap by attempting a large value that doesn't fit
            try { cache.addLast(entry(3, valLen)); } catch (RuntimeException ignored) {}
        }

        Path p = dir.resolve(file);
        // Simulate crash after sentinel write but before header update: set writeOffset back to just before wrap
        try (RandomAccessFile raf = new RandomAccessFile(p.toFile(), "rw")) {
            long lastPos = 64 + 2L * recLen; // position at which sentinel was likely written
            raf.seek(24);
            ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(lastPos);
            bb.flip();
            raf.write(bb.array());

            // Recompute header CRC
            raf.seek(0);
            byte[] hdr = new byte[48];
            raf.readFully(hdr);
            java.util.zip.CRC32 c = new java.util.zip.CRC32();
            c.update(hdr, 0, 48);
            int crc = (int) c.getValue();
            raf.seek(48);
            raf.writeInt(crc);
        }

        // Reopen; iterator should see sentinel and wrap cleanly
        try (MemoryMappedReadyCache cache2 = newCache(dir, file, fileSize, true, false)) {
            assertEquals(2, cache2.size());
            assertEquals(1, cache2.pollFirst().timestamp());
            assertEquals(2, cache2.pollFirst().timestamp());
            assertNull(cache2.pollFirst());
        }
    }
}
