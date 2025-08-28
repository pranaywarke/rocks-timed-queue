package dev.rocksqueue.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple memory-mapped 8-byte counter. Thread-safe within a single process.
 * Persists the current value to a file via a mapped region at offset 0.
 */
public final class MappedLongCounter implements Counter, AutoCloseable {
    private final FileChannel channel;
    private final MappedByteBuffer mapped;
    private final AtomicLong value;

    private MappedLongCounter(FileChannel channel, MappedByteBuffer mapped, long initial) {
        this.channel = channel;
        this.mapped = mapped;
        this.value = new AtomicLong(initial);
    }

    public static MappedLongCounter open(String path) {
        try {
            File f = new File(path);
            File parent = f.getParentFile();
            if (parent != null) parent.mkdirs();
            try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
                if (raf.length() < 8) {
                    raf.setLength(8);
                    raf.seek(0);
                    raf.write(new byte[8]);
                }
            }
            RandomAccessFile raf2 = new RandomAccessFile(f, "rw");
            FileChannel ch = raf2.getChannel();
            MappedByteBuffer mbb = ch.map(FileChannel.MapMode.READ_WRITE, 0, 8);
            mbb.order(ByteOrder.BIG_ENDIAN);
            long initial = mbb.getLong(0);
            return new MappedLongCounter(ch, mbb, initial);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open mapped counter at " + path, e);
        }
    }

    public long get() {
        return value.get();
    }

    public long incrementAndGet() {
        long cur = value.get();
        if (cur == Long.MAX_VALUE) {
            throw new IllegalStateException("Insertion counter overflow (Long.MAX_VALUE). Rotate group or compact data.");
        }
        long v = value.incrementAndGet();
        write(v);
        return v;
    }

    public void set(long v) {
        value.set(v);
        write(v);
    }

    private void write(long v) {
        ByteBuffer tmp = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(v);
        tmp.flip();
        mapped.position(0);
        mapped.put(tmp);
        // Do not force() every time to avoid overhead; rely on OS flush and force on close
    }

    public void force() {
        mapped.force();
    }

    @Override
    public void close() {
        try {
            force();
        } catch (Exception ignored) {}
        try { channel.close(); } catch (Exception ignored) {}
    }
}
