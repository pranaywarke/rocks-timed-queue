package dev.rocksqueue.core;

public interface Counter {
    long get();
    long incrementAndGet();
    void set(long value);
}
