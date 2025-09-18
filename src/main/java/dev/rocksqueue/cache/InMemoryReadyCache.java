package dev.rocksqueue.cache;

import dev.rocksqueue.core.RawCacheEntry;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * Simple in-memory implementation backed by ArrayDeque.
 */
public final class InMemoryReadyCache implements ReadyCache {
    private final Deque<RawCacheEntry> dq = new ArrayDeque<>();

    @Override
    public RawCacheEntry peekFirst() {
        return dq.peekFirst();
    }

    @Override
    public RawCacheEntry pollFirst() {
        return dq.pollFirst();
    }

    @Override
    public void addLast(RawCacheEntry e) {
        dq.addLast(e);
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
    }

    @Override
    public Iterator<RawCacheEntry> iterator() {
        return dq.iterator();
    }
}
