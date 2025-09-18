package dev.rocksqueue.cache;

import dev.rocksqueue.core.RawCacheEntry;

import java.util.Iterator;

/**
 * A minimal deque-like cache abstraction for ready entries.
 *
 * Drop-in replacement for the subset of ArrayDeque methods used by RocksTimeQueue:
 * - peekFirst
 * - pollFirst
 * - addLast
 * - size
 * - isEmpty
 * - clear
 * - iteration (Iterable)
 *
 * Implementations may be purely in-memory or durable (memory-mapped).
 */
public interface ReadyCache extends Iterable<RawCacheEntry>, AutoCloseable {
    RawCacheEntry peekFirst();

    RawCacheEntry pollFirst();

    void addLast(RawCacheEntry e);

    /**
     * Try to append without throwing when full. Default delegates to addLast.
     * @return true if appended, false if cannot accept (e.g., full)
     */
    default boolean offerLast(RawCacheEntry e) {
        addLast(e);
        return true;
    }

    /**
     * Non-mutating capacity check for appending an entry. Implementations that
     * have fixed capacity should accurately report whether a subsequent call to
     * offerLast/addLast would succeed under the current state.
     * Default returns true for unbounded caches.
     */
    default boolean canFit(RawCacheEntry e) { return true; }

    int size();

    boolean isEmpty();

    void clear();

    @Override
    default void close() throws Exception { /* no-op by default */ }

    @Override
    Iterator<RawCacheEntry> iterator();
}
