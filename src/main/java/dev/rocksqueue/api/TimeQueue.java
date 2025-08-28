package dev.rocksqueue.api;

/**
 * Time-based queue semantics:
 * - Items are ordered by (executeAtMillis, insertion-order) and processed FIFO among items that are eligible
 *   (i.e., executeAtMillis <= now) at the time of a dequeue scan.
 * - Disclaimer: If producers enqueue items that are already eligible at enqueue time (executeAtMillis < now),
 *   strict FIFO across all currently-eligible items may not be guaranteed under high concurrency. Newly enqueued
 *   earlier items can be served after items that were already batched by a consumer. Applications that require
 *   strict global FIFO among eligibles should avoid backdated enqueues or enable stricter coordination.
 */
public interface TimeQueue<T> extends AutoCloseable {
    // Enqueue for exact time
    /**
     * Enqueue an item to be available at the given timestamp (epoch millis).
     *
     * Note: If {@code executeAtMillis < System.currentTimeMillis()} (a "backdated" enqueue), ordering guarantees
     * may be relaxed depending on configuration; see interface-level docs.
     */
    void enqueue(T item, long executeAtMillis);

    // Enqueue after delay from now
    default void enqueueDelayed(T item, long delayMillis) {
        enqueue(item, System.currentTimeMillis() + Math.max(0, delayMillis));
    }

    // Non-blocking dequeue of first ready item (executeAtMillis <= now)
    T dequeue();

    // Peek at next ready item without removing; non-blocking
    T peek();

    // Visibility
    long sizeApproximate();

    boolean isEmptyApproximate();

    @Override
    void close();
}
