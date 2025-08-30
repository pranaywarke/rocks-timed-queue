package dev.rocksqueue.api;

/**
 * Time-based queue semantics:
 * - Items are ordered by (executeAtMillis, insertion-order) and processed FIFO among items that are eligible
 *   (i.e., executeAtMillis &lt;= now) at the time of a dequeue scan.
 * - Monotonic timestamp enforcement: If executeAtMillis &lt; current time at enqueue, the timestamp is
 *   automatically adjusted to the current time to maintain ordering consistency and prevent backdated items.
 * - This ensures strict FIFO ordering among items with the same execution timestamp and prevents
 *   iterator invalidation issues in the underlying storage layer.
 */
public interface TimeQueue<T> extends AutoCloseable {
    /**
     * Enqueue an item to be available at the given timestamp (epoch millis).
     *
     * Note: If {@code executeAtMillis &lt; System.currentTimeMillis()}, the timestamp will be
     * automatically adjusted to the current time to maintain monotonic ordering.
     *
     * @param item the item to enqueue
     * @param executeAtMillis the timestamp when the item should become available for dequeue
     */
    void enqueue(T item, long executeAtMillis);


    T dequeue();

    T peek();

    long sizeApproximate();

    boolean isEmptyApproximate();

    @Override
    void close();
}
