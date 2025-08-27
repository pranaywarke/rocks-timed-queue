package dev.rocksqueue.api;

public interface TimeQueue<T> extends AutoCloseable {
    // Enqueue for exact time
    void enqueue(T item, long executeAtMillis);

    // Enqueue after delay from now
    default void enqueueDelayed(T item, long delayMillis) {
        enqueue(item, System.currentTimeMillis() + Math.max(0, delayMillis));
    }

    // Non-blocking dequeue of first ready item (executeAtMillis <= now)
    T dequeue();

    // Blocking dequeue; waits until an item is ready
    T dequeueBlocking() throws InterruptedException;

    // Peek at next ready item without removing; non-blocking
    T peek();

    // Visibility
    long sizeApproximate();

    boolean isEmptyApproximate();

    @Override
    void close();
}
