package dev.rocksqueue.cli;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.client.QueueClient;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.Utf8StringSerializer;

/**
 * Prints the next ready item of one queue group without removing it.
 *
 * <pre>
 * java dev.rocksqueue.cli.QueuePeek ./data/rocksqueue user-tasks
 * </pre>
 */
public final class QueuePeek {
    private QueuePeek() {}

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("usage: QueuePeek <base-path> <group>");
            System.exit(2);
        }
        try (QueueClient client = new QueueClient(new QueueConfig().setBasePath(args[0]))) {
            client.registerQueue(args[1], String.class, new Utf8StringSerializer());
            TimeQueue<String> queue = client.getQueue(args[1]);
            String next = queue.peek();
            System.out.println(next == null ? "(no ready item)" : next);
        }
    }
}
