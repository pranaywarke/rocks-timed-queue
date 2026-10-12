package dev.rocksqueue.cli;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.client.QueueClient;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.Utf8StringSerializer;

/**
 * Prints the approximate size of queue groups stored under a base path.
 *
 * <pre>
 * java dev.rocksqueue.cli.QueueInspector ./data/rocksqueue user-tasks events
 * </pre>
 */
public final class QueueInspector {
    private QueueInspector() {}

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("usage: QueueInspector <base-path> <group>...");
            System.exit(2);
        }
        QueueConfig config = new QueueConfig().setBasePath(args[0]);
        try (QueueClient client = new QueueClient(config)) {
            for (int i = 1; i < args.length; i++) {
                String group = args[i];
                client.registerQueue(group, String.class, new Utf8StringSerializer());
                TimeQueue<String> queue = client.getQueue(group);
                System.out.printf("%s\t%d%n", group, queue.sizeApproximate());
            }
        }
    }
}
