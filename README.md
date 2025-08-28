# rocksqueue

A minimal, high-performance, time-based FIFO queue built on RocksDB.

- Per-queue-group isolation: each queue-group uses its own RocksDB instance (directory).
- Binary-encoded keys for compact storage and correct lexicographic ordering.
- FIFO within the same execution timestamp using a persisted insertion counter.
- Simple, clean API that hides RocksDB internals.

## Features

- Enqueue for a timestamp or after a delay
- Non-blocking dequeue (returns null when no ready item)
- Peek next ready item
- Approximate size introspection

## Key Design

- Keys are 16-byte binary values: `[8B big-endian executeAtMillis][8B big-endian insertionSequence]`.
- This preserves natural ordering: first by timestamp, then FIFO.
- No string prefixes or padding; metadata (e.g., insertion counter) is stored under small dedicated keys inside the same DB.

## Getting Started

### Requirements

- JDK 17+
- Gradle (wrapper provided)

RocksDB JNI native library is bundled via `rocksdbjni` dependency.

### Build

```bash
./gradlew clean build
```

### Usage

```java
import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.client.QueueClient;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;

QueueConfig config = new QueueConfig()
        .setBasePath("./data/rocksqueue")
        .setSyncWrites(false)
        .setDisableWAL(false);

try (QueueClient client = new QueueClient(config)) {
    TimeQueue<String> q = client.getQueue("my-group", String.class, new JsonSerializer<>());

    q.enqueue("hello", System.currentTimeMillis());
    String v = q.dequeue(); // returns null if not ready
}
```

## Threading

- Per-group dequeue serialization: `QueueClient` provides a shared lock per group; all `dequeue()` calls for the same group are serialized within the JVM to avoid duplicate deliveries from intra-process races.
- Producers are fully concurrent; only `dequeue()` per group is serialized.
- Multi-process note: Java-level synchronization does not coordinate across processes or hosts. If you run multiple processes against the same group DB and require at-most-once without duplicates, you need a DB-level atomic claim (e.g., RocksDB transactions or a claim-marker pattern) and/or idempotent consumers.

## License

Apache-2.0
