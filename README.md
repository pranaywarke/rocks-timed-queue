# rocksqueue

A minimal, high-performance, time-based FIFO queue built on RocksDB.

- Per-queue-group isolation: each queue-group uses its own RocksDB instance (directory).
- Binary-encoded keys for compact storage and correct lexicographic ordering.
- FIFO within the same execution timestamp using a persisted insertion counter.
- Simple, clean API that hides RocksDB internals.

## Features

- Enqueue for a timestamp or after a delay
- Non-blocking and blocking dequeue
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
    String v = q.dequeue();
}
```

## Threading

- `QueueClient` manages read/write thread pools and a small maintenance scheduler.
- Writes are executed asynchronously for throughput.
- Blocking dequeue uses a simple polling backoff; more advanced waiting strategies can be added.

## License

Apache-2.0
