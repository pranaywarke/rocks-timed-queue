# RocksQueue

A minimal, high-performance, time-based FIFO queue built on RocksDB.

[![Build Status](https://github.com/pranaywarke/rocksqueue/workflows/CI/badge.svg)](https://github.com/pranaywarke/rocksqueue/actions)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

RocksQueue provides a persistent, time-based FIFO queue with the following key features:

- **Time-based scheduling**: Enqueue items for execution at specific timestamps
- **High performance**: Optimized batch operations and intelligent in-memory caching
- **Persistence**: Built on RocksDB with graceful shutdown recovery (see limitations below)
- **Isolation**: Per-queue-group isolation with separate RocksDB instances
- **Thread-safe**: Concurrent producers with serialized dequeue per group
- **Simple API**: Clean interface that abstracts RocksDB complexity

## Key Design

- **Binary keys**: 16-byte keys `[8B timestamp][8B sequence]` for optimal ordering
- **FIFO guarantee**: Items with identical timestamps maintain insertion order
- **Compact storage**: No string prefixes or padding, metadata stored separately
- **Batch operations**: Efficient bulk processing for high throughput

## Quick Start

### Requirements

- **JDK 17+**
- **Gradle** (wrapper provided)

RocksDB JNI native library is automatically included via the `rocksdbjni` dependency.

### Installation

Clone the repository and build:

```bash
git clone https://github.com/pranaywarke/rocksqueue.git
cd rocksqueue
./gradlew clean build
```

### Basic Usage

```java
import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.client.QueueClient;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.JsonSerializer;

// Configure the queue
QueueConfig config = new QueueConfig()
        .setBasePath("./data/rocksqueue")
        .setSyncWrites(false)
        .setDisableWAL(false);

// Create client and queue
try (QueueClient client = new QueueClient(config)) {
    TimeQueue<String> queue = client.getQueue("my-group", String.class, new JsonSerializer<>());

    // Enqueue for immediate execution
    queue.enqueue("immediate-task", System.currentTimeMillis());
    
    // Enqueue for future execution (5 seconds from now)
    queue.enqueue("delayed-task", System.currentTimeMillis() + 5000);

    // Dequeue ready items (returns null if none ready)
    String task = queue.dequeue();
    
    // Peek at next ready item without removing it
    String nextTask = queue.peek();
    
    // Check approximate queue size
    long size = queue.sizeApproximate();
}
```

### Advanced Configuration

```java
QueueConfig config = new QueueConfig()
        .setBasePath("/path/to/queue/data")
        .setSyncWrites(true)                    // Force fsync for durability
        .setDisableWAL(false)                   // Enable Write-Ahead Log
        .setDequeueBatchSize(1000)              // Batch size for dequeue operations
        .setWriteBufferSizeMB(64)               // RocksDB write buffer size
        .setMaxWriteBufferNumber(3)             // Number of write buffers
        .setCompressionType(CompressionType.LZ4); // Compression algorithm
```

## Performance

RocksQueue is designed for high-throughput scenarios:

- **Throughput**: 100K+ operations/second on modern hardware
- **Latency**: Sub-millisecond dequeue operations with cache hits
- **Scalability**: Handles millions of queued items efficiently
- **Memory**: Configurable memory usage with intelligent caching

Run the included benchmark:

```bash
./gradlew test --tests QueueThroughputSmokeTest
```

## Threading Model

- **Concurrent Producers**: Multiple threads can enqueue simultaneously
- **Serialized Dequeue**: Per-group dequeue operations are serialized to prevent duplicates
- **Process Isolation**: Java-level synchronization within single JVM
- **Multi-process**: Requires additional coordination for at-most-once delivery

## ⚠️ Performance vs. Durability Trade-off

**Design Choice**: RocksQueue prioritizes throughput over absolute durability:

### **Data Loss Risk**
- **In-memory cache**: Ready items are moved from RocksDB to in-memory `ArrayDeque`
- **Vulnerability window**: Items deleted from RocksDB before being cached in memory
- **Bad shutdown scenarios**: Process crash, `SIGKILL`, power failure, or JVM crash
- **Potential loss**: Up to `dequeueBatchSize` items (default: 2000) per crash

### **Performance Rationale**
- **Tested alternatives**: Memory-mapped durable cache implementation showed significant throughput degradation
- **Conscious trade-off**: Chose high performance over absolute durability for target use cases
- **Benchmark results**: ArrayDeque cache maintains 100K+ ops/sec vs. substantial drops with durable alternatives

### **Mitigation Strategies**
- **Graceful shutdown**: `close()` method restores cached items back to RocksDB
- **Reduce batch size**: Lower `dequeueBatchSize` to minimize exposure (trades throughput for safety)
- **Idempotent consumers**: Design consumers to handle potential message loss
- **External durability**: Consider upstream message persistence for critical data
- **Monitoring**: Track cache restoration events and unexpected shutdowns

### **Use Cases**
- ✅ **Excellent for**: High-throughput event processing, analytics pipelines, background tasks
- ✅ **Good for**: Development, testing, non-critical real-time processing
- ⚠️ **Consider alternatives**: Financial transactions, critical notifications, audit logs
- 💡 **Hybrid approach**: Use for high-volume processing with critical subset persisted elsewhere

## Architecture

### Queue Groups

Each queue group operates as an independent RocksDB instance:

```java
// These use separate RocksDB instances
TimeQueue<String> userTasks = client.getQueue("user-tasks", String.class, serializer);
TimeQueue<String> systemTasks = client.getQueue("system-tasks", String.class, serializer);
```

### Serialization

Built-in serializers for common types:

- `JsonSerializer<T>`: JSON serialization for POJOs
- `Utf8StringSerializer`: Optimized UTF-8 string serialization
- Custom serializers: Implement `Serializer<T>` interface

## Testing

Run the full test suite:

```bash
./gradlew test
```

Run specific test categories:

```bash
# Unit tests
./gradlew test --tests "dev.rocksqueue.core.*"

# Integration tests
./gradlew test --tests "dev.rocksqueue.integration.*"

# Performance tests with JFR profiling
./gradlew throughputJfr
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes and add tests
4. Run the test suite: `./gradlew test`
5. Submit a pull request

### Code Style

- Follow standard Java conventions
- Add JavaDoc for public APIs
- Include unit tests for new functionality
- Ensure all tests pass before submitting

## Roadmap

See [TODO.md](TODO.md) for planned improvements and future enhancements.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/pranaywarke/rocksqueue/issues)
- **Discussions**: [GitHub Discussions](https://github.com/pranaywarke/rocksqueue/discussions)
- **Documentation**: Check the [wiki](https://github.com/pranaywarke/rocksqueue/wiki) for detailed guides
