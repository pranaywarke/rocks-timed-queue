# TODO: Roadmap and Future Improvements

This document tracks planned improvements and known areas for enhancement in RocksQueue.

## Performance Optimizations

### 1. Hardware-Specific Tuning
- **RocksDB Configuration**: Optimize settings for different hardware tiers (CPU, memory, storage)
  - Adjust `write_buffer_size` and `max_write_buffer_number` based on available memory
  - Evaluate compression trade-offs (`NO_COMPRESSION` vs `LZ4`) for different CPU/IO profiles
  - Fine-tune `max_background_jobs` and `max_subcompactions` for available cores
- **Batching Strategies**: 
  - Implement enqueue batching using `WriteBatch` to reduce WAL/fsync overhead
  - Optimize dequeue batch sizes for different storage types
- **Payload Optimization**: Consider optional compression for large payloads

### 2. Long-Running Process Stability
- **GC Optimization**: 
  - Provide guidance for G1/ZGC configuration in high-throughput scenarios
  - Minimize temporary allocations in serialization paths
- **RocksDB Monitoring**: 
  - Expose compaction and flush metrics
  - Implement back-pressure mechanisms for compaction debt
- **Soak Testing**: Develop comprehensive long-running test suites

## Durability Enhancements

### 3. Crash Recovery Improvements
- **Current State**: Clean shutdown restores cached items to RocksDB
- **Planned Improvements**:
  - **Option A**: Durable cache using separate RocksDB instance
  - **Option B**: Two-phase commit with consumer acknowledgments
  - **Option C**: WAL-based approach with tombstone markers

## Observability and Monitoring

### 4. Enhanced Metrics
- Expose detailed performance metrics (TPS, latency percentiles)
- RocksDB internal statistics (compaction status, memory usage)
- Cache hit ratios and batch operation timings

### 5. Configuration Management
- System property-based feature flags for easy tuning
- Runtime configuration updates for non-destructive changes

## Testing and Quality

### 6. Test Infrastructure
- Automated performance regression testing
- Chaos engineering tests for crash scenarios
- Multi-platform compatibility testing

## API Enhancements

### 7. Future API Considerations
- Bulk operations for high-throughput scenarios
- Async API variants
- Transaction support for complex workflows
- Dead letter queue functionality

---

**Contributing**: If you're interested in working on any of these items, please check the [Contributing Guidelines](CONTRIBUTING.md) and open an issue to discuss your approach.
