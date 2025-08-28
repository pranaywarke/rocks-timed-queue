# TODO: Performance, Longevity, and Durability Follow-ups

This document tracks concerns and concrete next steps for RocksQueue.

## 1) Throughput on slow hardware

- __Problem__: Lower CPU, slower storage (SATA HDD, low-end NVMe), and fewer cores can bottleneck enqueue/dequeue.
- __Potential improvements__:
  - __Tune RocksDB options per hardware tier__
    - Reduce `write_buffer_size` and `max_write_buffer_number` for low-memory boxes.
    - Consider `CompressionType` trade-offs: `NO_COMPRESSION` vs `LZ4` (measure CPU vs IO).
    - Enable `use_direct_io_for_flush_and_compaction` if kernel/distro allows and measure.
    - Adjust `max_background_jobs`, `max_subcompactions` conservatively.
  - __Batching strategies__
    - Enqueue: group small payloads using a `WriteBatchWithIndex` or standard `WriteBatch` to reduce WAL/fsync overhead.
    - Dequeue: current scan/delete batching is good; evaluate batch sizes of 256..4096 for slow disks.
  - __Threading__
    - Use a small fixed thread pool for producers/consumers mapped to hardware cores.
    - Consider separate IO vs compute pools (serialization, compression) if CPU-bound.
  - __Key/Value size__
    - Encourage compact payloads. Consider optional Snappy for values (outside RocksDB compression) if profiling shows wins.

- __Testing strategy__:
  - Provision a slow VM (2 vCPU, 2–4 GB RAM, SATA-like disk). Run `QueueThroughputSmokeTest` with:
    - `-Dq.n=2_000_000`, vary `-Dq.batch` (256, 512, 1000, 2000, 4000)
    - Toggle `-Dq.disableWAL` and `-Dq.syncWrites`
  - Capture metrics: enqueue TPS, dequeue TPS, p95/p99 latency for dequeue.
  - Run with different `CompressionType` settings and record throughput deltas.

## 2) Long-running processes (GC and compaction impact)

- __Problem__: After hours/days, GC pauses and RocksDB compaction/flush can introduce tail latency or throughput drops.
- __Potential improvements__:
  - __JVM/GC__
    - Prefer G1 or ZGC for high-throughput workloads; cap heap size to avoid long STWs.
    - Avoid large temporary allocations; reuse buffers in serializers.
    - Periodically log GC stats/JFR events to detect regressions.
  - __RocksDB compaction/flush__
    - Monitor `rocksdb.num-immutable-mem-table`, `rocksdb.cur-size-active-mem-table` and compaction pending stats.
    - Tune `level_compaction_dynamic_level_bytes = true` (default) and consider `level0` trigger thresholds.
    - Use `max_background_jobs` appropriate to hardware; avoid saturating CPU.
  - __Back-pressure__
    - If compaction debt grows, temporarily reduce enqueue rate (producer back-pressure) to stabilize.

- __Testing strategy__:
  - 6–12 hour soak test:
    - Continuous mixed workload (producers with mix of backdated/near-future timestamps and a consumer).
    - Periodically sample RocksDB properties and GC logs.
  - Use JFR profiling sessions split in 30–60 minute chunks; analyze allocation profiles and pause times.
  - Inject periodic compaction stress (e.g., smaller memtables, more L0 files) to observe tail latencies.

## 3) Bad shutdown: messages in memory may be lost

- __Problem__: Crash-stop (no `close()`) can lose messages currently staged in `readyCache`.
- __Current mitigation__: Clean shutdown persists cache back into RocksDB with WAL+fsync.
- __Options for crash resilience__:
  - __A) Spill-to-disk cache DB (recommended for high durability)__
    - Maintain a small, separate RocksDB per group for `readyCache` mirror (e.g., `<group>/.cache`).
    - On refill: write entries to cache DB before deleting from main DB; on consume: delete from cache DB.
    - On startup: drain cache DB back to main DB (idempotent) and clear.
    - Pros: survives process crash; Cons: extra write amplification.
  - __B) Idempotent delivery with consumer ack__
    - Transition to a two-phase model: move-ready-to-"inflight" (or cache DB), delete-from-ready only after ack.
    - Requires API change (ack) and retry policy.
  - __C) WAL-only approach__
    - Avoid delete-before-commit semantics: mark-ready with a tombstone key, delete later in a compaction-friendly batch.
    - More complex keyspace and cleanup logic.

- __Testing strategy__:
  - For (A):
    - Inject a crash right after `refillReadyCache()` populates cache and before consumer polls all items (e.g., add a test hook to `RocksTimeQueue` to pause after refill and `Runtime.halt(1)` in a subprocess test harness).
    - Restart and verify all items are recovered from cache DB in FIFO order.
  - For (B):
    - Simulate consumer crash before ack; ensure message reappears and is not duplicated when ack is idempotent.
  - For (C):
    - Validate that tombstoned entries survive restarts until compaction confirms safe deletion.

## Cross-cutting instrumentation

- __Expose metrics__
  - Enqueue/dequeue TPS, iterator scan timings, delete batch timings (already present; can be toggled).
  - RocksDB properties sampling: `estimate-num-keys`, L0 file counts, pending compactions.
- __Feature flags__
  - System properties to toggle WAL/sync, batch sizes, readahead, and future spill-to-disk cache.
- __Artifacts__
  - Scripts or Gradle tasks to run soak tests and gather metrics + JFR recordings.
