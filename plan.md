# 24h EC2 Benchmark Plan (RocksDB Timed Queue)

This plan runs a 24-hour continuous enqueue/dequeue benchmark for `rocksqueue` on EC2, captures system metrics in CloudWatch, and archives app throughput logs for later analysis. No code changes required for phase 1.

## Infra

- **Instance**: `m7i.large` (2 vCPU, 8 GB RAM). Avoid t-class burstable instances (credit throttling over 24h).
- **Storage**: 200 GB `gp3` EBS (baseline 3,000 IOPS, 125 MB/s). Attach as a single volume.
- **Networking**: Security group with SSH from your IP; outbound open for packages/CloudWatch.
- **AZ/Placement**: Any single AZ.

## OS & Filesystem Prep

- **Packages**: Install Amazon CloudWatch Agent, Java 21+, Git, unzip, `jq`, `tmux` (or `screen`), `sysstat` (for `iostat`), `nvme-cli`.
- **Ulimits** (for the test user):
  - `nofile=1048576`
  - `nproc=131072`
- **Sysctl** (stable defaults):
  - `vm.swappiness=10`
  - `vm.dirty_background_ratio=5`
  - `vm.dirty_ratio=20`
  - `vm.max_map_count=1048576`
  - Disable Transparent Huge Pages (set to `never`).
- **Filesystem**: Format EBS as ext4 and mount at `/data/rocksqueue` with options:
  - `noatime,nodiratime,commit=60`
  - Create `dirs`: `/data/rocksqueue/db` (DB data) and `/data/rocksqueue/logs` (logs).

## Repo & Build

- **Clone**: Pull the repo and checkout the branch to test.
- **Build**: Use wrapper to compile jars quickly: `./gradlew clean build -x test`.
- **Smoke**: Run a short smoke test locally to validate the build before the 24h run.

## Test Execution (24h)

- **Workload**: Continuous enqueue/dequeue using the existing integration test:
  - Test target: `dev.rocksqueue.integration.QueueThroughputSmokeTest.enqueue_100k_and_dequeue_immediately_print_throughput`.
  - Run in a loop for 24h using `tmux`/`screen` or `nohup`.
  - Redirect output to `/data/rocksqueue/logs/throughput.log`.
- **DB Path**: Prefer running from a working directory under `/data/rocksqueue` so RocksDB files land on the EBS volume. If/when the DB path becomes configurable, point it to `/data/rocksqueue/db`.
- **Concurrency**: Start with a single process. Optionally run a second parallel process targeting a different queue-group (separate RocksDB instance) to test tenant isolation.

## Metrics & Logs (CloudWatch)

- **CloudWatch Agent**: Configure 60s intervals for:
  - CPU: `CPUUtilization`
  - Memory: `mem_used_percent`, `swap_used_percent`
  - Disk: `disk_used_percent` for the mount, read/write ops & bytes, queue length
  - Network: bytes/packets in/out, errors
  - Process metrics: per-PID CPU and RSS for the Java/Gradle process (regex on `java`).
- **Logs**: Ship `/data/rocksqueue/logs/throughput.log` to CloudWatch Logs.
- **Dashboard**: Include CPU, memory, disk IO ops/sec, disk bytes/sec, disk utilization %, network throughput. If app throughput is parsed later, plot it too.
- **Alarms** (optional):
  - CPU > 90% for 10m
  - `mem_used_percent` > 90% for 10m
  - `disk_used_percent` > 85%
  - Sustained disk queue length > 5 for 10m

## Operational Notes

- **Run control**: Use `timeout 24h` around the loop for deterministic duration. Alternatively, manage under a `systemd` user service.
- **Stability**: Avoid burstable instances to prevent throttling during long runs.
- **JVM/GC**: Start with defaults (G1GC). If GC becomes a bottleneck, adjust heap/GC in a follow-up.
- **EBS Health**: `gp3` baseline should be sufficient; monitor IO queue and throughput.
- **Retention**: Keep CloudWatch Logs 7–14 days. Optionally upload `/data/rocksqueue/logs/throughput.log` to S3 at the end.

## Phase 2 (Optional, post-first run)

- **App metrics**: Standardize a log line like `THROUGHPUT itemsPerSec=<num>` and add a CloudWatch Logs metric filter for real-time graphing.
- **RocksDB tuning**: Block cache ~30–50% of RAM, compaction threads, WAL sync, `use_direct_io_for_flush_and_compaction=true` (SSD), and per-tenant RocksDB instances per design.
- **Multi-tenant**: Run multiple processes bound to separate DB paths to test isolation and tail-latency.
- **Latency**: If the test emits latency stats (p50/p95/p99), include them in logs and parse.

---

### Quick Execution Outline (for reference)

1) Provision EC2 `m7i.large` with 200 GB `gp3` EBS.
2) Install prerequisites, set ulimits/sysctls, THP off.
3) Format/mount EBS at `/data/rocksqueue` with recommended options.
4) Clone repo; build with `./gradlew clean build -x test`.
5) Configure and start CloudWatch Agent (metrics + log shipping).
6) Start 24h test loop in `tmux/screen`, logging to `/data/rocksqueue/logs/throughput.log`.
7) Monitor CloudWatch dashboard throughout the day.
8) After 24h, collect results (throughput trends, resource usage, anomalies).
