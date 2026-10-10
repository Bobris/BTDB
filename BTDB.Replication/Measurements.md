# Replication measurements

Measurements for [ImplementationPlan.md](ImplementationPlan.md) item 5, taken on Azure on 2026-09-28. They compare
BTDB with replication disabled and enabled, measure publication and follower comparison, and restore a 100 GiB
database from Azure Blob Storage. The harness is `DBBenchmark/Replication`; its remote is either an in-memory
`IReplicationStorage` with an optional fixed latency or `AzureReplicationStorage` against a real storage account.

## Environment

Sweden Central, one resource group with an Ubuntu 24.04 VM per generation, all 8 vCPU and 64 GiB, data on the local
temporary disk, .NET 10.0.12, workstation GC, Release build, no compression, `FileSplitSize` 64 MiB:

| VM | CPU | Local disk |
| --- | --- | --- |
| E8as_v4 | AMD EPYC 7763 | 128 GiB temporary SSD |
| E8ads_v5 | AMD EPYC 7763 | 300 GiB temporary SSD |
| E8ads_v6 | AMD EPYC 9V74 | 440 GiB NVMe (560 MB/s write, 1.1 GB/s read measured with direct I/O) |
| E8ads_v7 | AMD EPYC 9V45 | 440 GiB NVMe (560 MB/s write, 1.1 GB/s read) |

Blob tests used a Standard_LRS StorageV2 account in the same region, accessed with the VM's managed identity.
Earlier runs on an Apple M2 Max found the problems fixed below; the tables show only the final code on Azure.

## How to run

```bash
cd DBBenchmark && dotnet run -c Release -- replication-commit --filter '*'
dotnet run -c Release --project DBBenchmark -- replication pipeline --dir /mnt/data --transactions 200000 --readers 4
dotnet run -c Release --project DBBenchmark -- replication compare --transactions 50000 --value 4096 --poll-transactions 500 --latency-ms 1 --inline-kb 4096
dotnet run -c Release --project DBBenchmark -- replication restore --dir /mnt/data --dataset-mb 102400 --account <storage account> --downloads 8
```

`replication all` runs pipeline, compare and restore. Options: `--transactions`, `--value` (bytes), `--readers`
(concurrent point-lookup tasks), `--latency-ms`, `--poll-ms` (publication cadence, default 50), `--poll-transactions`
(follower poll size), `--inline-kb` (poll inline budget), `--dataset-mb`, `--split-mb` (PVL size, also the TRL
size unless `--trl-soft-mb`/`--trl-hard-mb` set TRL rotation limits), `--downloads` (restore
concurrency), `--dir` (local storage location, e.g. a temporary disk), `--memory` (in-memory local storage) and
`--account`/`--container` (Azure Blob remote, authenticated with `DefaultAzureCredential`). The BenchmarkDotNet job
uses a fixed invocation count: every commit appends to a TRL that nothing compacts, and BenchmarkDotNet's pilot
otherwise grows the database until disk or memory runs out (a first attempt filled 58 GB).

## Local commit cost (BenchmarkDotNet)

One single-key commit. Standalone uses `InMemoryFileCollection`, `OnDiskFileCollection` (File) or
`OnDiskMemoryMappedFileCollection` (Mapped); replicated uses `ReplicationFileSet` over `InMemoryReplicationFileStorage`
or `OnDiskReplicationFileStorage` with `TransactionLogCapture` and `StartWritingTransaction(eventId)`. Mean and
allocation per commit:

| Storage | Value | E8as_v4 | E8ads_v5 | E8ads_v6 | E8ads_v7 |
| --- | ---: | ---: | ---: | ---: | ---: |
| StandaloneMemory | 64 B | 1.75 µs, 713 B | 1.75 µs, 713 B | 1.43 µs, 713 B | 1.03 µs, 713 B |
| StandaloneMemory | 4096 B | 4.72 µs, 8340 B | 4.89 µs, 8339 B | 4.19 µs, 8339 B | 2.99 µs, 8340 B |
| StandaloneFile | 64 B | 3.09 µs, 633 B | 3.07 µs, 633 B | 2.70 µs, 633 B | 2.10 µs, 633 B |
| StandaloneFile | 4096 B | 5.83 µs, 633 B | 5.96 µs, 633 B | 5.03 µs, 633 B | 4.16 µs, 633 B |
| StandaloneMapped | 64 B | 1.94 µs, 633 B | 1.88 µs, 633 B | 1.51 µs, 633 B | 1.10 µs, 633 B |
| StandaloneMapped | 4096 B | 10.14 µs, 633 B | 10.48 µs, 633 B | 8.04 µs, 633 B | 8.08 µs, 633 B |
| ReplicatedMemory | 64 B | 1.85 µs, 713 B | 1.85 µs, 713 B | 1.52 µs, 713 B | 1.07 µs, 713 B |
| ReplicatedMemory | 4096 B | 4.91 µs, 8341 B | 4.85 µs, 8340 B | 4.21 µs, 8341 B | 3.09 µs, 8340 B |
| ReplicatedMapped | 64 B | 2.03 µs, 633 B | 1.84 µs, 633 B | 1.84 µs, 633 B | 1.16 µs, 633 B |
| ReplicatedMapped | 4096 B | 3.18 µs, 747 B | 3.29 µs, 747 B | 2.71 µs, 747 B | 2.11 µs, 747 B |

Replication adds no measurable commit cost in memory. On disk, `OnDiskReplicationFileStorage` is the fastest local
storage for 4 KiB values (2.1–3.3 µs against 4.2–6.0 µs for File and 8.0–10.5 µs for Mapped); the extra 114 B per
commit are pooled block bookkeeping.

## Commits with concurrent publication and readers

Sequential single-key commits (200,000 × 256 B or 50,000 × 4 KiB); Publish runs a publisher with the coordinator's
cadence (publish, then wait 50 ms) to the in-memory remote; CaptureOnly enables replication without a consumer.
Readers are four tasks doing random point lookups of committed keys in read-only transactions for the whole run.
Throughput, p99.9 and maximum commit latency, and reads per second:

256 B values, no readers:

| Mode | E8as_v4 | E8ads_v5 | E8ads_v6 | E8ads_v7 |
| --- | ---: | ---: | ---: | ---: |
| Standalone | 245k/s, p99.9 42 µs, max 8.3 ms | 243k/s, p99.9 37 µs, max 9.7 ms | 302k/s, p99.9 18 µs, max 6.4 ms | 257k/s, p99.9 28 µs, max 7.9 ms |
| StandaloneMapped | 408k/s, p99.9 18 µs, max 8.8 ms | 414k/s, p99.9 17 µs, max 8.9 ms | 561k/s, p99.9 31 µs, max 6.6 ms | 723k/s, p99.9 28 µs, max 5.3 ms |
| CaptureOnly | 453k/s, p99.9 17 µs, max 9.0 ms | 447k/s, p99.9 25 µs, max 9.6 ms | 559k/s, p99.9 13 µs, max 7.0 ms | 762k/s, p99.9 10 µs, max 5.6 ms |
| Publish | 467k/s, p99.9 24 µs, max 8.6 ms | 455k/s, p99.9 24 µs, max 8.4 ms | 606k/s, p99.9 14 µs, max 6.5 ms | 794k/s, p99.9 11 µs, max 5.0 ms |

4 KiB values, no readers:

| Mode | E8as_v4 | E8ads_v5 | E8ads_v6 | E8ads_v7 |
| --- | ---: | ---: | ---: | ---: |
| Standalone | 131k/s, p99.9 33 µs, max 20.6 ms | 133k/s, p99.9 35 µs, max 17.0 ms | 78k/s, p99.9 27 µs, max 114.3 ms | 82k/s, p99.9 22 µs, max 111.1 ms |
| StandaloneMapped | 92k/s, p99.9 4533 µs, max 14.8 ms | 98k/s, p99.9 4321 µs, max 9.9 ms | 126k/s, p99.9 1903 µs, max 5.7 ms | 125k/s, p99.9 1911 µs, max 6.1 ms |
| CaptureOnly | 269k/s, p99.9 290 µs, max 2.7 ms | 270k/s, p99.9 283 µs, max 2.4 ms | 343k/s, p99.9 196 µs, max 2.2 ms | 453k/s, p99.9 192 µs, max 1.9 ms |
| Publish | 257k/s, p99.9 299 µs, max 2.8 ms | 252k/s, p99.9 290 µs, max 2.4 ms | 331k/s, p99.9 202 µs, max 2.3 ms | 441k/s, p99.9 192 µs, max 2.6 ms |

256 B values, four readers:

| Mode | E8as_v4 | E8ads_v5 | E8ads_v6 | E8ads_v7 |
| --- | ---: | ---: | ---: | ---: |
| Standalone | 35k/s, p99.9 102 µs, max 17.2 ms, 1.1M reads/s | 38k/s, p99.9 100 µs, max 19.6 ms, 1.1M reads/s | 28k/s, p99.9 77 µs, max 15.9 ms, 1.5M reads/s | 38k/s, p99.9 70 µs, max 15.0 ms, 2.1M reads/s |
| StandaloneMapped | 183k/s, p99.9 50 µs, max 16.2 ms, 3.5M reads/s | 182k/s, p99.9 52 µs, max 15.0 ms, 3.4M reads/s | 304k/s, p99.9 35 µs, max 11.3 ms, 4.0M reads/s | 387k/s, p99.9 31 µs, max 6.8 ms, 4.0M reads/s |
| CaptureOnly | 285k/s, p99.9 40 µs, max 19.4 ms, 4.5M reads/s | 203k/s, p99.9 61 µs, max 24.1 ms, 4.9M reads/s | 253k/s, p99.9 38 µs, max 16.3 ms, 6.5M reads/s | 364k/s, p99.9 19 µs, max 12.2 ms, 8.0M reads/s |
| Publish | 249k/s, p99.9 50 µs, max 23.1 ms, 5.0M reads/s | 204k/s, p99.9 58 µs, max 24.4 ms, 5.2M reads/s | 354k/s, p99.9 18 µs, max 15.3 ms, 6.1M reads/s | 543k/s, p99.9 14 µs, max 11.6 ms, 8.3M reads/s |

4 KiB values, four readers:

| Mode | E8as_v4 | E8ads_v5 | E8ads_v6 | E8ads_v7 |
| --- | ---: | ---: | ---: | ---: |
| Standalone | 42k/s, p99.9 137 µs, max 26.6 ms, 1.4M reads/s | 49k/s, p99.9 110 µs, max 19.7 ms, 1.3M reads/s | 48k/s, p99.9 82 µs, max 113.5 ms, 1.8M reads/s | 47k/s, p99.9 75 µs, max 113.4 ms, 2.5M reads/s |
| StandaloneMapped | 65k/s, p99.9 4840 µs, max 13.5 ms, 1.3M reads/s | 63k/s, p99.9 4677 µs, max 9.7 ms, 1.5M reads/s | 95k/s, p99.9 1929 µs, max 6.5 ms, 1.7M reads/s | 110k/s, p99.9 1968 µs, max 6.9 ms, 1.8M reads/s |
| CaptureOnly | 147k/s, p99.9 371 µs, max 11.1 ms, 3.8M reads/s | 216k/s, p99.9 291 µs, max 8.0 ms, 3.6M reads/s | 192k/s, p99.9 268 µs, max 7.9 ms, 4.5M reads/s | 326k/s, p99.9 218 µs, max 7.7 ms, 5.9M reads/s |
| Publish | 220k/s, p99.9 316 µs, max 8.9 ms, 3.9M reads/s | 151k/s, p99.9 371 µs, max 11.4 ms, 4.0M reads/s | 201k/s, p99.9 260 µs, max 3.9 ms, 4.5M reads/s | 330k/s, p99.9 216 µs, max 7.9 ms, 5.2M reads/s |

- With concurrent readers, replication storage sustains 3–14x the commits and 2.1–4.5x the reads of
  `OnDiskFileCollection`. Against the memory-mapped standalone collection, whose readers and remaps serialize on a
  file lock, it serves 1.3–3.3x the reads, and with 4 KiB values 2.3–3.5x the commits.
- Publication allocates almost nothing per commit beyond the stored copy and coalesces thousands of transactions into
  each canonical write; the lag is about the cadence times the commit rate. Its effect on the writer is within the
  run-to-run variance, which reaches about ±30 % with concurrent readers.
- The remaining 4 KiB p99.9 (about 200–300 µs) comes from the mapping steps and the 1 MiB block writes.
- Standalone `OnDiskFileCollection` still shows 110 ms maxima on NVMe VMs: it fsyncs each 64 MiB TRL at rotation.

## Follower comparison

Leader and follower (restored from the leader's published genesis) apply the same transactions; the follower
compares after every poll, once by 256 KiB range reads only (as over HTTP) and once with inline poll bytes plus
`RetainingLeaderTrlReader`. A modelled 1 ms round trip is added per poll and per range read. E8ads_v7:

| Poll delta | Range reads only | Inline, 1 MiB budget | Inline, 4 MiB budget |
| --- | --- | --- | --- |
| 2,000 × 256 B (547 KiB) | 3.0 range reads/poll, 4.2 ms/poll | 0 range reads, 1.3 ms/poll | – |
| 500 × 4 KiB (2 MiB) | 8.1 range reads/poll, 9.3 ms/poll | 4.0 range reads/poll (macOS run) | 0 range reads, 1.6 ms/poll |

In-process comparison runs at 7–14 GiB/s, so round trips dominate; the coordinator now requests the 4 MiB maximum.

### Over HTTP

`HttpReplicationPeerTransportTest.MeasurePollLatencyAndInlineThroughput` (opt-in with `BTDB_REPLICATION_MEASURE=1`,
Release build) polls a leader endpoint over real Kestrel HTTP on loopback, with the binary peer encoding: an empty
poll takes 85 µs and a poll carrying the full 4 MiB inline budget 1.2 ms, 3.4 GiB/s, on an Apple M-series laptop;
223 µs and 2.6 ms (1.5 GiB/s) on an E8ads_v5 VM. The
transport therefore adds far less than the network round trip and stays an order of magnitude above the publication
rates measured above; between VMs, the round trip, not the HTTP stack, sets the cost of a follower step.

## Several databases publishing at once

`DBBenchmark replication multi --databases N` writes 4 KiB values into N leader databases concurrently, each
publishing to its own prefix of the same account (E8ads_v5, on-disk local storage):

| Databases | Data per database | Aggregate TRL publication | Per database |
| ---: | ---: | ---: | ---: |
| 1 | 2 GiB | 189 MiB/s | 189 MiB/s |
| 4 | 2 GiB | 651 MiB/s | 163 MiB/s |
| 1 | 1 GiB | 164 MiB/s | 164 MiB/s |
| 8 | 1 GiB | 802 MiB/s | 100 MiB/s |

Each database has its own publication lane, so publication scales with the number of databases until the VM's local
disk (about 835 MB/s of writes on this size) becomes the limit; the storage account was not throttled.

## Restore from Azure Blob Storage

The leader writes N × 4 KiB values, overwrites half of them at random, compacts until done, publishes a checkpoint
(KVI + PVLs) and adds a 10 % overwrite tail, publishing TRL throughout. Cold restore starts from an empty directory;
warm restore reuses the directory left by the cold one. E8ads_v7 with its NVMe temporary disk:

| Dataset | Downloads | Remote TRL + PVL/KVI | TRL publication | Checkpoint upload | Cold restore | Warm restore |
| --- | ---: | --- | ---: | ---: | --- | --- |
| 10 GiB | 4 | 16.1 + 7.6 GiB | 109 MiB/s¹ | 443 MiB/s | 11.4 GiB in 31.5 s, 371 MiB/s | 2.5 s, 4 MiB downloaded |
| 10 GiB | 16 | 16.1 + 7.6 GiB | 111 MiB/s¹ | 450 MiB/s | 11.4 GiB in 26.4 s, 443 MiB/s | 1.7 s, 4 MiB |
| 10 GiB | 8 | 16.1 + 7.6 GiB | 227 MiB/s | 447 MiB/s | 11.4 GiB in 27.7 s, 422 MiB/s | 1.8 s, 4 MiB |
| 100 GiB | 8 | 160.7 + 75.6 GiB | 175 MiB/s | 326 MiB/s | 114.1 GiB in 265 s, 440 MiB/s | 100 s, 43 MiB |

¹ Before `AzureReplicationStorage` staged TRL blocks concurrently. Publication figures include the local writes.

- **The 100 GB / 15 minute startup target is met:** a cold restore of 100 GiB of data (114 GiB of files) completes in
  4.4 minutes, including download, validation, KVI open and TRL replay.
- Restore is limited by the VM's local disk, not by Blob Storage: 440 MiB/s is about 80 % of the NVMe temporary
  disk's 560 MB/s write limit, and 16 instead of 4 concurrent downloads adds only 20 %. A larger VM (higher disk
  limits) should restore faster.
- Warm restore downloads only the active TRL tail: sealed TRLs carry their SHA-256 and are reused. At 100 GiB its
  time is the parallel SHA-256 validation of the cache (93 s, disk-read bound at about 1.2 GiB/s).
- Local compaction of the 100 GiB dataset with 64 MiB files took 2,555 s against 19 s for 10 GiB: it scales
  superlinearly with the number of files. With production file sizes (below) it rarely runs, so this is not a
  priority.

### 30 GiB with production file sizes

The same scenario with 2 GiB PVLs (`FileSplitSize` = `int.MaxValue`) and TRLs rotating at a 1 GiB soft and
4,095 MiB hard limit (`--split-mb 2048 --trl-soft-mb 1024 --trl-hard-mb 4095`), 30 GiB of data and 8 concurrent
downloads, run on all four generations at once against the same storage account. With these sizes no TRL reaches the
compaction threshold (a quarter of `FileSplitSize` wasted in one file), so the checkpoint is a KVI of 81 MiB that
references 48.2 GiB of TRL, and restore downloads all of it:

| VM | TRL publication | Cold restore | Warm restore |
| --- | ---: | --- | --- |
| E8as_v4 | 153 MiB/s | 48.3 GiB in 155.5 s, 318 MiB/s | 8.4 s, 204 MiB downloaded |
| E8ads_v5 | 175 MiB/s | 48.3 GiB in 83.3 s, 594 MiB/s | 35.7 s, 204 MiB |
| E8ads_v6 | 171 MiB/s | 48.3 GiB in 117.5 s, 421 MiB/s | 21.7 s, 204 MiB |
| E8ads_v7 | 194 MiB/s | 48.3 GiB in 119.6 s, 414 MiB/s | 22.0 s, 204 MiB |

- Cold restore follows each VM's local disk: the v5 temporary SSD sustains more writes than the v6/v7 NVMe
  temporary disks (560 MB/s), and the smaller v4 disk the least.
- Warm restore downloads only the active TRL (up to the 1 GiB soft limit); sealed 1 GiB TRLs are reused by checksum.
  It ran right after the cold restore, so part of its validation read from the page cache; after a reboot it is
  bound by disk reads (about 1.1 GB/s on v6/v7).
- Extrapolated to 100 GiB of data in this shape (about 160 GiB of TRL), a cold restore takes about 5–9 minutes on
  these VMs.

### KVI-heavy database

Real databases have a KVI of about 20–30 % of their size. This dataset uses 64 B keys (pseudo-random after the ID, so
prefix compression cannot shrink them) and 200 B values: 10 GiB of data, 53.7 million keys, a 3.3 GiB KVI, 7.2 GiB of
PVL and 21.9 GiB of TRL, of which a restore downloads 16.4 GiB (`--dataset-mb 10240 --value 200 --key-bytes 64` with
the production file sizes above). Each VM restored its own published copy with the current code and with the
libraries of the previous commit (`--reuse-prefix`), twice each; means of both rounds:

| VM | Cold, KVI streamed | Cold, before | Warm, KVI hashed while loading | Warm, before |
| --- | ---: | ---: | ---: | ---: |
| E8as_v4 | 73.9 s | 102.0 s (−28 %) | 34.3 s | 44.7 s (−23 %) |
| E8ads_v5 | 54.4 s | 78.9 s (−31 %) | 31.2 s | 40.6 s (−23 %) |
| E8ads_v6 | 64.1 s | 92.0 s (−30 %) | 31.5 s | 41.8 s (−25 %) |
| E8ads_v7 | 58.8 s | 83.9 s (−30 %) | 25.0 s | 32.9 s (−24 %) |

- Before, open downloaded or hashed the whole KVI, loaded it, and only then fetched the files it references; warm
  initialization hashed the entire cache first. Now the KVI loads while it downloads (cold) or while a parallel task
  hashes it (warm), and each referenced file is fetched as soon as the load reaches it.
- The rest is bound by one KVI load thread (tens of millions of keys into the B-tree) and the local disk. Runs of the
  same code vary by up to about 15 %, so differences below that are not meaningful; 16 instead of 4 blocks in flight
  for the streamed KVI was within that noise and was not kept.

### Why newer generations are not proportionally faster

CPU-bound work does scale: a single commit takes 1.75 µs on v4 and 1.03 µs on v7, and sealed-file SHA-256 runs at
1.6 GB/s on v5 against 2.1 GB/s on v7. Restore, however, is bound by limits Azure sets per VM size, not by generation:
the 8-vCPU temporary disk writes at 835 MB/s on E8ads_v5 (temporary SSD) but only 560 MB/s on E8ads_v6/v7 (NVMe,
which reads at 1.1 GB/s), measured with direct 4 MiB I/O. A cold restore writes every downloaded byte, so v5 can match
or beat v7 there, while v7 wins where the KVI load (CPU) dominates. A larger VM size raises these limits.

## Event log

Storage primitives on Azure Standard_GZRS are recorded in
[EventLogImplementationPlan.md](EventLogImplementationPlan.md#16-live-gzrs-storage-screening) (`DBBenchmark
eventlog-storage`). `DBBenchmark eventlog-e2e` measures publish-to-all-nodes latency: open-loop arrivals round-robin
over nodes in one process, loopback HTTP, in-memory storage and 1 KiB records. Every node follows the topic. The
latency of a record ends when the last node receives it.

Run on 2026-09-29, Apple M2 Max, Release:

| Nodes | Rate | Records | p50 ms | p95 ms | p99 ms | p99.9 ms | max ms | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 3 | 500/s | 1,500 | 0.18 | 0.45 | 1.37 | 5.64 | 7.70 | 0 |
| 4 | 5,000/s | 20,000 | 0.14 | 1.54 | 2.66 | 5.84 | 7.22 | 0 |

This is the library and loopback floor. Storage latency adds to every commit: about 9–27 ms p50–p99 per bounded Put
Blob on GZRS. Multi-VM and live Azure end-to-end runs on the target workload are still open.

## Event log on AKS

`EventLogBench` (see its [README](../EventLogBench/README.md)) ran on 2026-10-10 in Sweden Central: AKS 1.35 with five
Standard_E8ads_v5 nodes in zones 1–3 (one benchmark pod per node, one driver node), one Standard_GZRS StorageV2
account, workload identity, HTTPS/HTTP2 between pods with a private CA, .NET 10, server GC, default `EventLogOptions`.
Every pod published its share and consumed the whole topic; the first pod owned it. Raw EventLog records were 1 KiB;
event-sourcing (ES) events carried a 256-byte payload, were consumed on every pod into its own on-disk ObjectDB (one
relation upsert per event, `MaximumBatchSize` 32) and completed `PublishAndWaitAsync` after local visibility. Each
scenario ran 20 s; latency excludes the first second. Clock offsets between pods were estimated with 0.1–0.5 ms minimum
RTT and drifted less than 0.03 ms per scenario. All 96 runs (11.7 million records in the final 64) completed without
errors, missing deliveries or duplicates.

Open loop, delivery to the last pod, p50 / p99 ms:

| Load | 1 pod | 2 pods | 3 pods | 4 pods |
| --- | ---: | ---: | ---: | ---: |
| EventLog 100/s | 8.5 / 23.5 | 9.9 / 24.3 | 9.3 / 20.0 | 8.9 / 19.6 |
| EventLog 1,000/s | 12.9 / 27.5 | 14.1 / 27.2 | 13.9 / 27.0 | 13.2 / 29.6 |
| EventLog 5,000/s | 15.6 / 32.7 | 14.8 / 28.0 | 14.9 / 52.5 | 16.3 / 42.0 |
| EventLog 10,000/s | 25.5 / 55.5 | 19.6 / 39.7 | 17.0 / 47.0 | 19.4 / 43.4 |
| ES 100/s | 9.7 / 27.8 | 10.6 / 30.9 | 12.6 / 27.3 | 12.0 / 29.7 |
| ES 1,000/s | 13.7 / 29.6 | 15.2 / 36.8 | 14.6 / 27.9 | 14.8 / 35.5 |
| ES 5,000/s | 16.6 / 33.6 | 17.2 / 513.5 | 15.0 / 35.0 | 15.9 / 30.3 |
| ES 10,000/s | 20.4 / 52.6 | 19.1 / 44.3 | 16.1 / 79.7 | 17.3 / 37.5 |

Closed loop, C publish-and-await loops per pod: records/s and receipt p50 ms:

| Load | 1 pod | 2 pods | 3 pods | 4 pods |
| --- | ---: | ---: | ---: | ---: |
| EventLog C=1 | 114 @ 8.3 | 106 @ 18.2 | 161 @ 18.2 | 207 @ 18.7 |
| EventLog C=16 | 927 @ 16.6 | 1,741 @ 17.7 | 2,552 @ 18.4 | 3,414 @ 17.8 |
| EventLog C=128 | 6,931 @ 17.5 | 12,602 @ 19.3 | 15,905 @ 21.7 | 23,119 @ 20.8 |
| EventLog C=1024 | 24,181 @ 40.7 | 23,108 @ 86.4 | 23,301 @ 130.5 | 23,349 @ 171.4 |
| ES C=1 | 106 @ 8.9 | 102 @ 18.6 | 166 @ 17.3 | 218 @ 17.2 |
| ES C=16 | 828 @ 18.4 | 1,670 @ 18.5 | 2,555 @ 18.0 | 3,406 @ 18.0 |
| ES C=128 | 5,790 @ 19.9 | 12,619 @ 19.2 | 19,057 @ 18.9 | 25,512 @ 18.8 |
| ES C=1024 | 27,497 @ 33.9 | 49,062 @ 39.4 | 71,866 @ 41.3 | 75,516 @ 53.2 |

One conditional Put Blob of the owner took 7.9–10.5 ms p50 and 16–43 ms p99 (including background merges). A single
publisher therefore commits in about one write (8.3 ms p50); records that arrive while a write is in flight wait for
it and then for their own, so typical latency is 1.5–2 writes. ES consumption added 0.02–2 ms from reader to handler
and about 0.1 ms to batch visibility. The topic owner sustains about 100–115 writes/s, so throughput is bounded by the
batch per write: about 24,000 1 KiB records/s (23 MiB/s) and 76,000 small events/s, regardless of the number of pods.
Closed loops with few publishers and two or more pods take two writes, because the first record to reach an idle owner
starts a write alone. One scenario (ES 5,000/s, 2 pods) saw storage writes up to 293 ms for seven seconds while a
background merge ran, raising its p99 to 513 ms.

The same image with `MaxPublishRoundsInFlight = 1` and the previous owner lane (the code before these measurements)
had, in p50 receipt ms (final code in parentheses):

| Load | 2 pods | 4 pods |
| --- | ---: | ---: |
| EventLog 1,000/s | 26.9 (14.0) | 29.3 (13.1) |
| EventLog 5,000/s | 33.5 (14.5) | 30.5 (16.0) |
| EventLog C=16 | 29.2 (17.7), 1,138 (1,741)/s | 35.1 (17.8), 1,987 (3,414)/s |
| EventLog C=128 | 30.6 (19.3), 7,578 (12,602)/s | 55.9 (20.8), 9,651 (23,119)/s |
| ES 1,000/s | 28.4 (15.1) | 29.5 (14.6) |
| ES 5,000/s | 29.2 (16.9) | 30.0 (15.6) |
| ES C=16 | 37.4 (18.5), 854 (1,670)/s | 39.3 (18.0), 1,613 (3,406)/s |
| ES C=128 | 36.5 (19.2), 6,872 (12,619)/s | 39.4 (18.8), 12,843 (25,512)/s |

### Commit window

`EventLogOptions.CommitLinger` was measured on a second cluster of the same shape on 2026-10-10: a sweep of 0, 0.5, 1
and 2 ms over the unsaturated loads (`--suite linger`, 15 s each), then the full suite without a window and with 1 ms
(20 s each). All 176 runs completed without errors or missing deliveries. Receipt p50 ms and records/s, without → with
a 1 ms window:

| Load | 1 pod | 2 pods | 3 pods | 4 pods |
| --- | ---: | ---: | ---: | ---: |
| EventLog 100/s | 9.0 → 15.3 | 10.0 → 13.7 | 9.7 → 11.1 | 12.4 → 14.3 |
| EventLog 1,000/s | 14.1 → 15.1 | 14.2 → 14.5 | 14.4 → 14.6 | 16.4 → 15.2 |
| EventLog 10,000/s | 29.4 → 16.5 | 19.5 → 17.7 | 17.3 → 17.5 | 18.2 → 18.6 |
| EventLog C=1 | 9.2 → 10.5, 102 → 90 | 17.9 → 9.7, 106 → 182 | 17.9 → 9.8, 162 → 254 | 18.7 → 11.3, 205 → 273 |
| EventLog C=16 | 19.0 → 10.2, 800 → 1,506 | 17.9 → 10.4, 1,713 → 2,374 | 16.9 → 12.3, 2,759 → 3,134 | 19.1 → 19.4, 3,192 → 3,715 |
| EventLog C=128 | 19.6 → 10.6, 6,206 → 11,347 | 19.0 → 21.0, 12,685 → 12,617 | 20.0 → 20.6, 17,031 → 20,574 | 21.8 → 23.2, 21,663 → 20,621 |
| EventLog C=1024 | 43.1 → 45.9, 22,722 → 21,486 | 83.8 → 81.7, 23,653 → 24,614 | 124.9 → 134.5, 24,292 → 22,514 | 187.4 → 180.7, 21,390 → 22,583 |
| ES 100/s | 12.2 → 13.9 | 9.7 → 13.9 | 11.2 → 16.0 | 12.6 → 16.1 |
| ES 1,000/s | 14.7 → 15.3 | 14.4 → 15.7 | 15.1 → 16.3 | 15.9 → 16.0 |
| ES 10,000/s | 22.0 → 16.6 | 17.5 → 17.5 | 16.2 → 18.5 | 17.5 → 17.6 |
| ES C=1 | 9.9 → 10.6, 96 → 89 | 17.4 → 10.0, 111 → 183 | 17.8 → 10.7, 162 → 242 | 19.6 → 12.2, 195 → 257 |
| ES C=16 | 19.9 → 10.5, 777 → 1,460 | 18.1 → 10.8, 1,708 → 2,272 | 17.7 → 18.4, 2,627 → 2,885 | 19.2 → 19.4, 3,243 → 3,671 |
| ES C=128 | 19.9 → 11.3, 5,791 → 9,119 | 18.5 → 20.3, 12,978 → 13,322 | 19.3 → 21.8, 18,669 → 17,261 | 20.6 → 21.6, 23,389 → 22,988 |
| ES C=1024 | 34.9 → 26.6, 26,941 → 35,133 | 39.3 → 34.6, 49,292 → 55,625 | 39.9 → 42.1, 75,339 → 71,231 | 58.2 → 57.4, 68,822 → 69,778 |

The window helps publish-and-await loops whose next record arrives shortly after a commit: one publisher per pod on
2–4 pods fell from 18–19 ms to 10–11 ms, and 16 loops on 1–2 pods from 18–19 ms to about 10 ms with up to twice the
throughput. It only works when it is longer than the round trip of the other pods' answers: with 16 loops on 3–4 pods
1 ms was not enough and 2 ms brought p50 to 12 ms in the sweep. Open-loop traffic pays for it: 1.5–6 ms more at 100/s,
up to 1 ms at 1,000/s and nothing measurable from 5,000/s. Saturated throughput did not change for 1 KiB records.
A fixed window is therefore opt-in; the adaptive window below is the default.

### Adaptive commit window

`AdaptiveCommitLingerMax` = 3 ms was compared on a third cluster of the same shape with no window and fixed 1 and 2 ms
windows (`--suite adaptive`, 15 s each), which adds a mixed load: 1,000 independent records/s plus 4
publish-and-await loops per pod at once. Receipt p50 ms (none / 1 ms / 2 ms / adaptive) and the adaptive window's
median:

| Load | 1 pod | 2 pods | 3 pods | 4 pods |
| --- | ---: | ---: | ---: | ---: |
| EventLog 100/s | 9.5 / 10.6 / 16.1 / 12.5 (0.00) | 14.7 / 13.6 / 16.1 / 16.5 (0.00) | 12.1 / 16.1 / 15.9 / 12.9 (0.00) | 10.4 / 15.9 / 16.5 / 10.9 (0.00) |
| EventLog 1,000/s | 13.9 / 14.1 / 14.9 / 15.2 (0.00) | 16.7 / 15.4 / 16.2 / 16.7 (0.00) | 16.4 / 16.4 / 15.8 / 15.4 (0.00) | 14.9 / 15.8 / 16.9 / 14.7 (0.00) |
| EventLog 5,000/s | 17.5 / 15.1 / 15.9 / 15.6 (0.38) | 17.7 / 16.1 / 17.3 / 17.9 (0.00) | 17.5 / 16.6 / 16.8 / 16.8 (0.00) | 16.1 / 17.2 / 17.3 / 16.2 (0.00) |
| EventLog C=1 | 9.0 / 9.7 / 10.9 / 9.2 (0.00) | 18.7 / 9.9 / 11.7 / 11.0 (1.12) | 20.1 / 10.3 / 11.1 / 10.6 (1.12) | 18.2 / 18.3 / 11.7 / 10.1 (1.31) |
| EventLog C=16 | 18.4 / 10.0 / 11.2 / 9.2 (0.19) | 19.4 / 10.5 / 11.6 / 10.8 (1.31) | 19.8 / 13.3 / 11.9 / 11.2 (1.50) | 19.7 / 20.0 / 11.8 / 10.8 (1.69) |
| EventLog C=128 | 18.9 / 10.7 / 11.7 / 10.0 (0.56) | 20.6 / 21.7 / 12.9 / 12.4 (2.06) | 23.8 / 21.2 / 23.0 / 20.8 (2.81) | 22.6 / 23.8 / 24.8 / 21.7 (3.00) |
| EventLog mixed | 15.1 / 12.6 / 13.2 / 13.5 (0.38) | 17.7 / 13.3 / 13.6 / 14.2 (1.31) | 17.7 / 14.1 / 12.3 / 12.3 (1.12) | 17.4 / 17.6 / 12.4 / 11.3 (1.50) |
| ES 100/s | 10.7 / 14.8 / 14.5 / 12.7 (0.00) | 14.4 / 14.4 / 17.7 / 14.8 (0.00) | 16.5 / 14.3 / 16.0 / 11.2 (0.00) | 13.5 / 16.0 / 17.0 / 11.6 (0.00) |
| ES 1,000/s | 14.3 / 15.7 / 15.2 / 15.1 (0.00) | 16.6 / 15.6 / 17.8 / 15.6 (0.00) | 16.8 / 16.1 / 16.5 / 15.4 (0.00) | 15.6 / 16.7 / 16.9 / 15.3 (0.00) |
| ES 5,000/s | 18.5 / 15.5 / 16.3 / 15.5 (0.38) | 19.2 / 16.6 / 17.5 / 16.5 (0.00) | 17.4 / 16.6 / 16.8 / 15.8 (0.00) | 16.5 / 17.2 / 16.7 / 15.7 (0.00) |
| ES C=1 | 9.1 / 10.3 / 11.2 / 9.5 (0.00) | 19.5 / 10.6 / 12.3 / 10.6 (1.12) | 19.5 / 10.3 / 11.7 / 10.5 (1.12) | 19.4 / 19.0 / 11.4 / 10.5 (1.12) |
| ES C=16 | 17.9 / 10.7 / 11.5 / 10.0 (0.38) | 19.3 / 11.4 / 12.2 / 11.2 (1.31) | 20.1 / 18.0 / 11.7 / 11.6 (1.69) | 18.5 / 19.6 / 12.5 / 11.8 (1.88) |
| ES C=128 | 19.0 / 11.2 / 12.4 / 11.7 (1.12) | 22.4 / 21.4 / 16.6 / 14.9 (3.00) | 20.7 / 21.3 / 22.5 / 15.7 (3.00) | 20.6 / 22.5 / 24.2 / 22.6 (3.00) |
| ES mixed | 15.3 / 13.6 / 13.1 / 14.3 (0.75) | 17.0 / 14.9 / 13.7 / 14.4 (1.31) | 17.7 / 16.0 / 12.8 / 13.0 (1.50) | 18.1 / 18.0 / 13.2 / 11.9 (1.69) |

Across these 56 scenarios the adaptive window was on average 3.5 % above the best fixed choice for each scenario and
within 10 % of it in 50; no window averaged 35 %, 1 ms 18 % and 2 ms 14 %. It stayed closed for independent arrivals
and for one publish-and-await loop on one pod, and chose 1.1–1.7 ms for loops on 2–4 pods and 2–3 ms for 128 loops.
The full suite with the adaptive window showed no loss at saturation (1 KiB records 21–23k/s, small events up to
70k/s) and kept the closed-loop gains (for example 16 loops on 4 pods: 18.5 → 11.1 ms, 2,914 → 5,083 records/s).
Two corrections came from this measurement: arrivals are timed from the moment a write would start without a window,
so a periodic source that always reaches an idle owner shortly after a commit no longer opens one, and a window ends
once the queue fills the next batch. All runs, 65 million records, completed without errors or missing deliveries.

## Changes made from these measurements

- `OnDiskReplicationFileStorage`: lock-free reads from pinned blocks and read-only mappings, mappings grown in steps
  instead of remapped every 4 MiB, pooled blocks with a generation check, no fsync at seal (every cached file is
  validated against the remote before use). This removed a p99.9 stall of about 0.9 ms, 11–12 gen2 GCs per 200 MB
  written, a 110 ms stall at every TRL rotation, and lock contention that capped concurrent reads.
- Sealed TRLs record their whole-file SHA-256, so warm restores reuse them (previously 41 % of the cold bytes were
  downloaded again).
- Open streams the KVI while it downloads or while its checksum runs and fetches referenced files during the load;
  initialization no longer hashes the cache (above).
- Followers request the 4 MiB inline maximum per poll and retain two polls' worth of leader bytes.
- `AzureReplicationStorage` stages four TRL blocks at once; one at a time capped publication at about 110 MiB/s.
- EventLog publishers pipeline publication rounds (`MaxPublishRoundsInFlight`, default 64). With one round at a time a
  record published during the owner's write waited for two writes; 8 rounds were too few for event sourcing, whose
  concurrent callers form single-record rounds (ES at 64 loops per pod: 4,700 events/s at 26 ms p50 with 8 rounds,
  7,100 at 17.5 ms with 64).
- The EventLog owner fills and seals a partly written split when the queue exceeds its remaining room, instead of
  spending a seal-only write. Closed loops had settled on small batches: EventLog at 128 loops on 2 pods rose from
  7,000 to 12,600 records/s and its p50 fell from 36 to 19 ms.
- The EventLog owner chooses a commit window from the arrival times by default (`AdaptiveCommitLingerMax`, 3 ms);
  `CommitLinger` sets a fixed one (see [Commit window](#commit-window) and
  [Adaptive commit window](#adaptive-commit-window)).

## Not covered

- Warm-restore validation time for very large caches after a reboot (without page cache).
- TLS and a real network between VMs on the HTTP peer path (loopback HTTP is measured above).
- Multi-database contention and comparison while follower execution lags. (Planned handoff took about 1.2 s across
  processes on live Azure with a 1 s confirmation duration; see the subprocess tests.)
- Storage account throttling limits (not reached by eight databases on one VM).
