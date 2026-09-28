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

## Not covered

- Warm-restore validation time for very large caches after a reboot (without page cache).
- TLS and a real network between VMs on the HTTP peer path (loopback HTTP is measured above).
- Multi-database contention and comparison while follower execution lags. (Planned handoff took about 1.2 s across
  processes on live Azure with a 1 s confirmation duration; see the subprocess tests.)
- Publication throughput from more than one database at once, and storage account throttling limits.
