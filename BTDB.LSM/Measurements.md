# BTDB.LSM measurements

Phase L0 evidence for [ImplementationPlan.md](ImplementationPlan.md). Each section records what was measured, how,
and which design decision it supports.

## 1. Counted LSM prototype (2026-10-15)

Code: `BTDB.LSM/Counting` (`CountedComponent`, `CountedStack`). Tests: `BTDB.LSM.Test/Counting/CountedStackOracleTest.cs`.

The prototype implements the model of sections 6.1–6.5:

* points carrying `existsBelow`;
* range tombstone fragments with `coveredBelow`;
* per-restart and per-block prefix sums;
* index-only rank bounds;
* rank, select and count over a mutable memtable plus a stack of immutable components;
* flush, and compaction of any contiguous run of components.

It keeps everything in managed memory. It has no values, persistence or concurrency. Costs are counted as index reads
(pinned or cached index lookups) and block reads (data block accesses that may miss the cache).

### Correctness against an oracle

`CountedStackOracleTest` runs seeded random sequences against a Fenwick-tree oracle over a fixed key space:

* creates and updates, deletes of existing and missing keys, and inclusive range erases from 0 keys to a quarter of
  the key space;
* random flushes and random compactions of contiguous runs, with a cap of 10 components.

After every operation it checks:

* the returned created, erased and count values;
* `Count`;
* a random `Exists`;
* a random `Rank`, either at a key or just after a key, and checks that `RankBounds` contains the exact rank;
* a random `Select`.

Periodically it compares full iteration and the rank of every live key.

| Case | Key space | Operations | Block / restart | Memtable | Point-delete limit | Result |
| --- | --- | --- | --- | --- | --- | --- |
| seed 1 | 512 | 20,000 | 2 / 1 | 16 | 0 (always tombstones) | pass |
| seed 2 | 512 | 20,000 | 4 / 2 | 1 | 4 | pass |
| seed 3 | 4,096 | 50,000 | 16 / 4 | 64 | 64 | pass |
| seed 4 | 4,096 | 50,000 | 64 / 8 | 8 | 10⁶ (never tombstones) | pass |
| seed 5 | 64 | 20,000 | 2 / 2 | 4 | 0 | pass |
| seed 6 | 100,000 | 50,000 | 16 / 4 | 256 | 16 | pass |
| seed 42 | 20,000 | **1,000,000** | 16 / 4 | 64 | 64 | pass (about 80 s in a Debug build) |

The L0 exit criterion of 10⁶ random operations is met. The live population settles at 40–50% of the key space, so
range erases, tombstone fragments and compaction all act on dense data.

**Mutation check.** Each of these four deliberate breakages made at least three of the six theory cases fail:

* not moving the rank key to a straddled fragment's start;
* not subtracting `coveredBefore`;
* taking `existsBelow` as always true during compaction;
* dropping fragments with `coveredBelow = 1`.

The tests therefore cover the tombstone and compaction arithmetic, not only the plain point path.

### Select cost

`SelectCostIsLogarithmic` sets up this shape:

* the bottom component holds about 500,000 keys;
* five newer components each hold about a tenth of the next older one, with updates, deletes and one range erase
  per level;
* there are 6 components in total, with 64-entry blocks and restart interval 16.

Each figure is an average over 2,000 random selects or ranks:

| Metric | Without index bounds | With index-only bounds (current) |
| --- | --- | --- |
| Pivots per select | 11.9 | 11.9 |
| Exact pivot ranks per select | 11.9 | 0.5 |
| Rank calls per select | 11.9 | 1.5 |
| Data block reads per select | 477 (per-key counting) | 15.2 |
| Final iteration steps per select | 66.9 | 66.9 |
| Data block reads per rank | 5.8 | 5.8 |

The 477 in the first column came from an iteration that counted a block read for every key checked. The current
merging iterator counts a block only when it is entered. The two columns therefore differ in two ways, and the
reduction in exact ranks (11.9 to 0.5) is the part that comes from index bounds.

Conclusions:

* Rank costs about one data block per component, as section 6.4 predicts.
* With the running-delta extremes stored per block in the index, bisection decides about 96% of pivots from index
  data alone. A select then costs about 2.5 data blocks per component: one exact rank, plus the final iteration over
  at most `SelectFinalBlocks` blocks per component.
* The index entry therefore gains two small integers, the minimum and maximum running delta inside the block. Section
  9 of the plan is updated accordingly.
* A later improvement can reduce the final iteration (about 67 steps) by bisecting on restart points inside the
  bracketing blocks. It is not needed for correctness.

## 2. Azure storage screening (2026-10-15)

Harness: `DBBenchmark lsm-storage` (`DBBenchmark/LSM/LsmStorageScreening.cs`), based on the event log screening. It
uses raw REST with no SDK retries, closed loops and warm-up samples. It writes one JSON line per case plus
`summary.json` and a per-case sample file.

| Case | Decision it informs |
| --- | --- |
| `table-upload-{N}mib-4mib-blocks-parallel-8` | Flush and compaction upload throughput (section 9). |
| `range-get-async-{4k…4096k}` | `BlockSize`, readahead window sizes, cost of a cold point read (sections 9, 12; R1). |
| `range-get-sync-{32k,256k}` | Synchronous cursor reads through `HttpClient.Send` versus async (section 5; R2). |
| `range-get-async-32k-parallel-{4,16,64}` | Parallel component seeks and per-account request rate (section 12.2). |
| `range-get-sync-32k-threadpool-{16,64,256}` | Thread-pool growth and scheduling delay while cursors block in sync I/O (R2). |
| `sequential-read-4mib-parallel-{4,8,16}` | Compaction input and reopen throughput (section 13). |
| `probe-404` | Cost of probing the next WAL chunk or manifest edit in `RefreshAsync` (section 15). |
| `get-small-{1k,64k,256k}` | Reading WAL chunks and manifest edits. |
| `get-properties`, `list-{10,100,1000}` | Open and recovery cost; listing versus probing. |
| `putblob-create-{64k,256k,1024k}` | `WalChunkTargetSize` and the durable commit floor (section 8). |
| `putblob-create-256k-parallel-4` | Multi-chunk large commits (section 7.5). |
| `lease-renew-15s` | Writer lease renewal (section 14). |

**Smoke run.** All cases completed without errors against local Azurite (`BENCH_CONNECTION_STRING`, 40 samples, 16
MiB table). Azurite latencies are not representative and are not recorded.

**Live run.** The run took place on a Standard_D8ads_v5 VM (8 vCPU, Ubuntu 24.04, .NET 10.0.402, Release build) in
Sweden Central. It used a managed identity against three new accounts in the same region:

* Standard_LRS and Standard_GZRS (StorageV2);
* Premium_LRS (BlockBlobStorage).

Every account had shared keys disabled. Each case took 2,000 closed-loop samples after warm-up. Range cases read
random 4 KiB-aligned offsets of a freshly uploaded 1 GiB block blob. The run was reproduced with:

```bash
BENCH_ACCOUNT=<account> BENCH_SAMPLES=2000 BENCH_TABLE_MIB=1024 dotnet DBBenchmark.dll lsm-storage
```

The resources were deleted after the run. Values are latency p50 / p95 / p99 in ms, unless a column says otherwise.

### Ranged GET latency by size

| Read size | Standard_LRS | Standard_GZRS | Premium_LRS |
| --- | --- | --- | --- |
| 4 KiB | 14.5 / 45.6 / 79.0 | 8.7 / 39.7 / 65.1 | 4.8 / 6.2 / 11.2 |
| 16 KiB | 9.4 / 41.2 / 78.3 | 10.0 / 39.8 / 69.1 | 4.9 / 6.5 / 11.2 |
| 32 KiB | 8.0 / 38.9 / 69.5 | 9.4 / 39.0 / 75.0 | 4.9 / 6.5 / 10.9 |
| 64 KiB | 10.7 / 43.2 / 80.2 | 12.5 / 40.9 / 73.6 | 5.3 / 7.5 / 11.5 |
| 128 KiB | 18.2 / 47.4 / 92.0 | 18.8 / 45.4 / 71.3 | 5.7 / 8.2 / 12.3 |
| 256 KiB | 21.6 / 52.2 / 101 | 22.7 / 53.5 / 90.4 | 6.8 / 9.2 / 14.8 |
| 1 MiB | 35.8 / 79.8 / 114 | 32.0 / 73.4 / 106 | 11.7 / 52.3 / 54.4 |
| 4 MiB | 85.0 / 127 / 177 | 81.0 / 121 / 160 | 61.8 / 73.0 / 83.0 |

One 16 KiB LRS sample took 10 s. The next largest sample in the whole run was 706 ms, a GZRS group of parallel creates.

### Parallel and synchronous reads (32 KiB)

| Case | Standard_LRS | Standard_GZRS | Premium_LRS |
| --- | --- | --- | --- |
| async, 4 in flight | 7.2 / 40.3 / 63.6; 230 req/s | 8.8 / 35.6 / 58.3; 250 req/s | 5.0 / 7.0 / 11.3; 657 req/s |
| async, 16 in flight | 6.8 / 32.2 / 60.9; 963 req/s | 8.5 / 28.8 / 52.3; 944 req/s | 5.0 / 7.1 / 12.1; 2,250 req/s |
| async, 64 in flight | 6.7 / 22.7 / 46.5; 2,490 req/s | 8.3 / 27.4 / 70.8; 2,069 req/s | 5.2 / 7.0 / 10.6; 4,815 req/s |
| sync (`HttpClient.Send`), serial, 32 KiB | 17.5 / 42.1 / 66.9 | 15.8 / 40.9 / 67.2 | 5.0 / 6.6 / 10.6 |
| sync, serial, 256 KiB | 24.9 / 52.5 / 78.3 | 23.6 / 50.7 / 88.8 | 6.7 / 9.2 / 14.9 |
| sync on thread pool, 16 work items | 1,390 req/s | 1,081 req/s | 2,599 req/s |
| sync on thread pool, 64 work items | 403 req/s | 401 req/s | 438 req/s |
| sync on thread pool, 256 work items | 38 req/s | 37 req/s | 38 req/s |

In the thread-pool cases the pool grew to 21, 66 and 258 threads. The aggregate rate collapses because the pool
injects threads slowly while all of them block in I/O. Per-request latency stays normal, and the short-lived probe
work item showed no measurable queueing delay, so the probe did not capture the starvation. The serial sync p50 on
Standard is about twice the async p50, but Premium shows no difference, so a sync-path penalty is not established.

### Throughput, small objects, metadata, writes

| Case | Standard_LRS | Standard_GZRS | Premium_LRS |
| --- | --- | --- | --- |
| Upload 1 GiB, 4 MiB blocks, 8 parallel | 545 MiB/s | 450 MiB/s | 661 MiB/s |
| Sequential read, 4 MiB ranges, 4 / 8 / 16 parallel | 231 / 488 / 938 MiB/s | 238 / 466 / 890 MiB/s | 438 / 848 / 1,592 MiB/s |
| 404 probe | 3.4 / 4.3 / 7.6 | 4.9 / 6.2 / 10.7 | 2.5 / 3.6 / 8.3 |
| GET whole 1 KiB / 64 KiB / 256 KiB object (p50) | 3.5 / 4.0 / 5.9 | 5.0 / 5.4 / 7.4 | 2.7 / 3.1 / 4.8 |
| Get Properties | 3.1 / 3.8 / 6.8 | 4.5 / 6.3 / 9.9 | 2.3 / 2.7 / 6.8 |
| List 10 / 100 / 1,000 objects (p50) | 45.1 / 9.1 / 94.4 | 5.9 / 11.5 / 65.3 | 4.0 / 6.5 / 77.8 |
| Create-if-absent 64 KiB | 7.5 / 11.0 / 17.4 | 9.7 / 15.9 / 28.4 | 5.6 / 19.4 / 25.7 |
| Create-if-absent 256 KiB | 13.8 / 33.7 / 40.0 | 20.0 / 39.4 / 54.3 | 9.8 / 13.6 / 17.3 |
| Create-if-absent 1 MiB | 21.4 / 42.7 / 70.5 | 30.6 / 46.5 / 66.2 | 17.9 / 23.7 / 32.6 |
| 4 × 256 KiB created in parallel (one sample per group) | 16.6 / 41.8 / 64.0 | 23.4 / 46.7 / 67.1 | 10.9 / 17.0 / 21.2 |
| Lease renew | 4.0 / 5.0 / 5.9 | 5.1 / 6.5 / 8.8 | 2.7 / 3.1 / 7.4 |

### Decisions from the screening

* **`BlockSize` = 32 KiB (confirmed).** On every tier, reads up to 64 KiB cost the same as 4 KiB. From 128 KiB on,
  Standard pays about 8 ms more at p50. 64 KiB stays a reasonable option for scan-heavy data.
* **Readahead.** Grow from 64 KiB to 256 KiB per request and reach larger windows by issuing several requests in
  parallel, not by making one request bigger. Parallel requests keep their per-request latency up to at least 64 in
  flight, and a single 1 MiB request costs 2–3 times a 256 KiB one. Compaction input uses 4 MiB ranges, 16 in
  parallel (about 0.9 GB/s on Standard).
* **Cold-read latency is the dominant risk on Standard tiers (R1).** A miss costs about 8–10 ms at p50, but about
  40 ms at p95 and 65–80 ms at p99. A seek that misses in several components waits for the slowest of its parallel
  GETs, so its typical latency moves toward the single-request p90. Premium block blob delivers about 5 ms at p50 and
  11 ms at p99. Recommend Premium for latency-sensitive databases. Keep hedged reads (reissue a GET still pending
  after about the p90 latency) as a candidate optimization for Standard, to be admitted only with an end-to-end
  measurement.
* **Synchronous reads on the thread pool (R2) are confirmed as a real problem.** Sync reads from 64 or more concurrent
  thread-pool work items collapse throughput until the pool grows. The engine must:
  * bound concurrent blocking storage reads;
  * issue the per-component GETs of one seek asynchronously and wait once, so one blocked thread covers all of them;
  * document `ThreadPool.SetMinThreads` for hosts with many concurrent cursors.

  An async cursor path is out of scope for v1.
* **WAL.**
  * Keep `WalChunkTargetSize` = 256 KiB with up to 4 chunk uploads in flight. Four parallel 256 KiB creates take
    16.6 ms (LRS) against 21.4 ms for one 1 MiB create.
  * Small commits upload small chunks: the durable floor is about 7.5 ms p50 and 17 ms p99 on LRS for 64 KiB, and
    about 5.6 ms p50 on Premium.
  * The 20 ms `WalFlushInterval` default stays.
* **Refresh.** A 404 probe costs 2.5–5 ms p50 and at most 11 ms p99, so `RefreshAsync` on an idle database costs two
  cheap GETs. The 50 ms `RefreshRetryInterval` default stays.
* **Probe, don't list.** Listing 1,000 objects takes 65–95 ms, and small listings have outliers of 45 ms. The engine
  lists only at open and recovery and uses name probes afterwards.
