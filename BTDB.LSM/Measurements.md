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

## 3. Memtable GC cost (2026-10-16)

Test: `BTDB.LSM.Test/Perf/MemtableGcSmokeTest.cs`, Release build, Apple M2 Max, in-memory storage. One run
inserts 1,000,000 random 16-byte keys with 16-byte values into a database with a 64 MiB memtable and a 16 MiB block
cache, in transactions of 1,000 or 10 operations. GC numbers are process-wide; the four cases run one after another.
Two runs gave consistent results; the table shows the range.

| Memtable | Ops per transaction | Inserts/s | GC pause (share of run) | gen2 collections | GC heap at end |
| --- | --- | --- | --- | --- | --- |
| Managed treap (L1) | 1,000 | 319,000–327,000 | 1.03–1.09 s (33–35%) | 18–19 | 288–295 MiB |
| Managed treap (L1) | 10 | 249,000–250,000 | 1.31–1.32 s (33%) | 24 | 251–266 MiB |
| Native B+tree (L2) | 1,000 | 376,000–404,000 | 0.39 s (15–16%) | 36–39 | 90–134 MiB |
| Native B+tree (L2) | 10 | 364,000–373,000 | 0.42–0.44 s (16%) | 44–46 | 87–181 MiB |

Earlier, a single isolated run of the managed memtable with 1,000 operations per transaction measured a 32% GC pause
(937 ms of 2.9 s), 1.1 GB allocated and a 262 MiB heap. That evidence admitted the native memtable (plan, section 7.1).

Findings:

* The native memtable removes long-lived managed objects. GC pause time drops by about 3 times and inserts are 20–50%
  faster. The remaining allocations are short-lived: cursor key and value copies, entry materialization during flush,
  and table building.
* The native arena counts copy-on-write garbage against `MemtableSize`. With random keys, every transaction copies
  each first-touched leaf (about 1.1 KiB) and its branch path, so the arena fills about 3 times faster than the managed
  estimate: 76,000 entries per 64 MiB with 1,000 operations per transaction, and 26,000 with 10. The managed memtable's
  accounting (entry bytes + 96 per entry) underestimated its real heap use by about 4 times. A candidate follow-up, to
  admit only after measuring flush pressure on a real workload, is rebuilding the live tree into a fresh arena when
  garbage dominates; the old arena would be retired through the same snapshot-safe release.
* Larger transactions amortize the copies best. They match the expected use (plan, decision D4).

## 4. End-to-end qualification on Azure (2026-10-16)

Harness: `DBBenchmark lsm-azure` (`DBBenchmark/LSM/LsmAzureBenchmark.cs`) and the `BTDB.LSM.Azure.Test` suite with
`BTDB_AZURE_BLOB_ENDPOINT`. The setup matched section 2: a Standard_D8ads_v5 VM in Sweden Central (Release build,
.NET 10) with a managed identity, and two new accounts in the same region, Standard_LRS (StorageV2) and Premium_LRS
(BlockBlobStorage), with shared keys disabled. The resources were deleted after the run.

**Adapter qualification.** The live adapter suite passed against the Premium account: 79 of 79 tests (conformance,
the shared `KeyValueDBTestBase` suite, and crash recovery with a reader in a second instance), in 71 s.

**Workload.** 2,000,000 keys of 16 bytes, in pseudo-random order, with 100-byte values. Values are mostly zero, so
they compress. Bulk load uses transactions of 10,000 keys, then waits for WAL durability. Other settings: a 32 MiB
memtable, 1,000 samples per latency case, and the database under a prefix.

Two runs were made on each account:

* Run 1 used a 64 MiB block cache and had no readahead.
* Run 2 used the default 256 MiB cache, after readahead was added. The 100-key commit case was shortened to 200
  commits, and the BTree comparison was skipped.

The reader-freshness case measures the delay from a writer's commit until a reader instance's
`WaitForCommitUlongAsync` (10 ms first retry, doubling) returns. After every bulk load and full compaction, the
database was one 49 MB level-1 table plus one small level-0 table.

Values are p50 / p95 / p99 in ms unless noted.

| Case | Premium, run 1 | LRS, run 1 | Premium, run 2 | LRS, run 2 |
| --- | --- | --- | --- | --- |
| Bulk load, inserts/s (WAL durable at the end) | 25,650 | 30,500 | 27,790 | 32,340 |
| Bulk load plus full compaction, s | 80.2 | 68.2 | 74.2 | 64.4 |
| Durable single-key commit | 9.6 / 22.1 / 28.0 | 8.2 / 11.3 / 21.8 | 10.1 / 22.0 / 29.7 | 8.4 / 10.8 / 19.6 |
| Commits of 100 existing random keys, commits/s | 2.9 | 3.5 | 7.1 | 8.4 |
| Reader freshness | 15.8 / 48.6 / 56.6 | 29.5 / 34.3 / 38.7 | 15.5 / 50.3 / 57.1 | 13.3 / 34.0 / 40.3 |
| Open of a reader instance, s | 0.025 | 0.031 | 0.029 | 0.024 |
| Cold point read (`FindExactKey` + value) | 4.4 / 6.1 / 10.3 | 3.3 / 4.5 / 9.0 | 4.5 / 6.2 / 10.5 | 3.8 / 5.7 / 8.7 |
| Cold `FindKeyIndex` | 9.4 / 17.5 / 23.4 | 7.2 / 13.8 / 17.5 | 9.3 / 18.2 / 23.1 | 8.0 / 15.4 / 21.4 |
| Warm point read, µs | (cache too small) | (cache too small) | 2.4 / 3.1 / 4.0 | 2.5 / 3.2 / 3.7 |
| Warm `FindKeyIndex`, µs | (cache too small) | (cache too small) | 107 / 209 / 243 | 104 / 200 / 226 |
| Full key scan, keys/s | 68,900 | 85,300 | 745,600 | 983,300 |

Memory of the LSM writer after bulk load and compaction:

* run 1: 26 MiB managed and 56 MiB native (cache and memtable); working set 424 MiB;
* run 2: 27 MiB managed and 224 MiB native; working set 646–672 MiB.

The working set includes the runtime, the Azure SDK and freed native memory not yet returned to the OS. A freshly
opened reader held 2–10 MiB managed and no native memory before its first read.

Comparison in run 1: `BTreeKeyValueDB` on `AzureBlobFileCollection` with the same keys and values. Its bulk load
includes `CreateKvi` and the upload of the TRL and KVI.

| | Premium | LRS |
| --- | --- | --- |
| Bulk load, inserts/s | 115,900 | 143,900 |
| Native memory allocated during load, MiB | 1,572 | 1,572 |
| Open from blob storage into an empty local cache, s | 1.69 | 1.16 |
| Local file cache, MiB | 273 | 273 |
| Native memory after open, MiB | 58 | 58 |
| Point read, µs (first / second pass) | 3.4 / 0.9 | 3.3 / 1.0 |
| `FindKeyIndex`, µs (first / second pass) | 1.3 / 0.9 | 1.2 / 0.9 |

Findings:

* **Memory and open time meet the goal.** An LSM reader opens in 25–30 ms with a few MiB, independent of database
  size. `BTreeKeyValueDB` needs a full local copy (273 MiB here) and memory proportional to the key count, and its
  open time grows with the database.
* **Cold reads cost one ranged GET.** A cold point read takes 3.3–4.5 ms at p50 and 9–10.5 ms at p99. In this run LRS
  was not slower than Premium, unlike the section 2 screening. With one run per account, that does not support a
  general tier recommendation for reads. A warm point read takes 2.5 µs, about 2.5 times `BTreeKeyValueDB`.
* **Readahead was missing; it is now implemented.** In run 1, a scan paid one synchronous GET per 32 KiB block, about
  70–85k keys/s. Readahead (plan section 12.2) grows a window from 64 KiB to 2 MiB, issued as parallel requests of at
  most 256 KiB. It raised scans to 0.75–0.98 M keys/s with a cold cache. `LsmReadaheadTest` guards it.
* **The final select step was CPU-heavy; it is now fixed.** A warm `FindKeyIndex` took about 105 µs. Select bisected
  only down to two blocks per component and then stepped through up to about 600 entries, pinning the block for each
  step: about 850 cache pins per call. Select now bisects a second time over restart keys inside the remaining blocks,
  so the linear step covers at most about one restart interval. In-memory measurement on 400,000 keys in one table
  (Release build, Apple M2 Max): from 850 to 100 pins and from 122 to 19 µs per call.
* **Updates of uncached existing keys cost a read each.** `CreateOrUpdateKeyValue` must report whether the key
  existed, so the engine has to know it. New keys are mostly rejected by bloom filters, but an existing key whose
  block is not cached costs one synchronous GET. 100-key commits of random existing keys reach 3–8 commits/s, about
  300–800 updates/s, depending on the cache hit rate. This cost is inherent to the `IKeyValueDB` contract.
  Applications that update a cold working set should size `BlockCacheSize` for it. A batched existence prefetch for
  known key sets is a candidate optimization, to be admitted only with an application workload.
* **Bulk load was 4–5 times slower than `BTreeKeyValueDB`** (26–32k against 116–144k inserts/s). Section 5 profiles
  it: bloom filter false positives, not compaction, caused it. The default is now 16 bits per key. Candidates are a larger memtable
  for loads, sublevel-based delaying of level-0 compaction, and the arena rebuild from section 3.
* **Durable commits take 8–10 ms at p50**, about 2.5 ms over the single-PUT floor in section 2. Premium had a p95 of
  22 ms, against 11 ms on LRS. That fits the recommendation to wait for durability only where needed and to group work
  into larger transactions.
* **Reader freshness is dominated by polling.** The p50 of 13–16 ms is a WAL upload plus one or two probes. The tail
  of 34–57 ms comes from the doubling retry interval of `WaitForCommitUlongAsync`. The one LRS p50 of 29.5 ms in run 1
  was a different alignment of the same retry steps. An application that needs a tighter tail can lower
  `RefreshRetryInterval`.

## 5. Random-key bulk load profile (2026-10-16)

Test: `BTDB.LSM.Test/Perf/BulkLoadSmokeTest.cs`, Release build, Apple M2 Max. The workload repeats the section 4 bulk
load: 2,000,000 pseudo-random 16-byte keys, 100-byte values, transactions of 10,000 keys, a 32 MiB memtable and a 64
MiB cache. The run used `BTDB_LSM_BULK_KEYS=2000000` and `BTDB_LSM_BULK_MEMTABLE_MIB=32`.

`LatencyLsmStorage` wraps the in-memory storage with the section 2 Standard LRS p50 costs:

* a create costs 8 ms plus about 80 MB/s;
* a read costs 4 ms plus about 150 MB/s.

`CalcStats` now reports flush and compaction counts, bytes and time, and write stall time by cause.

| Storage | Bloom bits per key | Inserts/s | Synchronous table reads while inserting | Write stall |
| --- | --- | --- | --- | --- |
| In memory | 10 | 435,000–541,000 | 0 | 0.8–1.1 s, sealed memtables |
| Simulated latency | 10 | 25,900–26,000 | 15,357 (188 MiB, 69 s) | none |
| Simulated latency | 16 | 131,900 | 2,619 (115 MiB, 12 s) | none |
| Simulated latency | 20 | 247,000 | 706 (34 MiB, 3 s) | 1.7 s, sealed memtables |

In every run, flushes and compactions were the same: 19 flushes (46 MB) and 4 compactions (98 MB written). With
latency they took 7 s and 4 s on the background worker. WAL uploads (229 MiB in about 1,000 chunks, 11 s summed) ran
in parallel and never stalled the writer.

Findings:

* **The simulation reproduces Azure.** At 10 bits per key it reaches about 26,000 inserts/s, matching the measured
  25,650–32,340 (section 4). The time does not go to compaction or the WAL. The writer spends 69 of its 77 s in
  synchronous table reads: `CreateOrUpdateKeyValue` must know whether a new key exists, and every bloom filter false
  positive in a level-0 or level-1 table costs an uncached block read.
* **Default `BloomBitsPerKey` is now 16** (it was 10, the usual local-disk value). On object storage a false positive
  costs a GET of about 4 ms instead of a local read of about 0.1 ms. At 16 bits the bulk load reaches parity with
  `BTreeKeyValueDB` (132,000 inserts/s against 116,000–144,000), and point lookups of absent keys benefit equally.
  Filters grow from 1.25 to 2 bytes per key.
* **20 bits per key** nearly doubles that again. It is the better choice for insert-heavy databases whose filters
  fit the metadata part of the cache: one eighth of `BlockCacheSize`, so about 2.5 bytes per key times the key count.
  Above that, filter partitions are evicted and re-read, and more bits make it worse. That is why 16 stays the
  default.
* The section 4 hypothesis about level-1 rewrites was wrong for this size: compaction kept up and never stalled the
  writer.
