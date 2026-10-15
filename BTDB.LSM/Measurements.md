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

## 2. Azure storage screening (harness ready; live run pending)

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

**Live run, still to do.** Run on a same-region Azure VM with a managed identity against a dedicated storage account
(Standard_LRS and Standard_GZRS, optionally Premium block blob):

```bash
BENCH_ACCOUNT=<account> BENCH_SAMPLES=2000 BENCH_TABLE_MIB=1024 dotnet run -c Release --project DBBenchmark -- lsm-storage
```

Record the results here, then choose `BlockSize`, `WalChunkTargetSize`, the readahead window and
`RefreshRetryInterval`. Until then the plan's defaults (32 KiB blocks, 256 KiB WAL chunks) are provisional.
