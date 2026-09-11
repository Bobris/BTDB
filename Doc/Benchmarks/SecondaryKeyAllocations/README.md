# Secondary-key allocation benchmark

This report records the initial per-row stack-buffer optimization. The subsequent implementation
retains buffers across deleted rows; see [the follow-up measurements](Followup.md).
The next stages add [fused string conversion](FusedString.md) and [direct varint copying](IntegerCopy.md).

Measured on 2026-09-11 against base commit `a5318886354318305afed57bd530b2e5f1bb49ea`.
The baseline uses the original `RelationDBManipulator.cs` at that commit; the optimized run
uses the change in this worktree. Both use the identical new benchmark harness.
This tests the current BTDB implementation, not the production 33.6.15 binary.

## Workload and reproduction

```sh
DOTNET_TieredCompilation=0 dotnet run -c Release --project DBBenchmark -- secondary-key --filter '*' --artifacts /tmp/btdb-secondary-results
```

For the baseline, use a separate checkout at the base commit, copy the benchmark file and
its `Program.cs` dispatch into it, and run the same command. Do not replace library files
in a checkout with unrelated work. Run baseline and optimized measurements sequentially.

BenchmarkDotNet 0.14.0, .NET SDK 10.0.401 / runtime 10.0.12, macOS 26.6.2,
Apple M2 Max, Arm64. Tiered compilation is disabled identically for both runs to avoid
compilation transitions during short iterations. One launch, three warmup iterations,
ten measurement iterations, sixteen invocations per iteration, unroll factor one.

One operation means a complete 100- or 1,000-row batch, including transaction creation,
relation lookup, deletion/update, and rollback. Population and database disposal are excluded.
A BTreeKeyValueDB uses InMemoryFileCollection, no compression, no compactor and no durable flush.
This exercises the BTree and transaction-log implementation without disk latency.

The schema has three ulong primary-key fields (CompanyId, BatchId, MessageId), a Recipient
string stored via InKeyValue, and one CustomField secondary index including CompanyId.
CustomField contains deterministic unique ASCII strings of exactly 24 or 8,192 characters.
The encoded key is longer because of prefixes and primary-key fields. Update changes every
CustomField from an 'a' prefix to a 'b' prefix; update objects are prepared outside measurement.
Production batch sizes and field-length distributions are unknown, so these are synthetic assumptions.

Each invocation rolls back to the same committed populated batch. RemoveBatch checks the removed
count on every invocation. A fresh populated database is created before every iteration,
bounding log growth from rolled-back writes. Initial exploratory runs without this reset
had severe drift and are excluded. Short iterations trigger BenchmarkDotNet's minimum-iteration-time
warning; timing is indicative, whereas managed allocation differences are the primary result.
No tests or other benchmarks were intentionally run concurrently with the final measurements.
Ordinary desktop applications remained running.

## Buffer lifetime and scope

The serialized-key helper now receives a writer owned by its caller, matching the existing
object/computed-key helper. Each caller initializes a 4 KiB stack buffer outside the index loop.
Update uses independent old/new buffers, keeping both keys valid through comparison, removal
and insertion. Reset happens only before the next index. Cursor operations consume/copy the
key synchronously; no span escapes the caller. Computed-field loading and schema-version
merging remain unchanged.

MemWriter.Resize retains its pinned heap fallback for oversized keys. Reset retains an expanded
buffer for subsequent indexes of the same row. Buffers are not cached on the relation or shared
between transactions, and are not retained between deleted rows. This avoids introducing shared
mutable state, long-lived large buffers, or new ownership/disposal rules. Stack use increases by
4 KiB for serialized removal/object-update and 8 KiB for serialized UpdateById; computed paths
already used these buffers. No public API or persisted format changes.

The regression tests cover multiple indexes, short/oversized keys, growing and shrinking values,
unchanged updates, object Update and UpdateById, computed fields, batch isolation, repeated deletion,
and persisted indexes after reopening. UpdateById supplies the Recipient InKeyValue argument too.

## Limits

This measures managed allocations, elapsed operation time and collection counts, not GC pause duration,
native BTree allocation totals or production latency. The sampled production CPU stack motivates the
change but cannot establish a GC pause or explain the entire slow replica event. The microbenchmark
does not prove that CME-5877 is resolved; that requires production profiling after an independently
approved rollout. No Messenger chunking, operational changes or package publication are included.

## Results

Times are microseconds per complete batch; allocation units are KiB (BDN labels them KB).
First comparison: baseline then optimized. The raw reports include standard deviations and error intervals.

| Operation | Rows | Field chars | Before us | After us | Before KiB | After KiB | Allocation reduction | Gen0 / 1000 ops, both |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| RemoveBatch | 100 | 24 | 68.35 | 62.32 | 28.28 | 13.43 | 52.51% | 0.0000 |
| UpdateBatch | 100 | 24 | 158.26 | 151.02 | 35.87 | 21.02 | 41.40% | 0.0000 |
| RemoveBatch | 100 | 8192 | 863.73 | 815.45 | 4842.04 | 4827.19 | 0.31% | 187.5000 |
| UpdateBatch | 100 | 8192 | 1847.56 | 1828.31 | 10476.46 | 10461.62 | 0.14% | 250.0000 |
| RemoveBatch | 1000 | 24 | 832.49 | 721.82 | 314.57 | 166.13 | 47.19% | 0.0000 |
| UpdateBatch | 1000 | 24 | 1919.42 | 1802.41 | 418.10 | 269.67 | 35.50% | 0.0000 |
| RemoveBatch | 1000 | 8192 | 12832.61 | 10816.57 | 48631.95 | 48483.51 | 0.31% | 2000.0000 |
| UpdateBatch | 1000 | 8192 | 26008.46 | 22290.08 | 106916.88 | 106768.44 | 0.14% | 3187.5000 |

The writer change saves one 128-byte payload array (152 bytes including array overhead) per row
in both measured operations. For short keys this eliminates the key writer heap allocation.
For oversized keys it eliminates the initial small allocation but the large fallback remains.
Gen0 counts were unchanged; no Gen1/Gen2 collections were reported. Zero collections in a short
measurement does not imply that its allocations have no future collection cost.

### Reverse-order confirmation

A second independent run measured optimized first, then baseline, under the same settings.
Allocations and collection counts matched the first comparison (apart from rounding).

| Operation | Rows | Field chars | Before us | After us | Time reduction |
|---|---:|---:|---:|---:|---:|
| RemoveBatch | 100 | 24 | 71.22 | 62.79 | 11.8% |
| UpdateBatch | 100 | 24 | 163.83 | 154.20 | 5.9% |
| RemoveBatch | 100 | 8192 | 788.65 | 784.79 | 0.5% |
| UpdateBatch | 100 | 8192 | 1810.01 | 1791.75 | 1.0% |
| RemoveBatch | 1000 | 24 | 814.50 | 726.91 | 10.8% |
| UpdateBatch | 1000 | 24 | 1942.04 | 1800.38 | 7.3% |
| RemoveBatch | 1000 | 8192 | 12613.16 | 10926.72 | 13.4% |
| UpdateBatch | 1000 | 8192 | 25490.95 | 21957.37 | 13.9% |

Both runs show lower mean times for the optimized version. Small differences with overlapping
error intervals should not be treated as established speedups. The clearest result is the
repeatable managed allocation reduction; timing cannot be extrapolated to production batch latency.

## Validation

- `dotnet test BTDB.SourceGenerator.Test/BTDB.SourceGenerator.Tests.csproj`: 190 passed.
- `dotnet test BTDBTest/BTDBTest.csproj --filter FullyQualifiedName~ObjectDbSecondaryKeyBufferTest`:
  all 6 new regression cases passed on both the original and optimized implementations.
- `dotnet test BTDB.sln`: 1,739 passed, 9 existing skips, no failures
  (BTDBTest: 1,527 passed / 9 skipped; SourceGenerator: 190 passed; AzureStorage: 22 passed).
- `dotnet test -c Release BTDBTest/BTDBTest.csproj --filter FullyQualifiedName~ObjectDbSecondaryKeyBufferTest`:
  final regression verification after the report and changelog edits.
- `git diff --check`: clean.

Restore reports the existing NU1902 advisory for Microsoft.Build.Tasks.Git 8.0.0; it does not
prevent the build or tests. CHANGELOG.md includes the allocation optimization under unreleased.
