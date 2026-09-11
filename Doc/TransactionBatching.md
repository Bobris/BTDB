# BTree transaction batching

`BTreeKeyValueDB` supports opt-in reuse of its writable BTree across consecutive writing transactions:

```csharp
try
{
    foreach (var item in items)
    {
        using var transaction = await keyValueDb.StartWritingTransaction(inBatch: true);
        // Apply this item's changes, including CommitUlong and Ulong metadata.
        transaction.Commit();
    }
}
finally
{
    keyValueDb.FinishTransactionBatchAfterCurrentTransaction();
}
```

`inBatch` defaults to `false`. Passing `true` starts a new batch or joins the pending batch. Each queued writer retains
its own requested mode; writers are granted in FIFO order. There is no global enable/disable switch.

`StartWritingTransaction()` (or `inBatch: false`) finishes a pending batch before starting a normal writing transaction.
If a writer is active, the request waits in the writer queue. When granted, it publishes the pending batch and receives
its own independently committed BTree. A later request with `inBatch: true` starts a new batch.

`FinishTransactionBatchAfterCurrentTransaction()` is a nonblocking, one-shot request. With no active writer it publishes
the pending batch immediately. With an active batch writer it waits logically for that writer to commit or roll back,
then ends the batch **before granting the next queued writer**, even if that next writer requested `inBatch: true`.
It never commits the active transaction on the caller's behalf. Repeated finish requests are harmless. If no batch
exists, including during a normal writing transaction, finish does nothing and does not affect future batches.
Disposing the database also publishes a pending batch, subject to the normal requirement to finish/dispose active
transactions first.

These methods are exposed through both `IKeyValueDB` and `IObjectDB`. `ObjectDB` forwards
`StartWritingTransaction(inBatch: true)` and `FinishTransactionBatchAfterCurrentTransaction()` to its underlying database.
`InMemoryKeyValueDB` ignores the `inBatch` optimization hint and uses ordinary transactions; finishing a batch is a no-op.
Read-only databases continue to reject writing transactions.
The added optional parameter changes the interface signature, so custom implementations and wrappers must update it
and forward the mode and finish request as appropriate.

Each transaction still writes its own ordinary TRL start, mutations, metadata deltas and commit. A failed transaction
keeps its mutations and rollback marker in TRL. No file-format change is involved. `DurableTransactions` still applies
to **each transaction**; BTree batching does not defer its hard flush. Finishing a batch publishes the BTree without
adding a TRL command and does not add a durability guarantee beyond the normal commit policy.

The optimization avoids creating a new root and copying modified BTree paths after every commit. Successive
`StartWritingTransaction(inBatch: true)` calls reuse the mutable tree. On rollback, BTDB discards that tree and replays
the successful commits since the last published root, stopping before the failed transaction. Replay works across TRL
file boundaries and restores values, erases and metadata without rerunning application code.

Replay uses the reusable `BTDB.StreamLayer.FileCollectionFileReader` directly over `IFileCollectionFile.RandomRead`.
It starts at an absolute offset, caps reads at a captured exclusive end offset, and retains one buffer across TRL files.
`Restart(file, startOffset, endOffset)` selects another file or range; discard the previous `MemReader` and construct a
new one with the same controller after restart. The reader does not own the files. Callers must retain each file and
ensure its selected byte prefix stays immutable while reading.

Reader snapshot semantics stay unchanged. Opening a read-only or ordinary transaction publishes pending commits. If a
writer is active, BTDB reconstructs the committed prefix from TRL for the reader, leaving that writer's private changes
invisible. Existing readers retain their snapshots. Consequently, frequent reads and rollbacks reduce the benefit.
`StartTransaction()` requests a snapshot and introduces a publication boundary; it does not opt into batching.

`ReferenceAndGetLastCommitted()` only references the last published BTree; it never publishes or replays a pending
batch. Statistics and checkpoint creation use that published state too. A checkpoint may therefore precede successful
commits still in the batch; recovery replays those commits from the retained TRL.

Compaction requests a normal writing transaction for value remapping, which ends any pending batch before unlogged
compactor changes begin. Its durability barrier serializes with the writer and hard-flushes the current TRL without an
empty transaction or temporary-end marker. The existing `NextCommitTemporaryCloseTransactionLog()` API and decoding
of old temporary-end markers remain supported.

## Benchmark

Run:

```sh
dotnet run -c Release --project DBBenchmark/DBBenchmark.csproj -- transaction-batching --filter '*' --iterationCount 15 --warmupCount 5
```

`TransactionBatchingBenchmark` compares 500,000 identical updates to a prepopulated 100,000-key BTree:

- `WithoutBatching`: one transaction per update.
- `BTreeBatching`: the same transactions, with BTree publication every 100 or 1,000 transactions.
- `ApplicationBatching`: merge 100 or 1,000 updates into one application transaction.

Keys follow the same deterministic permutation. Database creation and population are outside the measured iteration.
Reported time and managed allocations are **per update**. The benchmark uses uncompressed, in-memory file storage and
`DurableTransactions = false` to measure BTree and transaction overhead without filesystem flush latency. Native BTree
allocations are not included in BenchmarkDotNet's managed allocation column. Application batching also saves TRL
framing, flushes and transaction objects, so it is expected to remain faster but has different rollback boundaries.

### Measured result with the per-writer API (2026-09-10)

Apple M2 Max, macOS 26.6.2, .NET 10.0.11 Arm64, BenchmarkDotNet 0.14.0; 5 warmup and 15 measured iterations.
Times are mean nanoseconds per update, with the half-width of the 99.9% confidence interval:

| Method | Batch size | Time per update | Managed allocation per update |
| --- | ---: | ---: | ---: |
| Without batching | 100 | 1,367.1 ± 23.39 ns | 529 B |
| BTree batching | 100 | 1,026.7 ± 17.09 ns | 435 B |
| Application batching | 100 | 620.6 ± 19.04 ns | 44 B |
| Without batching | 1,000 | 1,325.0 ± 178.68 ns | 529 B |
| BTree batching | 1,000 | 863.9 ± 19.38 ns | 435 B |
| Application batching | 1,000 | 464.6 ± 20.39 ns | 40 B |

Ratios of measured means give approximately **1.33×** and **1.53×** higher throughput for BTree batching, respectively,
while preserving individual transaction boundaries. Application batching reaches approximately **2.20×** and **2.85×**.
The baseline for batch size 1,000 varied more in this run, as reflected in its confidence interval. These are
CPU/in-memory results; per-transaction durable disk flushes can dominate and reduce the relative gain from BTree batching.
