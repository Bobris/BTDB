# Core preparation for replication

These are opt-in BTDB building blocks. They do not implement leader election, Blob publication, follower comparison,
volatile shutdown files or replicated compaction. Those remain in the replication layer/design; setting these options
alone does not make a database replicated or enforce leader authority.

```csharp
using var kv = new BTreeKeyValueDB(new KeyValueDBOptions
{
    FileCollection = files,
    RequireExplicitTransactions = true,
    UseOddTransactionLogIds = true,
    CompactorScheduler = null
});
using var db = new ObjectDB();
db.Open(kv, false, new DBOptions { DeferNewRelationMetadata = true });

using var transaction = await db.StartWritingTransaction(eventId: 123);
// Apply this input's mutations. CommitUlong is already 123 in this transaction.
transaction.Commit();
```

`StartWritingTransaction(ulong eventId, bool inBatch = false)` exists on KeyValueDB and ObjectDB interfaces and their
BTree/ObjectDB implementations. It awaits writer serialization first, then sets CommitUlong automatically. Disposing
without commit rolls that cursor back with the mutations. The existing boolean overload remains unchanged and does
not set an input cursor. Core performs no event ordering validation or leader wait: the replication adapter must admit
non-application work before calling the existing writer API. Commit remains synchronous and local.

`RequireExplicitTransactions` rejects `StartTransaction()` before it publishes a pending virtual batch or acquires a
root. Use `StartReadOnlyTransaction()` or the asynchronous writer APIs. ObjectDB's startup metadata and singleton-ID
lookup now use explicitly read-only transactions. Both new KeyValueDB options default to false.

`UseOddTransactionLogIds` assigns fresh odd IDs to TRLs and fresh even IDs to every other file type (including
KVI, PVL and chunk-storage files), without allocating intermediate dummy files. The policy is enforced by the
database file collection, including calls made by sub-databases. A valid
legacy even tail is closed before the first new transaction bytes and linked to a fresh odd TRL. Existing file IDs and
value references are preserved; size-based rotation may still happen inside transactions. InMemoryFileCollection,
OnDiskFileCollection and OnDiskMemoryMappedFileCollection implement the new `AddFile(hint, FileIdParity)` overload.
Custom collections must implement it before opting into parity-constrained allocation; its default implementation rejects an
unsupported constrained allocation rather than silently using the wrong parity. This is not a distributed file allocator.

`DBOptions.DeferNewRelationMetadata` defaults to false. When enabled, a new relation name/version can be registered in
memory by a read-only transaction; no hidden writer is opened. Its metadata is written inside the first transaction
that writes relation data (insert/upsert/update), before data serialization. A read or an empty commit does not persist
it. Persistence is checked against the writing transaction's view, so rollback followed by retry retains the metadata.
Object/table metadata already uses its normal data-write persistence path.

Existing relation-version changes and secondary-index rebuilds are deliberately not deferred. Prepare these in an
explicit non-application writer; trying to initialize a write-requiring upgrade from a read-only transaction throws
BTDBTransactionRetryException rather than blocking through a hidden `.Result` writer. The future replication layer
must obtain leadership before that writer starts. Custom relation creation callbacks that write also require explicit
writer admission; the option does not silently defer or rerun application callbacks.

Tests in ReplicationPreparationTest cover queued writer cursor assignment, rollback with and without virtual batching,
legacy even-tail rotation and cross-file replay, persistent/memory-mapped odd/even allocation, concurrent mixed allocation,
read-only relation registration, metadata rollback/reopen, and explicit secondary-index upgrade/reopen.

`KeyValueDBOptions.TransactionLogSizeStrategy` optionally supplies `ITransactionLogSizeStrategy.GetLimits(uint fileId)`.
It returns `TransactionLogSizeLimits(SoftLimit, HardLimit)` in bytes, derived **only** from that TRL's numeric ID.
The soft limit rotates before starting the next transaction once the current file reaches it. An ongoing transaction
may exceed the soft limit. The hard limit bounds the complete file, including its header and end markers: commands,
metadata deltas and commit markers rotate to a successor when needed, even inside a transaction. An individual command
cannot be split; a command that cannot fit the successor's hard limit throws before its bytes are written.

Limits must satisfy `1024 <= SoftLimit <= HardLimit < uint.MaxValue`; the hard limit is below 4 GiB because native
root/value offsets are 32-bit. The writer reserves space for rollback and end markers. The strategy is evaluated
for each newly allocated TRL and recomputed from the active file's ID on reopen. Opening alone does not rotate a file
that has reached its soft limit. FileSplitSize remains the fallback when no strategy is supplied; the existing
standalone rotation behavior remains unchanged in that case. With a strategy, AutoAdjustFileSize is rejected and
MaxTrLogFileSize reports the current soft limit and cannot be assigned manually.

For example, tests can inject a tiny fixed policy:

```csharp
sealed class TestLogSizeStrategy : ITransactionLogSizeStrategy
{
    public TransactionLogSizeLimits GetLimits(uint transactionLogFileId) => new(1024, 4096);
}
```

All nodes and application versions must retain the same ID-to-limits mapping for the database's lifetime.
BTDB validates returned limits but cannot prove purity or detect a changed mapping across deployments. No production
curve is prescribed by this API. A strategy must not depend on time, local disk usage, compaction, mutable counters,
or other process state. TransactionLogSizeStrategyTest covers deterministic rotation, batching, reopen, soft-limit
boundaries, hard-limit cross-file commit/rollback and metadata replay, oversized commands, and invalid policies.
