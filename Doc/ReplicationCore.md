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
db.Open(kv, false, new DBOptions());

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

New relation names/versions are always registered in
memory by a read-only transaction; no hidden writer is opened. Its metadata is written inside the first transaction
that writes relation data (insert/upsert/update), before data serialization, or allocates an ID with `AllocateId()`. A read or an empty commit does not persist
it. A shared pending flag is reset by a rollback action, so retries persist metadata without repeated lookups.
Object/table metadata already uses its normal data-write persistence path.

Existing relation-version changes and secondary-index rebuilds are deliberately not deferred. Prepare these in an
explicit non-application writer; trying to initialize a write-requiring upgrade from a read-only transaction throws
BTDBTransactionRetryException rather than blocking through a hidden `.Result` writer. The future replication layer
must obtain leadership before that writer starts. Custom relation creation callbacks that write also require explicit
writer admission; metadata deferral does not silently defer or rerun application callbacks.

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
standalone rotation behavior remains unchanged in that case. With a strategy, MaxTrLogFileSize reports the current
TRL soft limit and cannot be assigned manually. PVLs and chunk storage use FileSplitSize independently, and
AutoAdjustFileSize may still adjust that local file-size target without changing the strategy's TRL limits.

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

## Local TRL positions

`KeyValueDBOptions.TransactionLogCapture` accepts a parameterless `TransactionLogCapture`. It stores only two
`TransactionLogPosition(FileId, Offset)` values: `Completed`, the latest fully written commit or rollback, and
`Acknowledged`, the prefix consumed by publication or comparison. No transaction queue, sequence number, event ID,
classification, index file, wakeup or disposal protocol is needed. Memory usage is independent of backlog size.

Snapshot `Completed` before starting remote work. Later writes can advance it while that fixed prefix is being read.
Call `Acknowledge(snapshot)` only after successful publication or verification. Acknowledgement must advance
monotonically and cannot exceed `Completed`; the caller must use a known complete transaction boundary.
The compactor marks TRLs from the acknowledged file onward as used. Initially it conservatively retains existing
TRLs, or the first newly created TRL. Acknowledging an older in-flight snapshot never releases newer local work.
An active transaction never advances `Completed`; commits and rollbacks do, including virtual-batch transactions.
The ordinary native file chain supplies intermediate files and lengths; capture need not store them again.

## Binary TRL comparison

Compare corresponding TRL byte ranges directly in bounded chunks. Capture already supplies the latest complete local
transaction boundary; publication does not decode keys, values or commands a second time. File identity and
range boundaries come from the replication session and its positions. Matching bytes advance confirmation;
a mismatch rejects the local history. No semantic normalization or decoded-command comparison is required.
Database startup and virtual-batch replay decode commands directly in their replay loop, without a command object
or a separate decoder. Follower integration is pending.

## Startup secondary-index reconciliation

In replicated ObjectDB use, secondary-index reconciliation is the only non-application transaction and occurs at
most once, as the first writing transaction after opening ObjectDB. Before starting application events, the startup
coordinator waits for leadership, then uses the existing `ObjectDB.StartWritingTransaction()` to initialize all
required relations and commits with unchanged CommitUlong. Ordinary relation metadata remains deferred.
There is no generic core authority gate, queue admission flag or per-mutation authority check. Startup authority
and cancellation belong to the coordinator; its integration remains pending. Failed startup must not start the event loop.

`BTreeKeyValueDB.StartWritingTransaction(inBatch, cancellationToken)` still supports cancelling a queued writer
without leaking its reservation. Standalone ObjectDB writing APIs are unchanged.

Coverage: `TransactionLogCaptureTest`, `WriterCancellationTest` and
`ReplicationPreparationTest` cover actual compaction retention, decoding, cancellation and startup index reconciliation.

## Local execution after publication stops

Local transactions continue using the ordinary file collection, including commits, rollbacks and compaction.
There is no scratch collection, special file extension, write redirection or transaction-boundary switch in core.
Stop remote work using the publisher's independent cancellation token and authority checks. Already dispatched
storage requests may still land and use the existing ambiguous-write reconciliation rules.
On restart, restore validates the local cache against the selected Blob history: files absent there are discarded,
and differing tails are replaced through normal restore. File extensions do not determine eligibility.
Production startup validation and role orchestration remain pending; there is no separate scratch cleanup path.

## Streaming remote KVI export

`BTreeKeyValueDB.CaptureKeyIndexSnapshot(cancellationToken)` captures the currently published local root and pins
its required source files. It does not publish a pending batch. The replication coordinator must establish that the
captured cut is canonical before publishing it. Keep the database alive until the snapshot is disposed;
normal writes and local compaction may continue independently of remote publication.

`KeyIndexSnapshot.Sources` lists complete sealed PVLs and the minimum required prefixes of canonical TRLs. After
confirming those dependencies in Blob Storage, call `WriteTo(IMemWriter, generation, pureValueFileIds, remoteToken)`.
The output is forward-only and can upload chunks directly; no local KVI file, seek or complete in-memory KVI is required.
The serializer preserves native compression, values, offsets, CommitUlong, metadata and the TRL recovery cursor.
The supplied map must cover every PVL exactly once and must not contain TRLs, zero IDs or colliding destinations.
Generation must exceed the source generations; remote destination allocation/finalization remains the caller's job.
Neither the live BTree nor its local file IDs are changed.

The internal replication `CheckpointPublisher` retains session-local placements for whole PVLs. Verified complete
sealed downloads are recorded with the same remote ID; newly uploaded PVLs retain their allocated remote ID for
later checkpoints. Successful PVL uploads remain reusable even if KVI upload fails. An uncertain upload retries its
reserved destination through the storage adapter's exact reconciliation. No receipt is inferred from a partial
transfer or unconfirmed result. Receipts contain IDs and lengths, not retained file bytes or roots. Restore validation,
remote lifetime/GC, canonical TRL publication and production authority enforcement still need integration.

Local `Compact(localToken)` and remote `PublishAsync(snapshot, remoteToken)` / `WriteTo(..., remoteToken)` use
independent cancellation. Loss of leadership cancels remote operations; it does not cancel or roll back local maintenance.
The caller disposes the snapshot after the remote attempt completes, including cancellation.

## Canonical capture publication lane

The internal replication `CanonicalTrlPublisher` snapshots the latest complete local position and publishes the
contiguous native prefix from its selected remote tail to that position. It follows native predecessor file metadata,
prepares successors before selecting the predecessor, and retains a fixed plan during ambiguous outcomes. Multiple
local transactions, including rollbacks, coalesce into one publication. Only the final successful selection advances
`Acknowledged`; concurrent later local transactions remain pending. A restored tail must already be verified against
local bytes. Tail adoption updates conditional metadata without changing local execution.

Publication uses a remote cancellation token and checks local lease authority before every write. `Pending` retains
an unresolved intent; `retryPending` may resend only that same conditional write. `Adopted` reports a metadata-only
term transition, while `Published` acknowledges the fixed completed position. `Conflict` requires coordinator rediscovery and a new
verified lane. After authority loss, read-only reconciliation may confirm an earlier write without enabling new writes.
The lane does not acquire leadership, allocate canonical IDs, implement Azure I/O or perform production cold restore.

### Allocation and compaction retention

The database-aware `IFileCollectionWithFileInfos.AddFile(hint)` derives parity from the file type and database mode.
Only the underlying generic `IFileCollection` accepts an explicit parity, since it does not own that database policy.

`TransactionLogCaptureTest.CompactionRetainsUnacknowledgedHistory` overwrites the only
value repeatedly and runs the actual compactor. With capture disabled the obsolete first TRL is deleted; with capture
enabled its native transaction remains readable until acknowledgement. Retaining current values or a current root alone
cannot preserve obsolete commands needed for publication or follower comparison. Follower comparison wiring is still
pending; the current capture consumer is the canonical publisher.

### Relation schema initialization

After opening ObjectDB, call `InitializeRelations` with the complete relation-type list before application processing.
It checks schemas and indexes read-only and, when needed, persists all changes in one writing transaction, including
schemas of new empty relations. Creation callbacks run in that same transaction. CommitUlong remains unchanged.
Startup orchestration supplies leader authority before invoking initialization that may write; follower startup must
use compatible published schemas. The core API does not implement leader admission.

Application row writes and ID allocation never create relation metadata. The legacy transaction initialization path
also persists schemas eagerly; rollback removes affected cached registrations before releasing the writer so a retry
initializes them again. Internal rollback actions must not throw.
