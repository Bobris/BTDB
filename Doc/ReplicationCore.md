# Core preparation for replication

These are opt-in BTDB building blocks. They do not implement leader election, Blob publication, follower comparison,
volatile shutdown files or replicated compaction. Those remain in the replication layer/design; setting these options
alone does not make a database replicated or enforce leader authority.

```csharp
using var kv = new BTreeKeyValueDB(new KeyValueDBOptions
{
    FileCollection = files,
    RequireExplicitTransactions = true,
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
lookup now use explicitly read-only transactions. `RequireExplicitTransactions` defaults to false.

Replication mode (`IFileReplicatedCollection`) automatically assigns fresh odd IDs to TRLs and fresh even IDs to KVI and PVL files, without allocating intermediate dummy files. Odd and even IDs have independent
allocation sequences: allocating a PVL or KVI does not advance the next TRL ID, and vice versa. `FileIdParity.Any` allocates above both maxima and advances the sequence matching the allocated ID. The policy is enforced by the replicated
database file collection; standalone collections keep unconstrained allocation. Replication does not support sub-databases. A valid
legacy even tail is closed before the first new transaction bytes and linked to a fresh odd TRL. Existing file IDs and
value references are preserved; size-based rotation may still happen inside transactions. `InMemoryReplicationFileStorage` is a dedicated clone for replication cache/restore and implements
`AddFile(hint, FileIdParity)`. Existing in-memory and disk collections keep their standalone API and allocation. This is not a distributed file allocator.

`InMemoryReplicationFileStorage.ImportFile(fileId, hint)` creates an empty file under the exact nonzero ID. Use it for native-file
restore rather than approximating an identity through the allocation sequence. Imports can complete in any order,
including below existing maxima, and advance only the matching parity's maximum when necessary. Existing IDs are
rejected without opening or overwriting the file, even with a different hint. Import and allocation are serialized
per collection to prevent duplicate creation. The caller fills and validates the imported file before opening the
database and removes it on failure. Custom collections opt in by implementing the import operation.

Read-only relation registration stays in memory without opening a hidden writer. The first writing transaction
that accesses the relation persists its name/version, applies pending schema/index changes, and invokes its
creation callback. This also applies when that writer only reads the relation. An unrelated writer does not
initialize it. Rollback resets pending initialization so a later writer retries it. Before deferred index upgrades
have run, read-only transactions still see the stored indexes. Use explicit startup initialization when application
reads require the upgraded indexes. Object/table metadata retains its normal data-write persistence path.


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

`KeyValueDBOptions.TransactionLogCapture` accepts a parameterless `TransactionLogCapture` only through the replication
`BTreeKeyValueDB.OpenAsync` path. Synchronous constructors reject this option before accessing files. Capture initializes
from the remote inventory without reading TRL headers. It stores only two
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
For standalone snapshots, generation must exceed the source generations. Replication snapshots ignore the legacy
generation argument and write zero in the native header field; remote destination allocation/finalization remains the caller's job.
Neither the live BTree nor its local file IDs are changed.

The internal `ReplicationFileSet` implements `IFileReplicatedCollection` with two separate inventories.
Inherited `GetCount`, `GetFile`, and `Enumerate` expose only physical local cache/storage, including unverified
cache files; none downloads a remote body. After `InitializeAsync`, `GetRemoteCount`, `GetRemoteFile`, and
`RemoteEnumerate` expose the selected remote inventory, independent of local cache contents. Remote handles are
read-only and bound to the selected remote version; reading one does not populate local storage. Equal numeric IDs
alone never establish that cached and remote bytes match. Initialization validates existing local counterparts. `PrefetchAsync` downloads missing counterparts before BTDB
reads them; it validates any cache candidate introduced after initialization. Creating or removing a local file does not change remote inventory. Remote deletion remains
part of the publication protocol. No caller-driven file-restore phase is required.

Existing `BTreeKeyValueDB` constructors retain the original synchronous opening, eager metadata loading and advisory
prefetch for ordinary collections. They reject `IFileReplicatedCollection` before reading files; use `OpenAsync`
for that interface. `IFileCollection` retains only its original operations. Replication adds parity allocation,
`InitializeAsync`, `PrefetchAsync`, `GetFileType`, `ReadFileInfoAsync`, and the three remote inventory operations
on `IFileReplicatedCollection`. Type/header lookup uses the selected remote metadata, or local metadata for newly
created files without a remote counterpart.
`GetLocalFileId(remoteFileId)` exposes the session's stable remote-to-local ID assignment, even before a file is
cached. Initialization normally assigns identical IDs; verified PVL placements can retain a different local ID.
KVI loading translates remote PVL references into local IDs, and prefetch resolves those IDs back to the selected
remote objects. TRL and KVI IDs remain unchanged. Local eviction removes cached bytes but keeps the assignment
stable for a later prefetch. The mapping is memory-only: after restart a differently numbered local copy may be
removed and downloaded again under the remote ID. There is no persisted mapping or search across local file contents.

Leader checkpoint publication calls `PublishPureValuesAsync(source, cancellation)` for each complete sealed local
PVL pinned by its snapshot. It returns a confirmed remote ID, reusing a verified placement or reserving and uploading
to a fresh remote ID. Only confirmed uploads establish a mapping; uncertain outcomes retain their reserved ID for
retry. Calls are serialized by the owner and use its fenced remote adapter. Follower/local compaction never invokes
publication. Native KVI upload still starts only after every PVL and required canonical TRL is confirmed.

The prototype uses dedicated `InMemoryReplicationFileStorage` for exact-ID cache population, parity allocation,
and filename type hints. Existing standalone collections are not replication storage backends. `ImportFile` is not a logical replication operation.
`OpenAsync` on an ordinary collection retains standalone semantics, including historical opening and retention.

The collection owner first calls and awaits `InitializeAsync(cancellation)` to discover the remote inventory,
removing local files without a mapping to the complete remote listing. Mapped candidates are checked during initialization: compare the filename extension first, then length, then calculate the local SHA-256 and
compare it with remote metadata. Remove mismatches without downloading a replacement. Validated files remain cached
and prefetch reuses them without hashing again. Active files or files without trustworthy SHA metadata are removed
for later download. Failed or cancelled discovery leaves local files untouched; repeated initialization after success does not
remove files created in the current session. The owner then passes the initialized collection to `BTreeKeyValueDB.OpenAsync(options, cancellation)`. Neither `OpenAsync`
nor `PrefetchAsync` invokes initialization. Remote inventory/metadata access and prefetch throw
`InvalidOperationException` while initialization is incomplete, including after a failed or cancelled attempt;
local cache lookup remains available. Retry initialization explicitly before retrying open.

`ReplicationFileSet` accepts `IKeyValueDBLogger` through its `logger` constructor argument or `Logger` property.
Pass the same instance to `KeyValueDBOptions.Logger` so initialization and database operation share logging.
Each cache removal logs its local ID, mapped remote ID when available, and reason: absent from remote inventory,
extension mismatch, length mismatch, SHA validation failure, active remote file, missing SHA metadata, or failed
download. Downloads use canonical native extensions (`.trl`, `.kvi`, `.pvl`, `.hid`, `.hpv`).

```csharp
await files.InitializeAsync(cancellation);
using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions { FileCollection = files }, cancellation);
```

`OpenAsync` reads the initialized remote inventory through `RemoteEnumerate` without fetching file bodies.
Local-only cache files are not recovery candidates. Its separate `LazyFileCollectionWithFileInfos` caches metadata only when requested;
`GetFileType` derives types from filename extensions. Replication does not use generation: higher fileIds identify newer
TRLs within the TRL sequence and newer KVIs within the KVI sequence. TRLs retain their predecessor fileId links.
PVL IDs carry no age relative to TRLs. KVI candidates are tried in descending fileId order without scanning their
headers first. HID/HPV inventories and sub-database creation are unsupported in this mode.
Only a candidate being tried has its header/body read. TRL linkage reads back from the latest file only as far as the
accepted KVI cursor, or through the whole chain if there is no valid KVI. Older unused KVI/TRL headers stay unread.
Replication opening rejects `OpenUpToCommitUlong` and `PreserveHistoryUpToCommitUlong` before accessing files.
Use the existing synchronous constructors for either feature. History retention cannot be enabled later on a database
opened with `IFileReplicatedCollection`.
`ReadFileInfoAsync` reads required variable-length native headers through small version-bound ranges. Full KVI/TRL bodies needed to select a KVI and replay are fetched
asynchronously before decoding.

At the end of `OpenAsync`, issue `PrefetchAsync` for every value/log file referenced by the selected valid KVI,
including its TRL cursor file. If no valid KVI was accepted, request every TRL instead. Issue all requests before
awaiting them, and do not return the database until all finish. Unreferenced PVLs need neither header reads nor full
downloads during open. Later writes, snapshot export, diagnostics and local compaction do not inspect PVL headers
to recover generations. New replication files retain the native header layout with zero in its unused generation field.
Failed or cancelled opening releases its native roots without flushing an appender or changing the downloaded TRL.

The collection shares concurrent prefetch requests for the same file and limits active cache verification/downloads
(`maxConcurrentDownloads`, default 4); requesting the entire recovery set does not allocate one transfer buffer per
queued file. Cancellation of one waiter does not cancel another waiter's shared transfer; disposing the collection
cancels and drains remaining work. The caller still owns the physical cache and remote adapter. Synchronous reads
on an unprefetched logical file wait for its cache population; normal reads of the prefetched recovery set stay local.

A sealed cache candidate is reused only after a fresh whole-file checksum matches selected remote metadata.
A mismatching file is replaced; an active TRL is always downloaded again. Version-bound downloads use assigned local-ID
imports internally and remove partial files on failure. Complete version-bound downloads of sealed PVLs with checksum metadata establish reuse
receipts. `CheckpointPublisher` uses those receipts and confirmed upload placements, reserving new destinations from
the remote inventory. An uncertain upload retries the same destination through the storage adapter's reconciliation.
Receipts retain IDs and lengths, not file bytes or roots; their destinations must remain protected from remote cleanup.
Production Azure transport, canonical history/authority orchestration and remote lifetime/GC still need integration.

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

Local replication compaction considers only files present in the local cache; it never downloads or deletes a
remote-only entry. Evicted remote entries remain available for explicit prefetch.

Standalone databases retain the original generation-based `Compactor`. Databases backed by `IFileReplicatedCollection` use a
separate `ReplicationCompactor`: it can compact physical values independently, creates no local KVI, and protects
file IDs used by live roots, readers and export snapshots. The fileId set decoded from KVI is retained with only its
TRL fileId and offset, without storing its root. It is released once every live root has advanced beyond that position.
Pointer rewrites can leave the TRL position unchanged, so live roots also supply their current physical references. A disposable local cache need not remain independently reopenable. The capture boundary additionally retains unacknowledged
TRLs even when no current value points into them. Both PVL and old TRL can contain live values; neither is released
merely because its ID is numerically old. Concurrent replication compaction calls are serialized so one pass cannot
mistake another pass's newly created, unreferenced PVL for garbage. `CreateKvi` is unsupported in replication mode;
remote publication uses snapshot export. Native generation handling and HID/HPV remain unchanged for standalone use.

The database-aware `IFileCollectionWithFileInfos.AddFile(hint)` derives parity from the file type and database mode.
The underlying `IFileReplicatedCollection` and dedicated `InMemoryReplicationFileStorage` accept explicit parity.

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

All `IFileReplicatedCollection` members require explicit implementations, including identity mapping and
initialization for already-ready collections. The interface provides no fallback behavior.

Before admitting writes after a leader transition, the role owner quiesces collection operations, discards old
remote handles, and awaits `RefreshRemoteInventoryAsync`. Refresh requires prior initialization and replaces
remote membership/ETags only after successful discovery. It preserves local files and session mappings, including
unpublished local PVLs; it does not rerun startup cleanup. Changed objects are revalidated on prefetch. Leadership
acquisition and recovery/catch-up of the database are separate prerequisites, not effects of inventory refresh.


### Canonical restore integration

The replication owner selects published canonical links and binds downloads to observed
object versions through `CanonicalTrlInventory`, a remote inventory adapter for genesis/TRL-only history.
Initialize `ReplicationFileSet`, then call ordinary `BTreeKeyValueDB.OpenAsync` directly. Native header loading and
replay stay in core; there is no separate restore wrapper or header-validation pass. A missing published root fails the attempt; a changed remote
version or failed transfer requires disposal and rediscovery before retrying. Native transaction recovery retains its
existing behavior, with no additional expected-end option or strict replay mode in core BTDB.

Ordinary KVI-based restart already uses collection initialization followed by `OpenAsync`.
`RestartRecoveryTest` verifies it after deleting obsolete history, including genesis, and discarding the old process
state, then resumes publication using tail metadata read from remote storage. The genesis-only helper is not required
for that path. Durable allocation, disk-backed replication storage, production adapters and recovery-race
qualification remain pending.

Remote downloads fetch up to four 256 KiB blocks concurrently per active file and append completed batches in order.
They do not recompute or validate SHA; checksum validation still applies to preexisting cache candidates. A failed block
cancels and drains sibling reads before partial-file cleanup and pooled-buffer return.

Downloaded files always retain their remote IDs. Earlier cross-ID upload placements may reuse an existing local file,
but a redownload installs the remote ID and replaces the old placement rather than allocating another local ID.
