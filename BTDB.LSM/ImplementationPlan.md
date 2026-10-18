# BTDB.LSM — implementation plan

Status: phases L0–L6 are done (2026-10-16). The benchmark report (Measurements.md, sections 4–7) is reviewed and
the preview packages are published with BTDB 35.11.3. Decisions D1–D7 are confirmed (section 21). The v1 items left
after the preview (`MaxTransactionMemory` and compaction read pacing) are done (section 20.6); every item still open
is admitted only with evidence (sections 20.1, 20.2 and 20.4).

* L0: the counted LSM prototype (now `BTDB.LSM.Test/Counting/Prototype`) passes 10⁶ random operations against an oracle, and the live
  Azure storage screening is done (LRS, GZRS and Premium). Evidence is in [Measurements.md](Measurements.md).
* L1: `LsmKeyValueDB` passes the shared `KeyValueDBTestBase` suite with default and tiny settings. It also passes a
  randomized oracle test against `BTreeKeyValueDB` (180,000 operations over several cursors, commits, rollbacks and
  snapshots) and persistence tests.
* L2: leveled compaction, the positioned merging iterator, value separation with garbage collection, the native counted
  memtable, partitioned indexes and filters, the native block cache and background flush and compaction with write
  stalls. Section 20.1 lists the remaining differences from the target design.
* L3: WAL with group commit and multi-chunk commits, durability waits, recovery, the writer lease with epoch fencing,
  lease loss, ambiguity reconciliation and orphan adoption. Verified by crash-at-every-write and ambiguous-write
  simulations. Section 20.2 has the details.
* L5 (done before L4, because it needs no Azure resources): `RefreshAsync`, `WaitForCommitUlongAsync`, the reader
  registry and time-based retention. Section 20.3 has the details.
* L4: `BTDB.LSM.Azure` passes its suite on Azurite and on live Azure, and the end-to-end benchmark ran on Premium and
  LRS ([Measurements.md](Measurements.md), section 4).
* L6: ObjectDB suites run on the LSM, the benchmark compares it with `BTreeKeyValueDB` + `AzureBlobFileCollection`,
  scan readahead and a faster select were added from the findings, and the documentation and preview packaging are
  prepared. Section 20.4 has the details.

Confirmed requirements:

* A new `IKeyValueDB` implementation in a separate library, `BTDB.LSM`, inspired by Pebble (CockroachDB). The Azure
  adapter is a separate package, `BTDB.LSM.Azure`.
* The durable store is object storage, primarily Azure Blob Storage.
* Memory use is bounded by configuration and does not grow with database size. The implementation has no local
  disk cache and does not copy files to a local disk.
* One writer process and any number of consistent readers in that process.
* Other processes can read a consistent state and catch up to the freshest durable state on demand, while the writer
  keeps writing. There is no automatic background refresh. Instead, a reader can wait until it observes a given
  CommitUlong or higher, or explicitly catch up.
* Every instance opens as a reader. Starting a writing transaction waits for the writer lease, so the writer is simply
  the instance that currently holds the lease.
* Commits are non-durable by default. `WaitForDurabilityAsync` waits for durability.
* Applications are expected to use fairly large transactions for throughput. Transaction batching (`inBatch`) is not
  supported, and history preservation (`PreserveHistoryUpToCommitUlong`, `OpenUpToCommitUlong`) is not needed.

## 1. Goal and scope

`BTreeKeyValueDB` keeps every key and a 12-byte value pointer in memory. `AzureBlobFileCollection` keeps a complete
local copy of every TRL, PVL and KVI file. Both grow linearly with the database. `BTDB.LSM` stores a log-structured
merge tree directly in object storage:

* Immutable sorted tables live in object storage and are read with ranged GETs.
* A bounded in-memory block cache serves hot data.
* Only small per-table metadata stays resident.
* A write-ahead log (WAL) in object storage provides durability.
* A manifest log describes the table set.
* The same objects that make the writer durable also let other processes follow the database.

The engine implements the full `IKeyValueDB` contract, including the parts a classic LSM does not offer: O(log n) key
index (`GetKeyIndex`, `FindKeyIndex`), O(1) `GetKeyValueCount`, `EraseUpTo` returning an exact count, and cursor
stability across modifications. `ObjectDB` and `RelationsDB` must run on it unchanged.

Out of scope for v1:

* Multiple concurrent writers, or writes from reader processes.
* Integration with the `BTDB.Replication` leader, follower and TRL comparison protocol. `BTDB.LSM` provides its own
  durability and its own cross-process readers.
* Transaction batching (`StartWritingTransaction(inBatch: true)`). The hint is ignored, as in `InMemoryKeyValueDB`.
* History preservation and opening at a historical CommitUlong.
* Automatic background refresh of readers, and a push channel from the writer.
* Automatic standby election. Writer failover happens because another instance is waiting for the lease in
  `StartWritingTransaction`.
* A local disk or secondary cache. It can be added later behind the same block cache interface (section 12).
* Remote or offloaded compaction. The format keeps it possible (section 13).
* Converting a `BTreeKeyValueDB` file set in place. Migration goes through `KeyValueDBExportImporter`.
* S3 and GCS adapters. The storage abstraction is provider-neutral (section 5), but only Azure is implemented and
  qualified.

## 2. Source findings

Inspected: BTDB `45527451`.

| Source | Observed behavior | Consequence |
| --- | --- | --- |
| `IKeyValueDBCursor` (`BTDB/KVDBLayer/Interface/IKeyValueDBCursor.cs`) | `GetKeyIndex`, `FindKeyIndex(prefix, idx)`, `EraseUpTo` returning a count, `FastIterate` passing a `keyIndex`, and fuzzy invalidation after erase. | Rank and select are part of the contract. They cannot be emulated by scanning. |
| `BTDB/ODBLayer` usage | About 49 `GetKeyIndex` and 11 `FindKeyIndex` call sites. Relation `Count`, `CountWithPrefix`, `CountWithProposition`, enumerator `Position`, sorted queries (gather indexes, sort, fetch with `FindKeyIndex`) and per-step bound checks all depend on them. | Rank and select must be O(levels × log n). Rank must be O(1) while a cursor steps (section 6.5). |
| `IKeyValueDBCursorInternal` and the BTree cursor | When one cursor modifies data, every other cursor in the same transaction is notified and re-seeks by key index. | The LSM cursor keeps the same observable behavior. It re-seeks by key and adjusts a cached index (section 7.4). |
| `BTreeKeyValueDB`, `InMemoryKeyValueDB` | No shared base class. The writer queue (FIFO `TaskCompletionSource`), batching and the upgradable `StartTransaction` are duplicated. `InMemoryKeyValueDB` ignores `inBatch` and still passes the shared tests. | Port the writer queue semantics, verified by the shared tests, and ignore `inBatch` as `InMemoryKeyValueDB` does. Extracting a shared helper is optional and separate. |
| `BTDB/KVDBLayer/BTreeMem` | Managed copy-on-write B-tree with subtree counts; each transaction gets a new root. Internal to `BTDB`. | A good prototype for the memtable. The production memtable needs a second aggregate (delta sum) and native memory (section 7). |
| `BTDB/BTreeLib` | Native-memory counted B-tree with 12-byte value slots, used by `BTreeKeyValueDB`. | A pattern for the native memtable. It is not reusable as is, because it has no delta aggregate and no value arena. |
| `KeyValueDBTestBase` (`BTDBTest/KeyValueDBTestBase.cs`) | About 46 implementation-agnostic tests behind one abstract `NewKeyValueDB()`. | Subclassing it gives the generic contract suite for free. `KeyValueDBFileTestBase` is tied to the BTree file format; only its reopen and ulong tests apply. |
| `ObjectDbTestBase` and `ObjectDBTest` | They hardcode `BTreeKeyValueDB` or `InMemoryKeyValueDB`. | Add a factory hook so a chosen subset runs on the LSM (phase L6). |
| `BTDB.AzureStorage` | Whole-file download and upload; no range read, no ETag, no conditional write. | Not reusable for the read path. |
| `BTDB.Replication/EventLog/IEventLogStorage.cs`, `AzureEventLogStorage.cs` | A clean object-store contract: version-bound range read, create-if-absent or If-Match write, list, scheduled deletion, conditional delete. 404/412/416 mean "version changed". | Template for `ILsmStorage` (section 5). |
| `AzureReplicationStorage.Upload`, `ReplicationFileSet` | Large immutable uploads use 4 MiB staged blocks, 4 in parallel, `If-None-Match: *` and a SHA-256 in metadata. Cold restore reaches 371–594 MiB/s with 4–16 parallel 4 MiB range GETs. | Reuse for table and value-file uploads and for compaction input reads. |
| `ObjectStorages.md`, `LeaseAuthority`, `AzureLeaderStorage` | SDK retries are disabled because they are unsafe for CAS. Writes after dispatch have three outcomes (Applied, Rejected, Ambiguous). Leases use a conservative local deadline. A 15 s lease was measured expiring 14.94–15.06 s after dispatch. | Reuse the outcome model, the lease deadline logic and the "ambiguous is never proof of absence" rule. |
| `EventLogImplementationPlan.md` §16, `Measurements.md` | Conditional Put Blob p50/p99: 8.7/19.3 ms at 64 KiB, 9.5/27.0 ms at 256 KiB, 22.5/55.4 ms at 1 MiB. About 100–115 writes/s on one blob. | WAL group commit has a floor of about 10 ms per durable upload. Small range-GET, HEAD, 404 probe and List latencies were never measured; phase L0 measures them. |
| `BTDB.Replication.Test/Simulation` | Deterministic scheduler, seeded random and a simulated blob store that separates dispatch, effect and response, with fault injection. Internal to the test project. | Copy into the LSM tests, or move to a shared test library. |
| Compression and checksums | In-house Snappy (`BTDB/SnappyCompression`), Brotli from the BCL, custom `XxHash64`, Fletcher32. No CRC32C, LZ4, Zstd or bloom filter. | v1 uses Snappy and XxHash64 with no new dependencies. Bloom filters are new code. Add LZ4 or Zstd only with measured evidence. |
| `BTDB/Properties/AssemblyInfo.cs` | `InternalsVisibleTo` is granted only to `BTDBTest`. | Add `InternalsVisibleTo("BTDB.LSM")` (decision D7). |

## 3. Invariants

| ID | Invariant |
| --- | --- |
| L1 Single writer | At most one writer session authors WAL chunks and manifest edits. Every write by a fenced session fails at storage. A takeover never depends on timing alone (section 14). |
| L2 Immutable objects | Every table, value file, WAL chunk and manifest edit is written once with create-if-absent and never modified. Only deletion removes it. |
| L3 Atomic commits | A reader in any process observes a prefix of commits that ends at a commit boundary. A commit is visible to another process only once every WAL chunk carrying it is durable. |
| L4 Snapshot isolation | A transaction observes one immutable state: a pinned `Version` (table set) plus a list of immutable memtable roots. Nothing a later commit, flush or compaction does changes that state. |
| L5 Exact counts | Every entry carries a delta that makes rank and count additive across layers (section 6). Flush and compaction preserve the sum of deltas for every key. |
| L6 Bounded memory | Resident memory is block cache + memtables + pinned per-table metadata + per-transaction buffers. Every term is bounded by an option, except per-table metadata, which is proportional to the table count and is reported. |
| L7 No silent loss | A missing or corrupt object raises `BTDBException`. It is never treated as an empty range. A reader that has fallen behind retention fails explicitly with "snapshot too old". |
| L8 Deferred deletion | An object is deleted only after no live version references it in the writer and no registered reader can still need it (section 16). |
| L9 Ambiguity | A timeout or lost response after dispatch is never proof that a write did not land. The writer reconciles by reading before it continues. |

## 4. Architecture overview

```
          instance holding the writer lease                     instances without the lease
 ┌────────────────────────────────────────────┐        ┌──────────────────────────────────┐
 │ LsmKeyValueDB (writer)                     │        │ LsmKeyValueDB (reader)           │
 │  write txn → COW memtable root ─┐          │        │  RefreshAsync /                  │
 │  commit → WAL buffer ──group────┼─► wal/   │        │  WaitForCommitUlongAsync:        │
 │  memtable full → flush → L0 ────┼─► tables/│◄──────►│   new WAL chunks → memtables     │
 │  compactor → Ln tables/values ──┼─► values/│ object │   new manifest edits → Version   │
 │  version edits ─────────────────┴─► manifest/ store│  shared read path (below)        │
 │                                            │        │  StartWritingTransaction waits   │
 │  read path: merging iterator over          │        │   for the lease, then becomes    │
 │   memtables + L0 sublevels + L1..Ln,       │        │   the writer                     │
 │   block cache (native, bounded),           │        │                                  │
 │   ranged GETs with readahead               │        │                                  │
 └────────────────────────────────────────────┘        └──────────────────────────────────┘
```

Writer and reader are not separate classes or open modes. They are two states of the same `LsmKeyValueDB` instance
(section 14).

Module layout (namespaces inside `BTDB.LSM`):

| Module | Content |
| --- | --- |
| `Storage` | `ILsmStorage`, write outcomes, `InMemoryLsmStorage`, `FileSystemLsmStorage` (development and benchmarks only; a store, not a cache), latency and fault wrappers. |
| `Format` | Block builder and reader, table writer and reader, value-file writer and reader, WAL chunk codec, manifest edit codec, bloom filter, checksums. |
| `Memory` | Native arena, counted COW memtable, block cache. |
| `Engine` | `Version`, `VersionSet`, snapshots, merging iterator, rank and select, range tombstones. |
| `Write` | Writer queue, write transactions, WAL group commit, memtable rotation and flush, backpressure. |
| `Compaction` | Picker, executor, value-file GC, rate limiting. |
| `Refresh` | On-demand catch-up from WAL and manifest, `WaitForCommitUlongAsync`, reader registry. |
| `Lease` | Writer lease acquisition and renewal, fencing, the reader-to-writer transition. |
| `Api` | `LsmKeyValueDB`, transaction, cursor, `LsmKeyValueDBOptions`, `ILsmKeyValueDB` extras. |

Packages (decision D1): `BTDB.LSM` (core, provider-neutral, in-memory and file-system storage) and `BTDB.LSM.Azure`
(Azure adapter, references `Azure.Storage.Blobs`). Tests go in `BTDB.LSM.Test` and `BTDB.LSM.Azure.Test`. This mirrors
`BTDB.Replication` / `BTDB.Replication.Azure`. `BTDB` grants `InternalsVisibleTo("BTDB.LSM")` (decision D7), so the
allocator, `StructList`, the Snappy codec and similar helpers are reused rather than copied.

## 5. Storage abstraction and object layout

```csharp
public interface ILsmStorage
{
    // Immutable object create; fails with Rejected if the key exists. Content may be streamed in parts.
    ValueTask<LsmWriteOutcome> CreateAsync(string key, LsmObjectContent content, CancellationToken ct);

    // Small-object read (WAL chunks, manifest edits); returns null on 404.
    ValueTask<LsmObject?> TryReadAsync(string key, CancellationToken ct);

    // Ranged read of an immutable object. Synchronous variant for cursor cache misses.
    ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken ct);
    void ReadRange(string key, long offset, Span<byte> destination);

    IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, string? startAfter, CancellationToken ct);
    ValueTask DeleteAsync(string key, CancellationToken ct);

    // Writer lease on a single lock object (section 14).
    ILsmLeaseStorage Lease { get; }
}
```

* All objects are immutable, so version-bound reads are unnecessary: a key never names two contents. Each object
  stores its length and a checksum, and every block has its own checksum.
* The outcome is `Applied`, `Rejected` (409/412: the key exists, so the session is fenced) or `Ambiguous`. On
  `Ambiguous`, the writer reads the object back and compares its checksum. If it matches, the write is adopted; if it
  differs, the session is fenced; if the object is absent, the write is retried once under the same key.
* The synchronous `ReadRange` exists because `IKeyValueDBCursor` is synchronous. The Azure adapter uses the SDK's
  synchronous `DownloadStreaming` (HttpClient sync send), not sync-over-async. Thread-pool impact is a measured risk
  (section 22).
* The Azure client is created with `Retry.MaxRetries = 0` for conditional writes, as in `ObjectStorages.md`. Reads use a
  separate client with bounded retries.

Object layout under a configurable prefix:

| Key | Content |
| --- | --- |
| `LOCK` | Lease object (section 14). |
| `manifest/{seq:D16}` | One version edit, or a full checkpoint every N edits (section 11). |
| `wal/{chunk:D16}` | One WAL chunk (section 8). |
| `tables/{fileNum:D16}.sst` | Sorted table (section 9). |
| `values/{fileNum:D16}.vlf` | Value file (section 10). |
| `readers/{readerId}` | Reader registry heartbeat (section 16). |

Zero-padded sequence numbers make `ListAsync(prefix, startAfter)` return them in order. A reader probes the next
expected name with `TryReadAsync`, so it does not need to list.

## 6. Data model: counted entries

### 6.1 Entry

An internal entry is `(userKey, kind, existsBelow, value)`:

* `kind ∈ {Set, Delete}`.
* `existsBelow` says whether the key exists in the logical state formed by everything older than this entry's
  component.
* `value` is inline bytes or a value-file handle.

The rank delta is derived from these fields, not stored separately: `δ = [kind = Set] − [existsBelow]` ∈ {+1, 0, −1}.

The writer always knows `existsBelow`. `CreateOrUpdateKeyValue` must report "created" anyway, and `EraseCurrent` only
deletes existing keys. A Delete whose `existsBelow` is false is a no-op and is never written.

No per-key sequence numbers are needed. A snapshot pins whole immutable components (L4), so compaction never has to
keep older versions of a key for snapshots: old snapshots keep reading the old table files, which are kept alive by
reference counting (section 16). Ordering between components follows their position (memtables, then L0 sublevels by
age, then L1..Ln).

### 6.2 Why deltas telescope

For a key, consider its entries stacked from newest to oldest component. Each entry's `existsBelow` equals the
existence produced by the entries below it, so the sum of their deltas equals `[the key exists in the newest state]`.
Therefore:

* `count(snapshot) = Σ δ over all entries in all components`. This total is maintained incrementally, so
  `GetKeyValueCount` is O(1).
* `rank(x) = Σ δ over all entries with userKey < x`, summed independently per component.

Merging entry N (newer) over O (older) for the same key gives `kind = N.kind` and `existsBelow = O.existsBelow`; its
delta equals `δN + δO`. Compaction therefore preserves the sum (L5). A merged `Delete` with `existsBelow = false` is
dropped at any level, not only at the bottom. This is a stronger form of Pebble's SINGLEDEL that comes free with the
counts.

### 6.3 Range tombstones

`EraseUpTo` and `EraseAll(prefix)` (19 call sites in ODB) must not write one point Delete per key for large ranges.
A range tombstone `[a, b]` in component C carries `coveredBelow`: the number of keys in `[a, b]` that exist in the state
below C. The writer already computes this to return `EraseUpTo`'s result.

Tombstones in a component are fragmented into non-overlapping pieces, each with its own `coveredBelow`. Point entries
newer than a tombstone in the same component keep their `existsBelow` relative to the state with the tombstone
applied.

Rank with tombstones, per component C, from newest to oldest:

```
rankC(x) = Σ δ(points in C with key < x)
         − Σ coveredBelow(fragments in C entirely < x)
         − countBelowC([a, x))  for the at most one fragment [a, b] containing x
```

`countBelowC` is a rank difference over the components older than C. A fragment can straddle `x` only if `x` is a
newer point entry inside a range erased earlier. Recursion therefore branches at most once per component, giving a
worst case of O(components²) block lookups and a typical case of O(components).

Compaction and fragments: when compaction merges a fragment from C into the next component down and output still has
older components below it, it subtracts the merged component's own contribution inside the fragment from
`coveredBelow`. When it splits a fragment at a new boundary, it computes the two parts with a rank query on the older
components. A fragment whose `coveredBelow` reaches 0 with no older components is dropped.

A small range (at most `PointDeleteRangeLimit` keys, default 64) is written as point Deletes, because they compact
better and keep rank queries simple.

### 6.4 Per-component prefix sums

| Component | Rank support |
| --- | --- |
| Memtable | A COW B-tree whose nodes aggregate `(entryCount, deltaSum)`, plus a small ordered set of tombstone fragments. |
| Table | Each index entry stores the cumulative delta sum before its data block. Each top-level index entry stores the sum before its index partition. Restart points inside a data block store the cumulative sum, so at most one restart interval (16 entries) is scanned. Tombstone fragments are pinned with cumulative `coveredBelow`. |

`rank(x)` reads one index path per component and at most one data block per component. Index partitions and recently
used data blocks are normally cached.

### 6.5 Select (`FindKeyIndex`) and stepping

`FindKeyIndex(i)` finds the smallest existing key whose rank is `i`.

1. **Index-only bracketing.** Pivots are block boundaries of the component with the most blocks left inside the
   current interval. The rank of a pivot is bounded from index data only: per component, the cumulative sum before the
   block containing the pivot, plus the block's minimum and maximum running delta (exact when the pivot is the block's
   first key). Tombstone terms are pinned and exact. Only a pivot whose bounds contain `i` needs an exact rank.
   Bisection stops when no component has more than `SelectFinalBlocks` (default 2) block boundaries inside the
   interval.
2. **Exact refinement.** Compute the exact `rank(lower)` (section 6.4), then run the merging iterator forward
   `i − rank(lower)` live keys.

`FindKeyIndex(prefix, i)` is `FindKeyIndex(rank(first key with prefix) + i)`.

The algorithm stays O(components × log n) whatever the level shape. It is the riskiest piece of the design, so phase L0
prototypes it against an oracle.

A cursor caches its key index:

* `FindNextKey` and `FindPreviousKey` move it by exactly ±1 by definition.
* `GetKeyIndex` after stepping is O(1). This covers the per-step bound checks in `RelationEnumerator`.
* `FindKeyIndex(j)` close to the cached index (`|j − cached|` at most a threshold, default 64) steps instead of
  selecting.
* The cache is invalidated by a seek, and adjusted or invalidated by modifications (section 7.4).

## 7. Memtable, write transactions and cursors

### 7.1 Memtable

* A copy-on-write B+tree in native memory (pattern of `BTreeLib`). Leaves hold the key, the kind and `existsBelow`
  bits, and an arena handle to the value. Branches aggregate `(entryCount, deltaSum)`.
* Each memtable owns a native arena. Keys and values are copied into it once. The arena is freed when the last snapshot
  referencing the memtable is released.
* Tombstone fragments live in a small COW ordered structure beside the tree.
* A prototype on `BTreeMem` (managed) comes first in L1, behind the same internal interface, so the counted tree and the
  engine can be developed separately.

### 7.2 Write transactions

* The FIFO writer queue and `BTDBTransactionRetryException` behave as in `BTreeKeyValueDB`; the shared tests are the
  specification.
* `StartWritingTransaction` first ensures the instance holds the writer lease. If it does not, the request waits
  asynchronously until the lease is acquired and the instance has become the writer (section 14). Queued requests wait
  behind it in FIFO order.
* `inBatch` is ignored and `FinishTransactionBatchAfterCurrentTransaction` does nothing (decision D4). Every commit
  publishes its root. Throughput comes from larger transactions (section 7.5).
* The upgradable `StartTransaction` can become a writer on its first write only if the instance already holds the lease
  and no other writer is active. Otherwise that write throws `BTDBTransactionRetryException`. A synchronous write must
  not block on lease acquisition.
* A writing transaction gets a writable copy of the mutable memtable root. That root is its private overlay, so
  read-your-writes, rank and select work without a separate batch structure. Rollback drops the root. Nothing reaches
  the WAL before commit, except the optional early upload of large transactions (section 7.5).
* While a transaction runs, it appends each logical operation (Set with `existsBelow`, Delete, range tombstone with
  `coveredBelow`, ulong and CommitUlong deltas) to a native operation buffer. At commit the buffer becomes one WAL
  record.
* `existsBelow` for a key that is not in the memtable comes from a point lookup in the older components, using the bloom
  filter, the cache and a ranged GET. Keys the cursor has just found are already cached.
* `UpdateKeySuffix` is a lookup by unique prefix, a value read, a Delete and a Set. A separated value keeps its handle;
  it is not re-read.
* Commit publishes the new root and adds the record to the WAL buffer (section 8). `GetTransactionNumber` increments per
  commit and is recorded in the WAL and the manifest.
* Memtable rotation happens only between transactions, at a WAL chunk boundary (section 8.3).

### 7.3 Merging iterator

* The inputs are the memtables (newest first), each L0 sublevel and each Ln level as a concatenating iterator over
  non-overlapping tables. A newer component's tombstones hide older entries.
* It supports forward and backward iteration, prefix bounds, and seeking to the first or last key with a prefix (prefix
  seeks are the dominant ODB pattern). Iterators skip whole blocks or tables using the index separators and table key
  ranges.
* Values are fetched lazily on `GetValue*`. Key-only scans never touch value files.

### 7.4 Cursor stability

Cursors of a writing transaction stay in the transaction's linked list (`FirstCursor` / `LastCursor`). On a modification
by another cursor:

* Every cursor records the modification stamp. Its iterator stack is rebuilt lazily by seeking to its current key
  (copied into a cursor-owned buffer).
* A cached key index is adjusted exactly: +1 for an insert before it, −1 for an erase before it, −n for a range erase
  before it.
* A cursor whose key was erased becomes fuzzy invalid. It remembers the key, so `FindNextKey` and `FindPreviousKey`
  move to the neighbors, as the contract requires.

### 7.5 Large transactions

Large transactions are the expected way to get throughput (decision D4). A durable upload costs about 10 ms whatever
its size up to 256 KiB, so the per-commit cost must be amortized over many operations. The design keeps them cheap:

* A transaction modifies the memtable root directly. Its size adds to the memtable: the effective memtable bound is
  `MemtableSize + MaxTransactionMemory` (default 64 MiB + 256 MiB). Rotation waits for the transaction to finish.
* A commit record may span many WAL chunks (section 8.1). The chunks are uploaded with up to `WalUploadParallelism`
  (default 4) requests in flight. The chunk carrying the final part is created last, so it is the commit point.
* **Early upload (optional, admitted only if L3 measurements show commit latency dominated by WAL upload):** once a
  running transaction's operation buffer exceeds `WalChunkTargetSize`, its leading parts are uploaded before commit.
  Commit then uploads only the remainder and the final part. A rollback after early upload writes an abort marker.
  Readers and recovery buffer parts by transaction number and drop aborted or unfinished ones. The operation buffer is
  freed as parts become durable, so its memory stays bounded.
* **Spill (deferred, risk R5):** a transaction above `MaxTransactionMemory` throws `BTDBException` in v1, rather than
  growing without bound. The limit counts the memtable bytes the transaction adds and is checked before every
  modification and at commit. If a workload needs larger transactions, the overlay can be written out as private sorted
  tables and committed by one manifest edit referenced from the commit record (comparable to Pebble's flushable
  ingest). This is not part of the v1 plan.

## 8. WAL, commit and durability

### 8.1 Chunk format

Each WAL chunk is one immutable object `wal/{chunk}`:

* **Header:** magic, format version, writer epoch, memtable generation, first commit number, and a flag saying whether
  this chunk starts a new memtable generation.
* **Records:** commit records, framed with a length and a checksum. A record holds the transaction number, CommitUlong,
  ulong deltas, and the operations with their precomputed `existsBelow` and `coveredBelow`. Readers and recovery replay
  these without any table lookup.
* A record larger than the chunk limit spans chunks, with continuation flags and the transaction number in every part.
  It becomes visible only once every part through its last is present. Chunks are applied in name order. A chunk with
  an epoch lower than an earlier chunk's epoch is ignored (section 14).

### 8.2 Group commit and durability modes

* A single background uploader takes the pending commit records as soon as a commit arrives (at the latest after
  `WalFlushInterval`, default 20 ms). A batch holds about `WalUploadParallelism × WalChunkTargetSize` bytes (at least
  one record). It is split into chunks of about `WalChunkTargetSize` (default 256 KiB, confirmed by L0 measurements),
  numbered in order and created with create-if-absent, up to `WalUploadParallelism` at once. That covers the parts of
  one large commit and chunks of different commits alike. Durability advances when the whole batch is created, so a
  commit is durable once every chunk up to and including its final part is durable. Commits arriving during an upload
  form the next batch.
* `DurableTransactions = false` is the default (decision D2, as in `BTreeKeyValueDB`). `Commit()` returns once the
  memtable is updated. The durability window is about one flush interval plus upload latency.
* `ILsmKeyValueDB.WaitForDurabilityAsync(transactionNumber, ct)` completes when that commit is durable, and throws if
  the instance lost the lease before that. It is the intended way to get durable commits without blocking a thread.
  The transaction number is optional: omitted or null, it captures the latest committed transaction at the call and
  waits for it, without including later commits or uncommitted changes. Rollback queues no WAL record and needs no
  durability wait. `FlushAsync(ct)` has the same effect as waiting without a transaction number. The application
  event-ID pattern (CommitUlong = event ID) also works: after a crash the application replays from the durable CommitUlong.
* `DurableTransactions = true` remains supported for compatibility. `Commit()` then blocks until the chunk carrying the
  record is durable. That is at least one Put Blob round trip, about 9 ms at p50 and 20–30 ms at p99. The writer slot
  is released before the wait, so concurrent durable writers share uploads (Measurements.md, section 7).
* Backpressure: if the WAL buffer exceeds `MaxUnflushedWalBytes`, `StartWritingTransaction` waits asynchronously.

### 8.3 Memtable rotation and flush

When the mutable memtable exceeds `MemtableSize` (default 64 MiB), the next transaction boundary does three things:

1. It seals the current WAL chunk.
2. It marks the memtable immutable.
3. It starts a new generation, whose first chunk carries the "new generation" flag.

A flush writes the immutable memtable as one L0 table (or several, split at `TargetFileSize`) plus value files. It then
writes a manifest edit "generation g → files F, WAL durable through chunk c" and releases the memtable once no snapshot
references it.

At most `MaxImmutableMemtables` (default 2) can wait. Beyond that, writers stall asynchronously.

## 9. Table format (`.sst`)

```
[data block 0] ... [data block n]
[index partition 0] ... [index partition k]
[filter partition 0] ... [filter partition k]
[range tombstone block]
[value-file reference block]    // per value file: referenced bytes, used for GC
[properties block]
[top-level index] [top-level filter index]
[footer: fixed size, magic, version, handles of the blocks above, checksum]
```

* **Data block.** Target size `BlockSize` (default 32 KiB uncompressed, confirmed in L0: on every tier a ranged GET
  of up to 64 KiB costs the same as 4 KiB, about 8–10 ms p50 on Standard and 5 ms on Premium). Keys are prefix-compressed with a restart point every 16 entries. The restart array
  carries cumulative delta sums. Each entry stores shared length, unshared length, flags (kind, `existsBelow`, value
  inline or separated, value compressed) and the value or handle. A key larger than `BlockSize` gets a block of its
  own; tests cover keys up to 1.2 MB.
* **Compression** is per block (Snappy by default, or none), behind an interface that allows LZ4 or Zstd later.
  Checksums use XxHash64 over the stored bytes, per block and for the footer.
* **Index partition.** One entry per data block: a shortest separator, the block handle (offset, length), the entry
  count, the cumulative delta sum before the block, and the minimum and maximum running delta inside the block. The
  last two let select bound a rank without reading the block (section 6.5); in the L0 prototype they decide about 96%
  of pivots from index data alone.
* **Top-level index.** One entry per partition, with the same fields. It is pinned for every open table.
* **Filter.** A partitioned blocked bloom filter on the full user key, 10 bits per key by default. It serves exact
  lookups and the `existsBelow` checks. Prefix filters are not built, because ODB prefixes are low cardinality and would
  give no benefit. Revisit with measurements.
* **Properties:** entry count, delta sum, smallest and largest key, tombstone count, raw and stored sizes, creating
  writer epoch and format version.
* **Opening a table** takes one ranged GET of the tail. `TableTailReadSize` (default 64 KiB) normally covers the footer,
  top-level indexes, tombstones and properties; a second GET handles larger tails. These parts stay pinned while the
  table is in the version set or referenced by a snapshot.
* **Size.** `TargetFileSize` defaults to 64 MiB for L1 and below, larger than Pebble's defaults, to cut object count,
  open cost and per-table pinned memory. L0 tables are the size of a flushed memtable. Uploads use 4 MiB staged blocks,
  4 in parallel (the `AzureReplicationStorage.Upload` pattern).

## 10. Value separation (`.vlf`)

This is modeled on Pebble's blob files and on BTDB's own split between key index and value files.

* At flush, values larger than `ValueSeparationThreshold` (default 1 KiB, tuned in L6) go to a value file that is
  written next to the L0 table. The table stores a handle (fileNum, offset, length, compressed flag).
* Compaction copies handles, not values. Large values are written once, which matters on object storage, where
  rewriting is bandwidth and money.
* A value file holds compressed value records in blocks, with a small block index in its tail. Values are read through
  the block cache, or directly for values larger than one block.
* GC: each table's reference block lists, for each value file, the bytes it references. The manifest keeps the live
  bytes per value file. When a file's live ratio drops below `ValueFileGcThreshold` (default 0.5), the next compaction
  of the referencing tables rewrites the live values into a new file. A value file is deleted when no table references
  it (section 16).

## 11. Manifest, versions and snapshots

* A `Version` is an immutable table set (levels, L0 sublevels, key ranges, entry counts, delta sums) plus the value-file
  set with live bytes.
* `VersionSet` applies edits. An edit can add or remove tables or value files, record "flushed generation g, WAL
  durable through chunk c", record the next file number, record CommitUlong, the ulongs and the transaction number as
  of the flush, or record the writer epoch.
* Each edit is the object `manifest/{seq}` (create-if-absent; its sequence number is also a fence, section 14). Every
  `ManifestCheckpointInterval` edits (default 100), the writer writes a full checkpoint. Opening reads the newest
  checkpoint and the edits after it.
* A snapshot is a `Version` reference plus references to the memtable roots visible at its creation. It also includes
  CommitUlong, the ulongs and the transaction number, which live in the memtable root.
* Tables and arenas are reference counted. Releasing the last reference makes a table a deletion candidate
  (section 16).

## 12. Read path, block cache and memory budget

### 12.1 Block cache

* A sharded CLOCK cache with a byte budget (`BlockCacheSize`, default 256 MiB), keyed by `(fileNum, blockOffset)`.
  Blocks are stored decompressed in native memory from the BTDB allocator, so the GC heap and LOH do not churn.
* It holds data blocks, index and filter partitions, and value blocks. Index and filter partitions get a higher
  retention priority.
* Concurrent misses for the same block are coalesced: one GET, other readers wait.
* Entries in use by a cursor are pinned by reference count and never evicted while pinned.
* `GetNativeMemoryStats` reports cache, memtable arenas and pinned table metadata.

### 12.2 I/O

* On a point miss, one ranged GET per needed block. When several components need a block for the same seek, the GETs
  are issued in parallel and joined.
* Readahead: an iterator that crosses block boundaries in one direction grows a readahead window from 64 KiB up to
  2 MiB. The window is split into requests of at most 256 KiB issued in parallel, because L0 measured a 1 MiB GET at 2–3
  times the latency of a 256 KiB one while parallel requests keep their latency. Requests run asynchronously ahead of
  the cursor; backward scans are handled the same way.
* A cursor seek that misses in several components issues all their GETs asynchronously and waits once, so one
  blocked thread covers them. The number of concurrently blocking storage reads is bounded (risk R2).
* Compaction inputs bypass the cache: sequential 4 MiB parallel GETs, as in `ReplicationFileSet`.
* A future local or secondary cache plugs in under the block cache (`IBlockSource`). It is not part of v1.

### 12.3 Illustrative memory budget

1 billion keys, 40-byte keys, 200-byte values, about 240 GB raw:

| Item | `BTreeKeyValueDB` + `AzureBlobFileCollection` | `BTDB.LSM` |
| --- | --- | --- |
| Keys and value pointers in memory | ~60+ GB | 0 (in tables) |
| Local disk copy | full database | none |
| Memtables | — | 64 MiB × (1 + 2) |
| Block cache | — | configurable, e.g. 1 GiB |
| Pinned per-table metadata | — | ~4,000 tables × ~16 KiB ≈ 64 MiB |
| Filters | — | partitions in the block cache |

The cost is latency on cache misses (section 22, R1).

## 13. Compaction

* Leveled compaction in the style of Pebble: L0 with sublevels, L1 to L6, dynamic level sizing so the bottom level holds
  about 90% of the data, and a level multiplier of 10.
* Compactions are picked by score: L0 file and sublevel count, level size over target, and value-file garbage ratio.
* The executor merges inputs with the merging iterator. It applies tombstones, drops dead Deletes (section 6.2),
  maintains fragment counts (section 6.3), writes output tables and value files, and commits one manifest edit.
* Background workers (`MaxConcurrentCompactions`, default 2). Read and write rate limits map to
  `CompactorReadBytesPerSecondLimit` and `CompactorWriteBytesPerSecondLimit`. `CompactorRamLimitInMb` bounds compaction
  buffers.
* Compactions are pure functions of immutable inputs plus parameters. This keeps remote compaction possible later.
* `Compact(ct)` runs the picker until there is nothing to do, or does one full pass. It returns true while more work
  remains.
* Write stalls: L0 sublevels above `L0StopWritesThreshold` make `StartWritingTransaction` wait asynchronously. Above
  `L0SlowdownThreshold` a warning is logged.

## 14. Writer lease, fencing and recovery

Every instance opens as a reader (section 15). The writer is the instance that currently holds the lease (decision
D3).

* **Liveness and exclusion:** a native Blob lease on `LOCK`, using the `LeaseAuthority` deadline logic (conservative
  local deadline, permanent self-fence after expiry). The writer renews the lease in the background.
* **Becoming the writer:** the first `StartWritingTransaction`, or `ILsmKeyValueDB.AcquireWriterLeaseAsync(ct)`, starts
  acquisition. While another instance holds the lease, acquisition retries with a period derived from the lease
  duration. The pending request waits asynchronously; other queued writers wait behind it. Disposing the instance or
  cancelling `AcquireWriterLeaseAsync` ends the wait. Once the lease is acquired:
  1. Fence the manifest: create the next manifest edit as an "epoch" edit with a fresh writer epoch.
  2. Catch up: load manifest edits and replay complete WAL records to the tip, exactly as `RefreshAsync` does
     (section 15), so the writer starts from the newest durable state. An incomplete trailing multi-chunk record is
     discarded.
  3. Fence the WAL: create an "epoch" marker chunk at the first free name.
  4. Restore CommitUlong, the ulongs and the transaction number from the manifest plus replayed records, then grant the
     writing transaction.

  Existing read transactions keep their snapshots. New snapshots see the caught-up state.
* **Holding the lease:** the writer keeps the lease until `Dispose`, which flushes the WAL, stops compaction and
  releases the lease, so a waiting instance takes over without waiting for expiry. An idle writer does not release it
  on its own.
* **Losing the lease:** if renewal fails or the local deadline passes, the instance fences itself permanently for that
  epoch. Commit of an active writing transaction throws. `WaitForDurabilityAsync` throws for commits that were not yet
  durable; they were never acknowledged as durable, and the new writer will not see them. The instance discards its
  non-durable memtable state, rebuilds its read state from storage and becomes a reader. A later
  `StartWritingTransaction` waits for the lease again.
* **Safety:** each writer session has a fresh epoch. It writes WAL chunks and manifest edits with create-if-absent under
  consecutive names. After the epoch edit and epoch marker exist, every write by the predecessor at those names fails
  with `Rejected`, and a predecessor that sees `Rejected` stops for good. If a predecessor chunk wins the race for a
  name, its records are legitimate committed history: the new writer replays them and fences at the next name.
  Readers, recovery and catch-up ignore a chunk whose epoch is lower than the epoch of an earlier chunk. This covers
  predecessor parts of a large commit that landed after a gap. The new writer continues after the highest existing
  name.
* Ambiguous writes are reconciled as described in section 5. The writer never re-creates a name with different content.

## 15. Readers in other processes

* An instance that does not hold the lease is a reader. With `ReadOnly = true` it never tries to acquire the lease, and
  writing transactions throw.
* Open: load the newest manifest checkpoint and the edits after it, then replay the WAL from the flushed position into
  private memtables, one per writer generation. The resulting state ends at a commit boundary (L3).
* **No automatic refresh** (decision D6). The visible state changes only when the application asks:
  * `RefreshAsync(ct)` reads manifest edits and WAL chunks after the instance's position until it finds the first
    missing name. It applies complete commit records and publishes the result as a new snapshot root. It returns the
    CommitUlong it reached. Existing transactions keep their snapshots.
  * `WaitForCommitUlongAsync(x, ct)` returns immediately if the instance's CommitUlong is already at least `x`.
    Otherwise it repeats `RefreshAsync` with `RefreshRetryInterval` (default 50 ms, growing to 1 s) until CommitUlong is
    at least `x` or `ct` is cancelled. This relies on the application's CommitUlong being non-decreasing (event IDs).
  * In the writer process, both return immediately: the writer is always at the tip.
* A manifest edit "generation g flushed to files F" atomically swaps the reader's generation-g memtable for those files
  in a new `Version`. Compaction edits change only the table set. Reader memory is therefore bounded by the same
  memtable limits as the writer.
* **Catching up after a long pause:** if the next WAL chunk the reader needs was already deleted because a flush covers
  it, the reader continues from the manifest. If manifest edits were folded into a newer checkpoint and deleted, it
  reloads that checkpoint and rebuilds its `Version` and memtables from scratch. In both cases the cost is bounded by
  one checkpoint plus at most `MaxImmutableMemtables + 1` generations of WAL.
* Freshness after a refresh is the writer flush interval plus upload latency plus one round of GETs: tens of
  milliseconds at p50. Phase L5 measures it.
* A reader survives writer takeover: an epoch marker is just another chunk. Records after it come from the new writer.

## 16. Retention and deletion

* **Writer-side candidates:** tables and value files that are not in the current version and not referenced by any
  in-process snapshot, and WAL chunks covered by a flushed generation.
* **Reader registry (opt-in, `RegisterReader = true`):** a reader writes `readers/{readerId}` on open, on each
  refresh and every `ReaderHeartbeatInterval` (default 10 s). The object holds the oldest manifest sequence and WAL
  chunk its live snapshots still need, and an expiry. The heartbeat only extends retention; it never changes the visible
  state. Overwriting it is the only mutable write in the design, and it touches only the reader's own key.
* The writer deletes a candidate only when it is older than `DeletionGracePeriod` (default 10 min), every live registry
  entry has moved past it, and expired entries are ignored. An unregistered reader is protected only by the grace
  period.
* A reader that gets a 404 for a table first reloads the manifest. If its snapshot really needs a deleted object, it
  throws "snapshot too old" (L7). An unregistered reader that keeps a transaction open longer than the grace period can
  hit this (documented limit).

## 17. `IKeyValueDB` mapping

| Member | Behavior in `BTDB.LSM` |
| --- | --- |
| `DurableTransactions` | Section 8.2. |
| `StartTransaction`, `StartReadOnlyTransaction`, `StartWritingTransaction` | As in `BTreeKeyValueDB`, plus waiting for the writer lease (sections 7.2 and 14). |
| `inBatch`, `FinishTransactionBatchAfterCurrentTransaction` | `inBatch` is ignored and finishing a batch does nothing (decision D4). |
| `CalcStats` | Levels, tables, sizes, cache hit rate, memtables, WAL lag, compaction debt, lease state, reader staleness. |
| `GetNativeMemoryStats` | Native allocator totals (section 12.1). |
| `Compact(ct)` | Section 13. |
| `CreateKvi(ct)` | Flushes the memtable and writes a manifest checkpoint, so the next open does not need to replay the WAL. |
| `PreserveHistoryUpToCommitUlong` | Not supported and not planned (decision D5). The getter returns null; setting a non-null value throws `NotSupportedException`. `LsmKeyValueDBOptions` has no `OpenUpToCommitUlong`. |
| `Logger` | `IKeyValueDBLogger`: `TransactionLogCreated` maps to a WAL generation, `KeyValueIndexCreated` to a checkpoint, `FileMarkedForDelete` to deletion, and compaction callbacks to compaction. |
| `CompactorRamLimitInMb`, compactor rate limits | Section 13. |
| `MaxTrLogFileSize` | Maps to `MemtableSize` (WAL bytes per generation). |
| `Transactions()` | Live transactions, for leak diagnostics. |
| `GetKeyValueCount` | O(1) delta total (section 6.2). |
| `GetUlong` / `SetUlong` / `CommitUlong` / `NextCommitTemporaryCloseTransactionLog` | Ulongs live in the memtable root and the WAL record. `NextCommitTemporaryCloseTransactionLog` makes the next commit durable before `Commit()` returns. |
| `CalcBTreeStats` | Returns level and table shape in the same dictionary type: depth = level, children = tables. |
| Cursor `GetStorageSizeOfCurrentKey` | Key length and stored (compressed) value length. |
| Cursor `IsValueCorrupted` | True if the value file is missing or the block checksum fails. |
| Cursor `FastIterate` / `FastIterateNoCursor` | Merging iterator with the cached index (section 6.5). |

`LsmKeyValueDBOptions` contains: storage, prefix, `MemtableSize`, `MaxImmutableMemtables`, `BlockSize`,
`BlockCacheSize`, `TargetFileSize`, `ValueSeparationThreshold`, `BloomBitsPerKey`, compression, the WAL options, the
compaction limits, `MaxTransactionMemory`, lease duration, `ReadOnly`, the refresh and reader registry options,
`DeletionGracePeriod`, `Logger`, `TimeProvider` and the allocator. Time and
storage are always injected, for deterministic tests.

`ILsmKeyValueDB : IKeyValueDB` adds:

```csharp
public interface ILsmKeyValueDB : IKeyValueDB
{
    ValueTask FlushAsync(CancellationToken ct = default);
    ValueTask WaitForDurabilityAsync(long? transactionNumber = null, CancellationToken ct = default);
    ValueTask AcquireWriterLeaseAsync(CancellationToken ct = default);
    bool HoldsWriterLease { get; }
    ValueTask<ulong> RefreshAsync(CancellationToken ct = default);
    ValueTask WaitForCommitUlongAsync(ulong commitUlong, CancellationToken ct = default);
    LsmStatistics GetStatistics();
}
```

## 18. Observability

Counters for:

* GETs (count, bytes, latency histogram, by purpose: point, scan, readahead, compaction), cache hits and misses.
* Bloom filter usefulness.
* WAL upload latency, size and queue length.
* Flush and compaction duration and bytes.
* Stalls and their reasons.
* Lease state and transitions, refresh duration and the number of chunks applied, reader staleness, and the reader
  registry.

All of these are exposed through `GetStatistics()` and summarized by `CalcStats()`.

## 19. Test strategy

1. **Contract:** `LsmKeyValueDBTest : KeyValueDBTestBase` on `InMemoryLsmStorage`, in several configurations:
   * defaults;
   * tiny memtable (4 KiB), tiny blocks (256 B) and an aggressive compaction trigger, so every test exercises flush,
     compaction, tombstones and multi-level rank;
   * value separation forced on (threshold 0).
2. **Oracle model test:** seeded random operation sequences (every cursor API, `EraseUpTo`, `UpdateKeySuffix`, large
   transactions, rollbacks, several cursors, snapshots) run against `InMemoryKeyValueDB`. After each step, compare keys, values,
   `GetKeyIndex`, `FindKeyIndex`, counts and ulongs. A component-level variant checks rank and select directly against
   a sorted list while compaction changes the shape.
3. **Format:** round-trip, corruption detection (bit flips, truncation), oversized keys, and golden files for format
   stability.
4. **Deterministic simulation:** a copied or extracted `SimulatedBlobStore` (dispatch, effect and response separated,
   lost responses, timeouts, delays) plus a virtual clock. Covered: crash at every storage operation and recover;
   ambiguous WAL and manifest writes; writer takeover with a delayed zombie, including large-commit parts landing after
   a gap; lease loss with non-durable commits; a reader refreshing during takeover; catch-up after the needed WAL and
   manifest objects were deleted; deletion versus a reader snapshot.
5. **ObjectDB:** add a factory hook to `ObjectDbTestBase` (and to the `ObjectDBTest` setup) and run a representative
   subset (relations, secondary keys, ODB dictionary, enumerator positions, sorted queries) on the LSM.
6. **Processes:** a writer subprocess plus a reader subprocess (`WaitForCommitUlongAsync`), and a second
   writer subprocess waiting in `StartWritingTransaction` that takes over when the first is killed or disposed, following the `BTDB.Replication.Process.Test`
   pattern, with consistency and freshness assertions.
7. **Azure:** adapter conformance on Azurite, plus the live suite through `BTDB_AZURE_BLOB_ENDPOINT`. Live runs happen
   from an Azure VM with managed identity; internal subscription names stay out of the public repo.

## 20. Phases

Each phase ends with green tests and a CHANGELOG entry where it changes public behavior.

| Phase | Content | Exit criteria |
| --- | --- | --- |
| **L0 Measure and de-risk** | Azure probes (extend `DBBenchmark eventlog-storage`): ranged GET p50/p99 for 4 KiB–4 MiB, 404 probe, Get Properties, List, parallel GET throughput, sync versus async SDK read, conditional Put Blob for WAL-sized chunks. Prototype counted components with rank and select (sections 6.2–6.5) and an oracle test. | Measured numbers recorded in `BTDB.LSM/Measurements.md`; `BlockSize`, `WalChunkTargetSize` and the readahead defaults chosen; rank and select prototype passes 10⁶ random operations. |
| **L1 Core engine (in-memory storage)** — done, see 20.1 | Projects, `ILsmStorage` and in-memory storage, block and table format, bloom filter, memtable (managed prototype), merging iterator, rank and select, write transactions, writer queue, cursors, flush to L0, manifest, versions, snapshots, block cache. | The `KeyValueDBTestBase` suite passes in all configurations; the oracle test passes. |
| **L2 Compaction and values** — done, see 20.1 | Leveled compaction, L0 sublevels, range tombstones with counts, value separation, value-file GC, native memtable, `MaxTransactionMemory`, stalls, rate limits. | The tiny-config suites pass with continuous compaction; long oracle runs pass; memory stays within the budget under a stress test. |
| **L3 Durability and lease** — done, see 20.2 | WAL chunks, multi-chunk commits, group commit, `WaitForDurabilityAsync`, lease wait in `StartWritingTransaction`, reader-to-writer transition, lease loss, epoch fencing, ambiguity reconciliation, deferred deletion; measure whether early upload of large transactions is needed. | The crash-at-every-operation, zombie-writer and lease-loss simulations pass. |
| **L4 Azure** — done, see 20.4 | `BTDB.LSM.Azure` adapter (sync and async ranged reads, staged uploads, lease), Azurite and live tests. | The conformance suite passes on Azurite and live Azure; first live latency and throughput numbers recorded. |
| **L5 Readers in other processes** — done, see 20.3; Azure freshness measured in L4 | `RefreshAsync`, `WaitForCommitUlongAsync`, generation swap, catch-up after deletion, reader registry, freshness measurement. | Reader simulation and process tests pass; refresh latency and freshness p50/p99 measured on Azure. |
| **L6 Integration and release** — done, see 20.4 and 20.5; report reviewed, preview published with 35.11.3 | ObjectDB subset on the LSM, benchmarks against `BTreeKeyValueDB` + `AzureBlobFileCollection` (memory, open time, cold and warm point reads, scans, write throughput, cost per operation), parameter tuning, `Doc/LsmKeyValueDB.md`, README pointer, preview packages. | The benchmark report is reviewed; the preview release is published. |

### 20.1 Phase L1 state and differences from the target design

Implemented in `BTDB.LSM`:

* `ILsmStorage` and `InMemoryLsmStorage`.
* The table format: prefix-compressed blocks with restart points and delta sums, per-block running-delta extremes in
  the index, bloom filter, tombstones, properties, footer and XxHash64 checksums.
* A byte-budgeted block cache.
* The counted copy-on-write memtable.
* Rank, index-only rank bounds, select and masked seeks over the component stack (`LsmView`).
* The count-preserving merge (`LsmMerger`).
* The writer queue and transactions.
* Key-based cursors.
* Flush to tables, the manifest, reopen, and deferred deletion of merged tables once no snapshot uses them.

Done in L2:

* **Leveled compaction** (`LsmVersion`, `LevelComponent`, `LsmCompaction`):
  * L0 tables are separate components; levels 1..n are non-overlapping tables presented as one component each, with
    prefix sums over tables for rank and blocks.
  * The picker scores L0 by table count and the other levels by size against `LevelBaseBytes × LevelMultiplier^(n-1)`.
    A level picks its table round-robin.
  * Single-table moves without overlaps are trivial moves.
  * Outputs are split at `TargetFileSize` into non-overlapping tables. Tombstones crossing a split are clipped, and each
    part gets its own `CoveredBelow` from the levels below.
  * `Compact()` merges everything into the bottom level.
* **Positioned merging iterator** (`MergingIterator`, `TableCursor`, `LevelCursor`): children stay positioned, a step
  advances only the children at the current key, a masking fragment moves older children past it in one seek, and a
  direction change reseeks. Cursors keep their iterator while the transaction is unmodified.
* **Value separation** (`ValueFile`, `ValueSeparation`):
  * Values longer than `ValueSeparationThreshold` move to value files at flush. Tables store a handle (file, offset,
    length) and record the bytes they reference per value file.
  * Compaction copies handles.
  * When a value file's live ratio is below `ValueGcThreshold`, compactions rewrite its values, and a rewrite compaction
    is picked for any table still referencing it.
  * Value files no table references are deleted once no snapshot can read them.
  * Cursors read separated values only on `GetValue*`.
* **Native memtable** (`NativeMemtable`, `NativeArena`):
  * A counted copy-on-write B+tree (32 entries per node) in a native arena shared by all versions of a memtable.
  * The arena is released when the memtable has been flushed and no snapshot uses it.
  * The managed treap remains as a reference implementation (`NativeMemtable = false`) and as the oracle in
    `NativeMemtableTest`.
  * Measurements: [Measurements.md](Measurements.md), section 3.
* **Partitioned index and filters:** tables pin only a top-level index with one entry per partition (first key, first
  block, index and filter handles). Index partitions (`IndexPartitionSize`, default 16 KiB) and their bloom filter
  partitions are read through a managed LRU that gets one eighth of the cache budget.
* **Native block cache** (`LsmBlockCache`, `NativeClockCache`, `RawBlock`):
  * Data blocks and separated values are kept raw (decompressed) in native memory in a sharded CLOCK cache. An entry
    is pinned only during one call.
  * Lookups binary-search the restart keys stored whole in the block and decode one restart interval. Table cursors
    step forward by remembered offsets.
  * Compaction and flush inputs are read sequentially in ranges of up to 4 MiB, bypassing the data cache.
* **Background work:**
  * A commit that fills the memtable seals it (`SealedMemtable`, with its commit metadata). A single worker flushes
    sealed memtables oldest first, runs full compactions requested by `Compact`, and otherwise runs picked compactions.
  * Writers stall in `StartWritingTransaction` while `MaxImmutableMemtables` sealed memtables are pending or level 0
    has `Level0StopWritesTrigger` tables.
  * The worker retries failed units with backoff and reports them through `ReportCompactorException`.
  * `CompactorWriteBytesPerSecondLimit` paces object creation.
  * `BackgroundCompaction = false` runs the same units synchronously in the sealing commit.
  * An upgradable transaction is invalidated only by commits (`CommitSequence`), not by background layout changes.
  * A snapshot's state is captured and registered atomically with the deletion decision. The concurrency test
    found the race when this was not so.

Remaining differences from the target design:

| Current state | Target |
| --- | --- |
| Compactions run one at a time. Read and write pacing are enforced (section 20.6). | `MaxConcurrentCompactions`, if measurements on Azure show the need. |
| The native arena counts copy-on-write garbage, so with small random transactions it fills about 3 times faster than its entry bytes. | Optional rebuild of the live tree into a fresh arena, if a real workload shows the flush pressure matters. |

The other L1 differences, full manifest checkpoints without a WAL and the missing lease, are closed by L3
(section 20.2). Manifests stay full checkpoints by decision (section 20.2).

Cursor semantics. `LsmKeyValueDB` cursors are positioned by key. Where `BTreeKeyValueDB` reports a cursor disturbed by
another cursor's write as invalid and refuses to read it, the LSM cursor re-seeks by its key and stays readable.

After its key is erased, a cursor is fuzzy invalid:

* its index is the rank of the key minus one;
* next and previous move to the neighbors of the key;
* creating the key again makes it current again.

These rules match the key-based behavior that the shared suite specifies. Cursors on a key renamed by
`UpdateKeySuffix` follow the new key.

`BTreeKeyValueDB` defects found by the oracle test (the oracle works around them; they are not fixed by this work):

* **Lost write.** In one transaction, cursor A is on key k1 and cursor B on key k2 in the same leaf. After
  `B.SetValue(v)`, `A.EraseCurrent()` restores k2's old value. Reproduced in both `StartTransaction` and
  `StartWritingTransaction`.
* `CreateOrUpdateKeyValue` keeps a stale "removed current" flag, so a following `GetKeyIndex` returns one less.
* After `UpdateKeySuffix` returns `NotFound` or `NotUniquePrefix`, the cursor keeps a stale key index.
* A cursor disturbed by several writes of other cursors can track a key index that differs from a fresh `Find` of the
  same key.

### 20.2 Phase L3 state

Implemented (`LsmKeyValueDB.Wal.cs`, `LsmKeyValueDB.Lease.cs`, `LsmKeyValueDB.Recovery.cs`, `Engine/Wal.cs`):

* **Commit records** log the memtable operations of a transaction in order (insert with `existsBelow`, update,
  remove, range remove, tombstone set) plus CommitUlong, the ulongs and the transaction number. Replay applies them to
  the memtable of the same generation without reading tables.
* **WAL chunks** `wal/{n}`:
  * Each chunk carries epoch, memtable generation, an epoch-marker flag, record parts and a checksum.
  * The uploader takes all pending records after `WalFlushInterval`, encodes them into chunks of about
    `WalChunkTargetSize` (one generation per chunk) and creates up to `WalUploadParallelism` chunks at once.
  * A failed batch keeps its chunk numbers and bytes, so a retry recreates exactly the missing chunks.
  * Durability advances only when a whole batch is created.
* **Durability API:**
  * `DurableTransactions` and `NextCommitTemporaryCloseTransactionLog` make `Commit` wait.
  * `WaitForDurabilityAsync(transactionNumber)` waits for one commit. Omitting the number or passing null, like
    `FlushAsync`, waits for everything committed at the time of the call; rollback does not write a WAL record.
  * `CreateKvi` seals and flushes the memtable to tables (a checkpoint).
* **Manifest** records the writer epoch, the newest flushed generation and the first WAL chunk recovery needs. Chunks
  before it are deleted after the manifest is written.
* **Recovery** loads the newest manifest, then replays the contiguous WAL prefix. A chunk with a lower epoch than an
  earlier chunk is skipped, an incomplete record is dropped, and each unflushed generation becomes a memtable. The
  simulation found that comparing chunk epochs with the manifest's epoch skipped a predecessor's legitimate chunks.
* **Writer lease:**
  * Every instance opens as a reader; `AcquireWriterLeaseOnOpen` or the first `StartWritingTransaction` acquires the
    lease.
  * Taking over: reload, adopt orphans, take epoch + 1, write a manifest (the manifest fence) and an epoch marker at
    the first free WAL name (the WAL fence), then start the WAL uploader, compaction worker and lease renewal.
  * A writer renews its lease before writing when less than two thirds remain, and it never writes after its local
    deadline (lease duration minus `LeaseSafetyMargin` from the request).
  * A rejected or foreign object at a writer's name fences it. It fails pending durability waits, reloads from storage
    and continues as a reader.
  * An upgradable `StartTransaction` can write only on the writer.
* **Ambiguous writes:** a lost or ambiguous create is reconciled by reading the object back. The same bytes mean the
  write landed, a missing object is retried, and other bytes fence the writer.
* **Orphans:** objects a crashed writer created but never published are found by listing `tables/` and `values/` at
  takeover. File numbers continue after them (the simulation found reuse), and the orphans are deleted.

Not done yet, admitted only with evidence:

| Item | Reason |
| --- | --- |
| Incremental manifest edits | Not admitted: a full manifest costs about 8 bytes per table, 81 KB at 625 GiB, which is small next to the table upload of each publication (Measurements.md, section 6). |
| Early upload of large transactions | No workload measurement yet (section 7.5). `MaxTransactionMemory` is done (section 20.6). |

### 20.3 Phase L5 state

Implemented (`LsmKeyValueDB.Reader.cs`, `WalReplay` in `LsmKeyValueDB.Recovery.cs`):

* **`RefreshAsync`** works incrementally:
  * It probes `manifest/{n+1}…` for newer manifests. When a probe misses, it lists manifests at most every
    `ManifestListInterval`. If a probed manifest's tables are already deleted, it falls back to the newest listed one.
  * A newer manifest rebuilds the version, reusing open table readers, and drops replayed memtables of flushed
    generations.
  * The reader then reads `wal/{next}…` until the first missing chunk and replays it into a copy of the memtable, so
    snapshots of the previous round never change.
  * Replaced memtable arenas are released when no snapshot uses them.
* **`WaitForCommitUlongAsync`** refreshes until CommitUlong reaches the target, pausing `RefreshRetryInterval` and
  doubling up to one second. The writer is always current.
* **Retention:**
  * The writer deletes tables, value files, old manifests and orphans only after `DeletionGracePeriod` (default 10
    min) and only if no in-process snapshot uses them.
  * With `RegisterReader`, a reader writes `readers/{id}`: the oldest manifest its snapshots use, and an expiry. It
    registers with 0 before loading anything and updates the entry on every refresh and heartbeat. The writer keeps
    any object obsolete since a manifest newer than a live entry, and it uses only a registry read made after the
    object became obsolete. The test found the stale-cache race this rule closes.
  * Covered WAL chunks are deleted at once: a reader that misses one reads the newer manifest instead.
* **Errors:** a deleted table surfaces as `BTDBException` "no longer exists: the snapshot is older than the deletion
  grace period"; `ILsmStorage.ReadRange` throws `FileNotFoundException` for a missing object.

Refresh freshness on Azure is measured in [Measurements.md](Measurements.md), section 4.

### 20.4 Phases L4 and L6 state

Implemented:

* **Azure adapter** (`BTDB.LSM.Azure/AzureLsmStorage.cs`):
  * Creates use `If-None-Match: *`, as one Put Blob up to 4 MiB or as staged blocks committed with the same
    condition. Transient failures after dispatch are reported as `Ambiguous`.
  * Ranged reads are synchronous (the SDK's sync pipeline) and asynchronous, with bounded retries.
  * The writer lease is a blob lease, clamped to Azure's 15–60 s.
  * The suite passes on Azurite and on live Premium storage.
* **ObjectDB on the LSM.** `ObjectDbTestBase.CreateKeyValueDB` is a factory hook. Seven `BTDBTest` ObjectDB suites
  (197 tests: tables, scans, relation queries, in-key values, update by id, secondary key buffers, tuples) run on
  `LsmKeyValueDB` with tiny settings (`BTDB.LSM.Test/Api/LsmObjectDbTest.cs`). They run serially, because ObjectDB code
  generation shares static state; `BTDBTest` disables parallelization for the same reason.
* **Readahead** (`TableCursor`, `TableReader.Readahead`), as section 12.2 specifies:
  * from the second consecutive block crossing in one direction, a window grows from 64 KiB to 2 MiB;
  * it is issued as parallel asynchronous requests of at most 256 KiB, and the decoded blocks go to the cache
    unreferenced;
  * a cursor that needs a block still in flight waits for it instead of reading it again, and a failed readahead
    falls back to the cursor's own read.
* **Select** bisects a second time over restart keys inside the last blocks (`ILsmComponent.AddFinePivots`). The
  final linear step is at most about one restart interval.
* **Bloom filters** default to 16 bits per key. Profiling the random-key bulk load (Measurements.md, section 5)
  showed that bloom false positives, not compaction, made it 4–5 times slower than `BTreeKeyValueDB`; at 16 bits it
  reaches parity in simulation. `CalcStats` reports flush, compaction and write stall times.
* **Oracle.** The workarounds for the `BTreeKeyValueDB` defects the oracle found are removed now that they are fixed.
  Only the documented difference in how a disturbed cursor reports validity remains.
* **Documentation and packaging:**
  * `Doc/LsmKeyValueDB.md`, a README pointer, and package READMEs;
  * both projects pack as `-preview` with symbols;
  * `Releaser` treats them as preview packages.

Added after the qualification:

* **Format tests** (`BTDB.LSM.Test/Format/LsmFormatTest.cs`, section 19.3):
  * Golden files pin the bytes of a deterministic sample database, which has tables on several levels, value files, a
    manifest and a WAL chunk with a commit.
  * Every object of the sample is bit-flipped and truncated. Damage is reported as `BTDBException`, or it touches only
    dead values in a value file.
  * The tests found that truncated objects surfaced as storage exceptions. `ILsmStorage` now throws
    `EndOfStreamException` for reads past the end, and the engine reports a truncated object.
* **`FileSystemLsmStorage`:**
  * a directory store for development, tests across processes and benchmarks;
  * creates are atomic create-if-absent, by `link(2)` on Unix and by a move without replace on Windows;
  * leases are records under a short exclusive file lock.
* **Process tests** (`BTDB.LSM.Process.Test`, section 19.6), with writers and a reader in separate processes:
  * the reader follows the writer through flushes and compactions;
  * a waiting writer takes over from a killed one, and every commit the first writer reported durable survives;
  * a disposed writer hands the lease over at once.
* **`ObjectDbTest`** gets the same factory hook as `ObjectDbTestBase`. 334 ObjectDB tests run on the LSM.
* **Parallel component reads** (section 12.2): seeks and ranks start the block reads of all stored components first.
  This makes a cold `FindKeyIndex` 25% faster in simulation (Measurements.md, section 6).

Findings that remain open, each to be admitted only with evidence (Measurements.md, sections 4–6):

| Item | Evidence so far |
| --- | --- |
| Updates of uncached existing keys cost one GET each | Inherent to `CreateOrUpdateKeyValue`; a batched existence prefetch API would need an application workload. |
| Durable commit p95 of 22 ms on Premium | One run; LRS had 11 ms. |
| Readahead across table boundaries in a level | Tables are 64 MiB by default, so a scan restarts the window rarely. |
| Bounding concurrent blocking reads (R2) | Not exercised by the single-cursor benchmark. |
| Prefetching index and filter partitions with the data blocks | They load synchronously before the parallel data reads; this matters only when the cache cannot hold them. |
| Hedged reads on Standard storage | Section 2 risk R1; needs an end-to-end measurement on Standard storage. |

### 20.5 Code review fixes

An independent review (2026-10-16) found these bugs. `LsmRegressionTest` reproduced the first three and the readahead
hang before the fixes:

* **Lost durable commits.** A checkpoint during a running flush sealed an empty memtable; the flush then set the WAL
  start past later commits and deleted their chunks.
  * Empty memtables are no longer sealed.
  * The WAL start is the first chunk of any later generation.
  * Chunk numbers change only under the WAL lock.
* **Double counting.** Sealing a memtable kept the commit sequence, so a transaction opened before a checkpoint
  upgraded to writing on the sealed memtable.
* **Mixed databases.** A shared block cache keyed entries by file number only. Entries are now keyed by database scope
  (storage object and prefix) too.
* **Refresh during a mode change.** A refresh could run while the instance became or stopped being the writer. It
  could overwrite the writer's state or publish a released arena.
* **Deleted live table.** Orphans found by a failed takeover attempt could later delete a live table. Deletion now
  also skips any file the current version references.
* **Readahead hang.** A failed index load during readahead left reads waiting forever.
* **Unfenced writes.** Lease checks were skipped during takeover and after the lease was lost. Every object write now
  requires the lease, from acquisition on.
* **Smaller fixes:**
  * Azure lease durations it cannot honor are rejected.
  * Compactions pending when the lease is lost fail instead of waiting forever.
  * `WorkerFailureTimeout` bounds `CreateKvi` and `Compact` waits.
  * Creates retry with backoff.
  * Deletion moved off `Transaction.Dispose`, and failed deletes retry.
  * File storage reads allow concurrent deletes.

Performance fixes are in Measurements.md, section 7. Simplifications:

* the counted prototype moved to the test project;
* unused merge and tombstone helpers were removed;
* one stall loop now serves both the async and the sync caller;
* section 8.2 now describes the WAL uploader as built.

### 20.6 After the preview release

The two v1 items the phases left open are done:

* **`MaxTransactionMemory`** (section 7.5, default 256 MiB) bounds the memtable bytes one writing transaction adds.
  It is checked before every modification and at commit, so one modification may exceed it but no commit does. The
  transaction then can only be rolled back. `LsmLimitsTest` covers both memtables.
* **`CompactorReadBytesPerSecondLimit`** paces the sequential reads of compaction inputs, as
  `CompactorWriteBytesPerSecondLimit` paces object creation. Index and filter reads and separated values read for
  value-file GC go through the cache and are not paced.

## 21. Decisions

Confirmed on 2026-10-15.

| ID | Question | Decision |
| --- | --- | --- |
| D1 | One package, or `BTDB.LSM` plus `BTDB.LSM.Azure`? | Split. The core has no Azure dependency. |
| D2 | Default durability mode? | `DurableTransactions = false` with group commit; `WaitForDurabilityAsync` for durable commits (section 8.2). |
| D3 | How does the writer start and fail over? | Starting a writing transaction waits for the writer lease. Every instance is a reader until then (section 14). |
| D4 | Transaction batching? | Not supported: `inBatch` is ignored. Applications use larger transactions instead (section 7.5). |
| D5 | `PreserveHistoryUpToCommitUlong` / `OpenUpToCommitUlong`? | Not needed at all. |
| D6 | How fresh are readers in other processes? | No automatic refresh. `WaitForCommitUlongAsync` (same or higher CommitUlong) and `RefreshAsync` catch up on demand (section 15). |
| D7 | `InternalsVisibleTo("BTDB.LSM")` in `BTDB`? | Yes. |

## 22. Risks

| ID | Risk | Mitigation |
| --- | --- | --- |
| R1 | Cache-miss latency. A cold point read costs a GET per component. Measured for 32 KiB: Standard 8–10 ms p50, ~40 ms p95, 65–80 ms p99; Premium 5 / 6.5 / 11 ms. ODB transactions doing many random reads become slow when the working set exceeds the cache. | Parallel component GETs, bloom filters, pinned top indexes, readahead. Recommend Premium block blob for latency-sensitive databases. Hedged reads are a candidate for Standard. Measure end to end in L4 and L6. A secondary cache can be added later behind `IBlockSource`. |
| R2 | The synchronous cursor API blocks threads on GETs. L0 confirmed the risk: at 64 or more concurrent thread-pool work items, throughput collapses to about 400 and then about 40 requests/s while the pool grows. | Bound concurrent blocking reads; issue one seek's component GETs asynchronously and wait once; document `ThreadPool.SetMinThreads` for hosts with many concurrent cursors. |
| R3 | Rank and select with tombstones and many components are complex and could be slow. | Prototype in L0 against an oracle, point Deletes for small ranges, compaction priority for tables with tombstones, a component count limit through L0 stalls. |
| R4 | `existsBelow` lookups make blind writes read first. | Bloom filters; an ODB write usually follows a read of the same key (cached). Measure with ODB benchmarks. |
| R5 | Large transactions are the expected workload. A transaction above `MaxTransactionMemory` fails, and memtable memory peaks at `MemtableSize + MaxTransactionMemory`. | Configurable limit; multi-chunk commits with parallel upload; optional early upload (section 7.5); spill to storage if a workload needs more. |
| R6 | Object storage cost (GET and PUT transactions) and throttling (about 3,000 requests/s per blob, account limits). | Large tables and blocks, coalesced readahead, group commit, cost per operation in the L6 report. |
| R7 | Reader snapshots in other processes versus deletion. | Reader registry plus grace period, explicit "snapshot too old" error, simulation tests. |
| R8 | Format evolution. | Version fields in footer, chunk and edit headers from day one; golden-file tests. |
