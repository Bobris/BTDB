# LsmKeyValueDB (BTDB.LSM)

`LsmKeyValueDB` is an `IKeyValueDB` implementation that stores a log-structured merge tree directly in object storage.
It is inspired by Pebble (CockroachDB). Use it when the database is larger than you want to keep in memory or on a
local disk, or when other processes need to read the database while one process writes it.

| Package | Content |
| --- | --- |
| `BTDB.LSM` | The engine, `ILsmStorage`, and in-memory storage for tests. Depends only on `BTDB`. |
| `BTDB.LSM.Azure` | `AzureLsmStorage`, an Azure Blob Storage adapter. |

The storage format can still change between versions.

## Compared with BTreeKeyValueDB

| | `BTreeKeyValueDB` + `AzureBlobFileCollection` | `LsmKeyValueDB` + `AzureLsmStorage` |
| --- | --- | --- |
| Memory | Every key plus a value pointer stays in memory. | Bounded by the block cache, the memtables and small per-table metadata. Does not grow with the database. |
| Local disk | A full local copy of every file. | None. Tables are read with ranged GETs. |
| Open time | Loads the newest KVI and replays the transaction log. | Reads the manifest and table footers, then replays the unflushed WAL. |
| Readers in other processes | Not supported. | Any number; they catch up on demand. |
| Writer failover | Application-specific. | Built in: the next instance that starts a writing transaction takes over the lease. |
| Point read latency | About 1 µs. | About 2.5 µs on a cache hit; one ranged GET on a miss (3–5 ms p50, about 10 ms p99 on Azure). |

The whole `IKeyValueDB` contract is supported, including O(log n) `GetKeyIndex` and `FindKeyIndex`, O(1)
`GetKeyValueCount`, exact `EraseUpTo` counts and cursor stability. `ObjectDB` and relations run on it unchanged.

Not supported:

* Transaction batching (`inBatch` is ignored).
* `PreserveHistoryUpToCommitUlong` and `OpenUpToCommitUlong`.
* Several writers at once, or writes from a process that does not hold the writer lease.
* A local disk cache.

## Opening

```csharp
using Azure.Core;
using Azure.Storage.Blobs;
using BTDB.LSM;
using BTDB.LSM.Azure;
using BTDB.ODBLayer;

var container = new BlobContainerClient(new Uri("https://account.blob.core.windows.net/my-db"), credential,
    new BlobClientOptions { Retry = { MaxRetries = 0 } });
await container.CreateIfNotExistsAsync();
using var db = await LsmKeyValueDB.OpenAsync(new LsmKeyValueDBOptions
{
    Storage = new AzureLsmStorage(container),
    Prefix = "orders/",
});
using var odb = new ObjectDB();
odb.Open(db, false);
```

Create the container client with `Retry.MaxRetries = 0`. The SDK's retry of a conditional create can report the
database's own landed write as a conflict. `AzureLsmStorage` reports transient failures as ambiguous instead, and the
database reconciles them by reading the object back. Reads are retried by the adapter.

Several databases can share one container with different `Prefix` values.

## Writer and readers

Every instance opens as a reader. An instance becomes the writer by taking the writer lease, which happens:

* when it opens with `AcquireWriterLeaseOnOpen = true`,
* in `AcquireWriterLeaseAsync`, or
* in the first `StartWritingTransaction`, which waits until the lease is free.

The writer keeps the lease until it hands the role over or is disposed:

```csharp
// On the node that wrote last: make every commit durable and let another node write at once.
await db.ReleaseWriterLeaseAsync();
```

`ReleaseWriterLeaseAsync` waits for a running writing transaction, uploads the WAL and releases the lease. The
instance continues as a reader of the same state. A later `StartWritingTransaction` takes the lease again. `Dispose`
of the writer does the same. Neither flushes memtables: the next writer flushes them.

Another instance waiting in `StartWritingTransaction` retries within milliseconds and then every `LeaseRetryInterval`
at most (250 ms by default). It continues from the state it already replayed as a reader. A takeover reads only the
newest manifest and the WAL chunks the reader has not seen yet, then writes one manifest and one WAL chunk. It sees
every commit the previous writer made durable.

The writer takes the lease over from a crashed writer once the lease expires, after `LeaseDuration` at most (15 s by
default; Azure accepts whole seconds from 15 to 60 and rejects other values). Taking over fences the previous writer:
it can no longer create objects, and if it is still alive it fails its pending durability waits and continues as a
reader.

A reader in another process sees a consistent snapshot. It does not refresh automatically:

```csharp
// Catch up to the newest durable state.
var commitUlong = await reader.RefreshAsync();

// Or wait until the writer's commit with CommitUlong >= 42 is visible.
await reader.WaitForCommitUlongAsync(42);
using var tr = reader.StartReadOnlyTransaction();
```

Transactions started before a refresh keep their snapshots.

## Durability

Commits are not durable when `Commit` returns. The WAL uploader sends them to storage in the background: a commit
wakes it, and commits that arrive during an upload are grouped into the next one. To wait:

```csharp
using var tr = await db.StartWritingTransaction();
// ... writes ...
tr.Commit();
await db.WaitForDurabilityAsync();
```

Without a transaction number (or with `null`), `WaitForDurabilityAsync` waits for the latest transaction committed at
the time of the call. It does not wait for later commits or uncommitted changes. To wait for a specific commit, capture
`tr.GetTransactionNumber()` after making changes and before `Commit`, and pass it to `WaitForDurabilityAsync`.
Cancellation can be passed as `WaitForDurabilityAsync(cancellation: cancellationToken)`.

`FlushAsync` has the same effect as `WaitForDurabilityAsync()` and waits for everything committed so far.

When several writers wait for durability, each commits again as soon as its upload completes, and without help their
commits would split into a small upload and a larger one. The uploader therefore waits briefly before an upload when
commits usually follow the previous upload closely, so they share it. It chooses the wait itself from when commits
arrive, up to `AdaptiveWalLingerMax` (3 ms by default), and keeps it at zero for one writer awaiting each commit and for
independent commits, so their latency does not grow. `AdaptiveWalLingerMax = TimeSpan.Zero` uploads at once.
`DurableTransactions = true` makes every `Commit` wait. Readers in other processes see only durable commits.

Disposing a transaction without committing rolls it back. Uncommitted changes are never queued for WAL upload, so
rollback writes no WAL record and needs no durability wait.

## Memory

Resident memory is roughly:

* `BlockCacheSize` (256 MiB by default; a native, sharded CLOCK cache that can be shared by several databases),
* the mutable memtable and up to `MaxImmutableMemtables` sealed ones (`MemtableSize`, 64 MiB by default, in native
  memory),
* a running writing transaction, which keeps at most `MaxTransactionMemory` (64 MiB by default) in memory, plus its
  WAL record until commit,
* small per-table metadata: the top-level index and table properties,
* WAL bytes waiting for upload, bounded by `MaxUnflushedWalBytes`.

Writers wait (stall) when sealed memtables, level 0 tables or WAL bytes exceed their limits, until background flush,
compaction or upload catches up.

## Large transactions

A writing transaction can be practically unlimited in size. Once it holds more than `MaxTransactionMemory` memtable
bytes, it spills:

* Its changes move into tables and value files that only it reads, and it continues with an empty memtable.
* Every four spilled parts of equal size merge into one run of non-overlapping tables. A transaction therefore keeps
  few runs, and each new key checks only a few bloom filters for whether it exists.
* Its commit writes the last part as a table and merges all runs into one. It waits until earlier memtables are
  flushed, then publishes that run as the newest level 0 run by one manifest edit. It writes no WAL record. The commit
  is durable when it returns.
* A level 0 run counts as one level 0 entry for `Level0CompactionTrigger` and `Level0StopWritesTrigger`, so the next
  writer does not stall behind a large commit.
* A rollback deletes the private tables at once.

Other transactions never see a spilled transaction before its commit. Memory stays below about
`MemtableSize + 2 * MaxTransactionMemory` whatever the size. `MaxTransactionSize` (unlimited by default) caps the
total: a transaction above it throws `BTDBException` on its next modification or commit and can only be rolled back.

Performance of a 2 GiB transaction on Azure ([Measurements](../BTDB.LSM/Measurements.md), section 8):

* Writing it took about 200 s, about 10 MiB/s, with a working set of under 2 GiB.
* Its commit took about 14 s, mostly the final merge. Another instance saw it 0.1 s later, and the next writer did
  not wait.
* Every new key checks the bloom filters of the transaction's runs. Size `BlockCacheSize` so that an eighth of it
  holds the filters of the transaction's keys: about 2 bytes per key at the default 16 bits. Raising
  `BloomBitsPerKey` without a larger cache makes large transactions much slower.

## Performance notes

Measured on Azure with 2 million keys ([Measurements](../BTDB.LSM/Measurements.md), section 4):

* A reader opens in about 30 ms with a few MiB of memory. `BTreeKeyValueDB` needed 1.2–1.7 s and a 273 MiB local copy.
* Sequential scans read ahead in parallel and reach about 0.75–1 million keys per second from a cold cache.
* `CreateOrUpdateKeyValue` must report whether the key existed. New keys are mostly answered by bloom filters, but
  updating an existing key whose block is not cached costs one ranged GET. Size `BlockCacheSize` for the working set
  you update, and prefer larger transactions.
* Inserting new keys still checks whether each key exists. Bloom filters (16 bits per key by default) answer almost
  all of these checks without a read, so a random-key bulk load runs at about the speed of `BTreeKeyValueDB`.
  Insert-heavy databases whose filters fit in an eighth of the cache can use `BloomBitsPerKey = 20`.
* Many cursors that miss the cache at once block thread-pool threads in synchronous reads. Hosts with many concurrent
  cursors should raise `ThreadPool.SetMinThreads`.

## Main options

| Option | Default | Meaning |
| --- | --- | --- |
| `MemtableSize` | 64 MiB | Memtable size that triggers a flush to a level 0 table. |
| `MaxTransactionMemory` | 64 MiB | Memtable bytes a writing transaction keeps in memory before it spills into tables. |
| `MaxTransactionSize` | unlimited | Most memtable bytes one writing transaction may add, in memory and spilled. |
| `BlockSize` | 32 KiB | Target uncompressed data block size; one cache miss reads one block. |
| `BlockCacheSize` / `BlockCache` | 256 MiB | Size of the block cache the database creates, or a shared cache. |
| `BloomBitsPerKey` | 16 | Bloom filter size; a false positive costs one ranged GET when a key is written or looked up. |
| `ValueSeparationThreshold` | 1 KiB | Longer values go to shared value files at flush, so compaction does not rewrite them. |
| `LevelBaseBytes`, `LevelMultiplier`, `TargetFileSize` | 256 MiB, 10, 64 MiB | Leveled compaction shape. |
| `LeaseDuration` | 15 s | Writer lease duration; also the longest failover wait. |
| `LeaseRetryInterval` | 250 ms | Longest pause between attempts to take a lease another instance holds. |
| `WorkerFailureTimeout` | 1 min | `CreateKvi` and `Compact` fail when flushes and compactions keep failing this long. |
| `WalChunkTargetSize` | 256 KiB | Size of one WAL object; larger commits span several. |
| `AdaptiveWalLingerMax` | 3 ms | Longest wait before a WAL upload for commits that answer the previous one; zero disables it. |
| `DeletionGracePeriod` | 1 day | Obsolete objects are kept this long for readers in other processes. |

`CompactorReadBytesPerSecondLimit` and `CompactorWriteBytesPerSecondLimit` on the database (0, no limit, by default)
pace the reads of compaction inputs and the creation of tables, value files and manifests.

A transaction in another process may keep its snapshot for at most `DeletionGracePeriod`. An older snapshot fails with
`BTDBException` ("no longer exists") once it needs a deleted object. It never reads a partial state.

The manifest records when each object became obsolete, so a new writer keeps counting from that time. Replaced manifests
are kept for at most one minute: snapshots do not read them.

## Object layout

| Key under `Prefix` | Content |
| --- | --- |
| `LOCK` | Writer lease. |
| `manifest/{n}` | The table set, the writer epoch and where WAL replay starts. |
| `wal/{n}` | WAL chunks with commit records. |
| `tables/{n}.sst` | Sorted tables with partitioned index and bloom filters. |
| `values/{n}.vlf` | Separated values. |

Every object is written once with create-if-absent and never modified.

## Other storage

`ILsmStorage` is provider-neutral: create-if-absent, read, ranged read (synchronous and asynchronous), list, delete
and a lease.

* `InMemoryLsmStorage` is for tests.
* `FileSystemLsmStorage` stores a database in a local directory that processes on one machine can share. It is meant
  for development, tests and benchmarks: it does not fsync.
* Only Azure is qualified for production.

## Design and measurements

* [Implementation plan and design](../BTDB.LSM/ImplementationPlan.md)
* [Measurements](../BTDB.LSM/Measurements.md)
