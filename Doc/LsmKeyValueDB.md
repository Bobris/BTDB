# LsmKeyValueDB (BTDB.LSM, preview)

`LsmKeyValueDB` is an `IKeyValueDB` implementation that stores a log-structured merge tree directly in object storage.
It is inspired by Pebble (CockroachDB). Use it when the database is larger than you want to keep in memory or on a
local disk, or when other processes need to read the database while one process writes it.

| Package | Content |
| --- | --- |
| `BTDB.LSM` | The engine, `ILsmStorage`, and in-memory storage for tests. Depends only on `BTDB`. |
| `BTDB.LSM.Azure` | `AzureLsmStorage`, an Azure Blob Storage adapter. |

Both packages are previews: the storage format can still change between versions.

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

The writer takes the lease over from a crashed writer once the lease expires, after `LeaseDuration` at most (15 s by
default; Azure allows 15 to 60 s). Taking over fences the previous writer: it can no longer create objects, and if it
is still alive it fails its pending durability waits and continues as a reader.

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
var transactionNumber = tr.GetTransactionNumber();
tr.Commit();
await db.WaitForDurabilityAsync(transactionNumber);
```

`FlushAsync` waits for everything committed so far. `DurableTransactions = true` makes every `Commit` wait. Readers in
other processes see only durable commits.

## Memory

Resident memory is roughly:

* `BlockCacheSize` (256 MiB by default; a native, sharded CLOCK cache that can be shared by several databases),
* the mutable memtable and up to `MaxImmutableMemtables` sealed ones (`MemtableSize`, 64 MiB by default, in native
  memory),
* small per-table metadata: the top-level index and table properties,
* WAL bytes waiting for upload, bounded by `MaxUnflushedWalBytes`.

Writers wait (stall) when sealed memtables, level 0 tables or WAL bytes exceed their limits, until background flush,
compaction or upload catches up.

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
| `BlockSize` | 32 KiB | Target uncompressed data block size; one cache miss reads one block. |
| `BlockCacheSize` / `BlockCache` | 256 MiB | Size of the block cache the database creates, or a shared cache. |
| `BloomBitsPerKey` | 16 | Bloom filter size; a false positive costs one ranged GET when a key is written or looked up. |
| `ValueSeparationThreshold` | 1 KiB | Longer values are stored in value files, so compaction does not rewrite them. |
| `LevelBaseBytes`, `LevelMultiplier`, `TargetFileSize` | 256 MiB, 10, 64 MiB | Leveled compaction shape. |
| `LeaseDuration` | 15 s | Writer lease duration; also the longest failover wait. |
| `WalChunkTargetSize` | 256 KiB | Size of one WAL object; larger commits span several. |
| `DeletionGracePeriod` | 10 min | Obsolete objects are kept this long for readers in other processes. |
| `RegisterReader` | false | A reader registers the oldest manifest it uses, so the writer keeps its objects past the grace period. |

A reader whose snapshot is older than the grace period, and which did not register, fails with `BTDBException`
("no longer exists"). It never reads a partial state.

## Object layout

| Key under `Prefix` | Content |
| --- | --- |
| `LOCK` | Writer lease. |
| `manifest/{n}` | The table set, the writer epoch and where WAL replay starts. |
| `wal/{n}` | WAL chunks with commit records. |
| `tables/{n}.sst` | Sorted tables with partitioned index and bloom filters. |
| `values/{n}.vlf` | Separated values. |
| `readers/{id}` | Reader registry entries. |

Every object is written once with create-if-absent and never modified.

## Other storage

`ILsmStorage` is provider-neutral: create-if-absent, read, ranged read (synchronous and asynchronous), list, delete,
put and a lease. `InMemoryLsmStorage` is for tests. Only Azure is implemented and qualified.

## Design and measurements

* [Implementation plan and design](../BTDB.LSM/ImplementationPlan.md)
* [Measurements](../BTDB.LSM/Measurements.md)
