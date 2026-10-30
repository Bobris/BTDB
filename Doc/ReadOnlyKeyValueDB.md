# ReadOnlyKeyValueDB

`ReadOnlyKeyValueDB` is an immutable `IKeyValueDB` stored in one continuous block of memory, usually a memory mapped
file. Use it to ship a snapshot of a database, for example reference data, which many processes read without loading
it: opening only checks the header, and nothing is copied into memory.

## Creating

`ReadOnlyCreator` writes all keys and values visible in any `IKeyValueDBTransaction`, together with its commit ulong
and ulongs:

```csharp
using var tr = sourceDb.StartReadOnlyTransaction();
new ReadOnlyCreator(tr).WriteToFile("data.robtdb");
// or: byte[] data = new ReadOnlyCreator(tr).WriteToArray();
// or: creator.Run(ref memWriter) to write into any MemWriter
```

`SetMaxChildren` sets the maximum number of keys in a leaf and children of a branch (2 to 127, default 31).

Limits: the whole data must fit into 4GB and one key into 64KB. Keys of one node are also split by size, so long keys
only make nodes smaller.

## Opening

```csharp
using var db = ReadOnlyKeyValueDB.OpenFile("data.robtdb"); // memory mapped
using var db2 = ReadOnlyKeyValueDB.Create(data);            // pins ReadOnlyMemory<byte>
using var db3 = new ReadOnlyKeyValueDB(address, length);    // memory owned by the caller
```

The data must not change while the database is open. `ObjectDB` and relations work on it unchanged for reading.

Opening reads only the header and footer, so a memory mapped file is read as it is accessed. `verifyChecksum: true`
(on `OpenFile`, `Create` and the constructor) or `VerifyChecksum()` reads all data first and throws
`InvalidDataException` when they do not match their checksum, or have none.

## Behavior

* All cursor operations follow `IKeyValueDBCursor`. `GetKeyIndex`, `FindKeyIndex` and finding a key are O(log n),
  `GetKeyValueCount` is O(1).
* `GetValueSpan` and `GetValueMemory` without `copy` return memory of the data directly. Keys are prefix compressed,
  so they are copied into the provided buffer unless their node has no common prefix.
* `StartTransaction` and `StartReadOnlyTransaction` return the same kind of read only transaction. Every modification,
  `StartWritingTransaction` and `CreateKvi` throw `BTDBException`. `SetCommitUlong` and `SetUlong` are allowed only
  with the stored value. `Compact` does nothing.
* Truncated data and node or value offsets outside the data throw `InvalidDataException` when the database is opened
  or the node is read. Other damage is detected only by checking the checksum; without it a damaged key or value reads
  as other content.

## Format

The format is documented in `ReadOnlyFormat` in
[ReadOnlyKeyValueDB.cs](../BTDB/KVDBLayer/ReadOnly/ReadOnlyKeyValueDB.cs). It is a B+tree written bottom up after
the values. Leaves and branches store a common key prefix and key suffixes; branches also store the number of keys
in their children, which makes access by key index O(log n). Since version 2 the data end with the XxHash64 of all
bytes before it; version 1 data, without it, are still read.
