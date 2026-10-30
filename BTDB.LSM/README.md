# BTDB.LSM

`LsmKeyValueDB` is an `IKeyValueDB` implementation that stores a log-structured merge tree directly in object storage,
inspired by Pebble. Memory use is bounded by configuration and does not grow with the database, and there is no local
disk cache. One process writes; any number of processes read consistent snapshots and catch up on demand with
`RefreshAsync` or `WaitForCommitUlongAsync`. `ObjectDB` and relations run on it unchanged.

This package contains the engine and in-memory storage for tests. For Azure Blob Storage, add `BTDB.LSM.Azure`.

```csharp
using var db = await LsmKeyValueDB.OpenAsync(new LsmKeyValueDBOptions
{
    Storage = new AzureLsmStorage(container),
});
```

The storage format may still change between versions.

Documentation: [https://github.com/Bobris/BTDB/blob/master/Doc/LsmKeyValueDB.md]
