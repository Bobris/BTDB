# BTDB.LSM.Azure (preview)

Azure Blob Storage adapter for `BTDB.LSM`. `AzureLsmStorage` stores one database in a Blob container, optionally
under a prefix. Objects are created with `If-None-Match: *`, large ones as staged blocks; reads are ranged GETs with
synchronous and asynchronous variants; the writer lease is a blob lease.

Create the `BlobContainerClient` with `Retry.MaxRetries = 0`: the database reconciles ambiguous writes itself, and the
SDK's retry of a conditional create could report the database's own landed write as a conflict.

```csharp
var container = new BlobContainerClient(endpoint, credential,
    new BlobClientOptions { Retry = { MaxRetries = 0 } });
using var db = await LsmKeyValueDB.OpenAsync(new LsmKeyValueDBOptions
{
    Storage = new AzureLsmStorage(container),
    Prefix = "orders/",
});
```

Documentation: [https://github.com/Bobris/BTDB/blob/master/Doc/LsmKeyValueDB.md]
