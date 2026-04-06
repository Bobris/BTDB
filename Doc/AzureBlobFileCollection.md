# AzureBlobFileCollection

`BTDB.AzureStorage` provides `AzureBlobFileCollection`, an `IFileCollection` implementation for running BTDB on top of
Azure Blob Storage while keeping the active working set in a local memory-mapped cache stored on disk.

The current implementation is intentionally optimized only for BTDB KeyValueDB file types: `trl`, `pvl`, and `kvi`.
Other `IFileCollection` use cases are not supported.

## Behavior

- Startup reconciles local cache files against Azure blobs.
- If `LocalCacheDirectory` is not specified, the cache is created under the system temp directory.
- Set `DeleteLocalCacheDirectoryOnDispose` to `true` to remove the local cache directory during dispose.
- Locally missing files and files where the remote blob is longer are downloaded from Azure.
- Files missing in Azure and files where the local cache is longer are queued for upload.
- `.trl` files are uploaded as block blobs, with `AzureBlobStorageBackend` using an internal 128 KB preferred block
  size, reusing committed full blocks, and reuploading the trailing partial block when needed so the committed block
  count stays bounded.
- `pvl` and `kvi` files are uploaded as regular block blobs.
- `AzureBlobStorageBackend` can scope all BTDB files under a prefix within a shared container.
- Remote writes and deletes are processed by a single FIFO background worker so synchronous BTDB file callbacks never
  block on blob I/O and queued deletes cannot race ahead of pending uploads.
- `AzureBlobFileCollectionOptions.Logger` can observe startup downloads plus FIFO queueing, execution, retries, and
  current queue length.
- Transaction log uploads are batched on a configurable timer via `TransactionLogFlushPeriod`.
- `FlushPendingChangesAsync()` can be used to await the current background queue state, which is useful before process
  shutdown or in tests. This drains queued Azure Blob operations; it does not make the local cache survive pod or
  process termination.

## Example

```csharp
var containerClient = new BlobContainerClient(connectionString, "btdb");

await using var fileCollection = await AzureBlobFileCollection.CreateAsync(new AzureBlobFileCollectionOptions
{
    BlobStorageBackend = new AzureBlobStorageBackend(containerClient, "tenant-a/prod"),
    TransactionLogFlushPeriod = TimeSpan.FromSeconds(30)
});

using var db = new BTreeKeyValueDB(fileCollection);
```
