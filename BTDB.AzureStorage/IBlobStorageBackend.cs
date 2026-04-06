using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.AzureStorage;

public delegate void BlobDataReader(long offset, Span<byte> destination);

public interface IBlobStorageBackend
{
    IAsyncEnumerable<BlobFileInfo> ListFilesAsync(CancellationToken cancellationToken = default);
    Task DownloadToAsync(string name, string localPath, CancellationToken cancellationToken = default);
    Task UploadBlockBlobAsync(string name, string localPath, CancellationToken cancellationToken = default);
    Task AppendBlockBlobAsync(string name, long targetLength, BlobDataReader reader,
        CancellationToken cancellationToken = default);
    Task DeleteIfExistsAsync(string name, CancellationToken cancellationToken = default);
}
