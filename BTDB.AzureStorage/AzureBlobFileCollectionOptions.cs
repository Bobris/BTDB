using System;

namespace BTDB.AzureStorage;

public sealed class AzureBlobFileCollectionOptions
{
    public IBlobStorageBackend? BlobStorageBackend { get; init; }

    public string? LocalCacheDirectory { get; init; }

    public bool DeleteLocalCacheDirectoryOnDispose { get; init; }

    public int DownloadParallelism { get; init; } = Math.Max(1, Environment.ProcessorCount);

    public TimeSpan TransactionLogFlushPeriod { get; init; } = TimeSpan.FromSeconds(30);

    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    public IAzureBlobFileCollectionLogger? Logger { get; init; }

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(BlobStorageBackend);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(DownloadParallelism);
        if (TransactionLogFlushPeriod <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(TransactionLogFlushPeriod));
    }

    internal IBlobStorageBackend CreateBlobStorageBackend()
    {
        return BlobStorageBackend!;
    }
}
