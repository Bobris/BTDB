using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using BTDB.LSM.Storage;

namespace BTDB.LSM.Azure;

/// ILsmStorage on one Azure Blob container (BTDB.LSM/ImplementationPlan.md, section 5). Keys are blob names; scope a
/// database inside a shared container with LsmKeyValueDBOptions.Prefix.
/// The container client must be created with <c>Retry.MaxRetries = 0</c>: SDK retries of a conditional create could
/// report the database's own landed write as a conflict. Creates report transient failures after dispatch as
/// Ambiguous instead, and the database reconciles them by reading back. Reads are retried here.
public sealed class AzureLsmStorage(BlobContainerClient container, AzureLsmStorageOptions? options = null)
    : ILsmStorage
{
    readonly AzureLsmStorageOptions _options = options ?? new();

    public BlobContainerClient Container => container;

    static bool IsTransient(Exception error, CancellationToken cancellation) => error switch
    {
        RequestFailedException failed => failed.Status is 0 or 408 or 429 or >= 500,
        OperationCanceledException => !cancellation.IsCancellationRequested,
        EndOfStreamException => false,
        IOException or HttpRequestException or TimeoutException => true,
        _ => false
    };

    async ValueTask<T> ReadWithRetry<T>(Func<ValueTask<T>> read, CancellationToken cancellation)
    {
        for (var attempt = 1;; attempt++)
        {
            try
            {
                return await read();
            }
            catch (Exception error) when (attempt < _options.ReadAttempts && IsTransient(error, cancellation))
            {
                await Task.Delay(_options.ReadRetryDelay * attempt, cancellation);
            }
        }
    }

    T ReadWithRetry<T>(Func<T> read)
    {
        for (var attempt = 1;; attempt++)
        {
            try
            {
                return read();
            }
            catch (Exception error) when (attempt < _options.ReadAttempts && IsTransient(error, default))
            {
                Thread.Sleep(_options.ReadRetryDelay * attempt);
            }
        }
    }

    public async ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content,
        CancellationToken cancellation)
    {
        var blob = container.GetBlockBlobClient(key);
        var conditions = new BlobRequestConditions { IfNoneMatch = ETag.All };
        try
        {
            if (content.Length <= _options.SingleUploadLimit)
            {
                await blob.UploadAsync(new BinaryData(content).ToStream(),
                    new BlobUploadOptions { Conditions = conditions }, cancellation);
                return LsmWriteOutcome.Applied;
            }

            // Large objects: staged blocks with attempt-unique ids, committed once with the create-if-absent condition.
            var attempt = Guid.NewGuid().ToString("N")[..8];
            var ids = new List<string>();
            for (var offset = 0; offset < content.Length; offset += _options.BlockSize)
                ids.Add(Convert.ToBase64String(Encoding.ASCII.GetBytes($"{attempt}-{ids.Count:D8}")));
            await Parallel.ForEachAsync(Enumerable.Range(0, ids.Count),
                new ParallelOptions { MaxDegreeOfParallelism = _options.UploadParallelism, CancellationToken = cancellation },
                async (index, token) =>
                {
                    var offset = index * _options.BlockSize;
                    var length = Math.Min(_options.BlockSize, content.Length - offset);
                    await blob.StageBlockAsync(ids[index], new BinaryData(content.Slice(offset, length)).ToStream(),
                        cancellationToken: token);
                });
            await blob.CommitBlockListAsync(ids, new CommitBlockListOptions { Conditions = conditions }, cancellation);
            return LsmWriteOutcome.Applied;
        }
        catch (RequestFailedException error) when (error.Status is 409 or 412)
        {
            return LsmWriteOutcome.Rejected;
        }
        catch (Exception error) when (IsTransient(error, cancellation))
        {
            return LsmWriteOutcome.Ambiguous;
        }
    }

    public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation) =>
        ReadWithRetry<byte[]?>(async () =>
        {
            try
            {
                var result = await container.GetBlobClient(key).DownloadContentAsync(cancellation);
                return result.Value.Content.ToArray();
            }
            catch (RequestFailedException error) when (error.Status == 404)
            {
                return null;
            }
        }, cancellation);

    public async ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination,
        CancellationToken cancellation)
    {
        await ReadWithRetry(async () =>
        {
            try
            {
                var response = await container.GetBlobClient(key).DownloadStreamingAsync(
                    new BlobDownloadOptions { Range = new HttpRange(offset, destination.Length) }, cancellation);
                await using var stream = response.Value.Content;
                await stream.ReadExactlyAsync(destination, cancellation);
                return true;
            }
            catch (RequestFailedException error) when (error.Status == 404)
            {
                throw new FileNotFoundException("Object does not exist: " + key, key, error);
            }
            catch (RequestFailedException error) when (error.Status == 416)
            {
                throw new EndOfStreamException($"Range beyond the end of {key}", error);
            }
        }, cancellation);
    }

    /// Synchronous ranged read through the SDK's synchronous pipeline (HttpClient.Send), not sync-over-async.
    public void ReadRange(string key, long offset, Span<byte> destination)
    {
        var length = destination.Length;
        var buffer = ReadWithRetry(() =>
        {
            try
            {
                var response = container.GetBlobClient(key).DownloadStreaming(
                    new BlobDownloadOptions { Range = new HttpRange(offset, length) });
                using var stream = response.Value.Content;
                var data = new byte[length];
                stream.ReadExactly(data);
                return data;
            }
            catch (RequestFailedException error) when (error.Status == 404)
            {
                throw new FileNotFoundException("Object does not exist: " + key, key, error);
            }
            catch (RequestFailedException error) when (error.Status == 416)
            {
                throw new EndOfStreamException($"Range beyond the end of {key}", error);
            }
        });
        buffer.CopyTo(destination);
    }

    public async IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        await foreach (var item in container.GetBlobsAsync(BlobTraits.None, BlobStates.None, prefix, cancellation))
            yield return new(item.Name, item.Properties.ContentLength ?? 0);
    }

    public async ValueTask DeleteAsync(string key, CancellationToken cancellation)
    {
        await container.GetBlobClient(key).DeleteIfExistsAsync(cancellationToken: cancellation);
    }

    public async ValueTask PutAsync(string key, ReadOnlyMemory<byte> content, CancellationToken cancellation)
    {
        await container.GetBlobClient(key).UploadAsync(new BinaryData(content), overwrite: true, cancellation);
    }

    /// Azure leases last 15 to 60 seconds; other durations are clamped.
    static TimeSpan LeaseDuration(TimeSpan duration) =>
        TimeSpan.FromSeconds(Math.Clamp(Math.Floor(duration.TotalSeconds), 15, 60));

    public async ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration,
        CancellationToken cancellation)
    {
        var blob = container.GetBlobClient(key);
        try
        {
            await blob.UploadAsync(BinaryData.Empty, new BlobUploadOptions
            {
                Conditions = new() { IfNoneMatch = ETag.All }
            }, cancellation);
        }
        catch (RequestFailedException error) when (error.Status is 409 or 412)
        {
        }

        try
        {
            await blob.GetBlobLeaseClient(leaseId).AcquireAsync(LeaseDuration(duration), cancellationToken: cancellation);
            return true;
        }
        catch (RequestFailedException error) when (error.Status == 409)
        {
            return false;
        }
    }

    public async ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration,
        CancellationToken cancellation)
    {
        try
        {
            await container.GetBlobClient(key).GetBlobLeaseClient(leaseId).RenewAsync(cancellationToken: cancellation);
            return true;
        }
        catch (RequestFailedException error) when (error.Status is 409 or 412 or 404)
        {
            return false;
        }
    }

    public async ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation)
    {
        try
        {
            await container.GetBlobClient(key).GetBlobLeaseClient(leaseId).ReleaseAsync(cancellationToken: cancellation);
        }
        catch (RequestFailedException)
        {
        }
    }
}

public sealed class AzureLsmStorageOptions
{
    /// Objects up to this size are created with one Put Blob request; larger ones are staged in blocks.
    public int SingleUploadLimit { get; init; } = 4 * 1024 * 1024;

    public int BlockSize { get; init; } = 4 * 1024 * 1024;
    public int UploadParallelism { get; init; } = 4;
    public int ReadAttempts { get; init; } = 3;
    public TimeSpan ReadRetryDelay { get; init; } = TimeSpan.FromMilliseconds(100);
}
