using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace BTDB.Replication.Azure;

/// <summary>
/// A versioned blob as one Block Blob; the version is its ETag. A write is a conditional upload (<c>If-Match</c>, or
/// <c>If-None-Match: *</c> to create) that installs the whole content atomically; content above the SDK's single-upload
/// limit is staged in blocks and committed with the same condition. The caller supplies an authenticated client of an
/// existing container, configured with <c>Retry.MaxRetries = 0</c>: a transient failure of a write is reported as
/// Ambiguous, and an SDK retry of a write that landed would fail its own condition and read as Rejected.
/// </summary>
public sealed class AzureVersionedBlob(BlobClient blob) : IVersionedBlob
{
    readonly BlobClient _blob = blob ?? throw new ArgumentNullException(nameof(blob));

    public async ValueTask<VersionedBlobContent?> ReadAsync(CancellationToken cancellation)
    {
        try
        {
            var result = await _blob.DownloadContentAsync(cancellation).ConfigureAwait(false);
            return new(result.Value.Details.ETag.ToString(), result.Value.Content.ToMemory());
        }
        catch (RequestFailedException error) when (IsMissingContainer(error)) { throw MissingContainer(error); }
        catch (RequestFailedException error) when (error.Status == 404) { return null; }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        { throw new IOException("Azure versioned blob read failed.", error); }
    }

    public async ValueTask<VersionedBlobWriteResult> WriteAsync(string? expectedVersion, ReadOnlyMemory<byte> content,
        CancellationToken cancellation)
    {
        var conditions = expectedVersion == null
            ? new BlobRequestConditions { IfNoneMatch = ETag.All }
            : new BlobRequestConditions { IfMatch = new ETag(expectedVersion) };
        try
        {
            var result = await _blob.UploadAsync(BinaryData.FromBytes(content),
                new BlobUploadOptions { Conditions = conditions }, cancellation).ConfigureAwait(false);
            return new(VersionedBlobWriteOutcome.Applied, result.Value.ETag.ToString());
        }
        // A missing container would otherwise read as an empty blob whose every write is Rejected, a livelock.
        catch (RequestFailedException error) when (IsMissingContainer(error)) { throw MissingContainer(error); }
        catch (RequestFailedException error) when (error.Status is 404 or 409 or 412)
        { return VersionedBlobWriteResult.Rejected; }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        { return VersionedBlobWriteResult.Ambiguous; }
    }

    static bool IsMissingContainer(RequestFailedException error) =>
        error.Status == 404 && error.ErrorCode == BlobErrorCode.ContainerNotFound;

    InvalidOperationException MissingContainer(RequestFailedException error) =>
        new($"Container '{_blob.BlobContainerName}' of versioned blob '{_blob.Name}' does not exist.", error);
}
