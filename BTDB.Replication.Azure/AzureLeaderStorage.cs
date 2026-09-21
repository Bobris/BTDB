using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace BTDB.Replication.Azure;

/// <summary>Finite lease and conditional JSON selection on the same leader blob. The caller supplies an
/// authenticated BlobClient and initial cluster JSON. No container creation or credential discovery occurs here.</summary>
internal sealed class AzureLeaderStorage(BlobClient blob, TimeSpan duration, string initialJson,
    Func<Guid>? newLeaseId = null) : IReplicationLeaseStorage, ILeaderRecordStorage
{
    bool _initialized;

    public async ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation)
    {
        if (duration < TimeSpan.FromSeconds(15) || duration > TimeSpan.FromSeconds(60) || duration.Ticks % TimeSpan.TicksPerSecond != 0)
            throw new ArgumentOutOfRangeException(nameof(duration), "Azure finite leases require 15–60 whole seconds.");
        try
        {
            if (!_initialized)
            {
                try
                {
                    await blob.UploadAsync(BinaryData.FromString(initialJson), new BlobUploadOptions
                    {
                        Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All }
                    }, cancellation).ConfigureAwait(false);
                }
                catch (RequestFailedException error) when (error.Status is 409 or 412) { }
                _initialized = true;
            }
            var id = (newLeaseId?.Invoke() ?? Guid.NewGuid()).ToString();
            var lease = blob.GetBlobLeaseClient(id);
            try
            {
                await lease.AcquireAsync(duration, cancellationToken: cancellation).ConfigureAwait(false);
            }
            catch (RequestFailedException error) when (error.Status is 409 or 412) { return null; }
            catch (Exception error) when (IsTransient(error, cancellation))
            {
                // A lost acquire reply may already own the lease. Renewing the exact proposed ID proves it.
                // If that cannot be confirmed, never return ownership; a later fresh attempt may wait for expiry.
                return await RenewAsync(id, cancellation).ConfigureAwait(false) is { } renewed ? new(id, renewed) : null;
            }
            return new(id, duration);
        }
        catch (Exception error) when (IsTransient(error, cancellation))
        {
            throw new IOException("Azure lease acquisition could not be confirmed.", error);
        }
    }

    public async ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation)
    {
        try
        {
            await blob.GetBlobLeaseClient(handle).RenewAsync(cancellationToken: cancellation).ConfigureAwait(false);
            return duration;
        }
        catch (RequestFailedException error) when (error.Status is 404 or 409 or 412) { return null; }
        catch (Exception error) when (IsTransient(error, cancellation))
        {
            throw new IOException("Azure lease renewal could not be confirmed.", error);
        }
    }

    public async ValueTask<LeaderRecord> ReadAsync(CancellationToken cancellation)
    {
        try
        {
            var result = await blob.DownloadContentAsync(cancellation).ConfigureAwait(false);
            return new(result.Value.Details.ETag.ToString(), result.Value.Content.ToString());
        }
        catch (Exception error) when (IsTransient(error, cancellation))
        {
            throw new IOException("Azure leader record read failed.", error);
        }
    }

    public async ValueTask<LeaderWriteOutcome> WriteAsync(string leaseHandle, string expectedToken, string json,
        CancellationToken cancellation)
    {
        try
        {
            await blob.UploadAsync(BinaryData.FromString(json), new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { LeaseId = leaseHandle, IfMatch = new ETag(expectedToken) }
            }, cancellation).ConfigureAwait(false);
            return LeaderWriteOutcome.Applied;
        }
        catch (RequestFailedException error) when (error.Status is 409 or 412) { return LeaderWriteOutcome.Rejected; }
        catch (Exception error) when (IsTransient(error, cancellation)) { return LeaderWriteOutcome.Ambiguous; }
    }

    internal static bool IsTransient(Exception error, CancellationToken cancellation) =>
        error is IOException || error is RequestFailedException { Status: 0 or 408 or 429 or >= 500 } ||
        (error is OperationCanceledException && !cancellation.IsCancellationRequested);
}
