using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using BTDB.Replication.EventLog;

namespace BTDB.Replication.Azure;

/// <summary>
/// Event-log topics as Block Blobs below a prefix. A write up to the single-request limit is one conditional Put Blob
/// (If-Match, or If-None-Match: * to create); larger content is staged under SDK-generated unique block IDs and committed
/// with the same condition. Content and owner metadata change atomically. Configure the client with
/// <c>Retry.MaxRetries = 0</c>: a transient failure of a mutation is reported as ambiguous and the log repeats the exact
/// write itself.
/// </summary>
public sealed class AzureEventLogStorage : IEventLogStorage
{
    readonly BlobContainerClient _container;
    readonly string _root;
    readonly TimeProvider _time;
    readonly long _singleUploadLimit;
    const string OwnerKey = "btdb_elog_owner";
    const string EndpointKey = "btdb_elog_endpoint";
    const string Sha256Key = "btdb_sha256";
    const string DeleteAfterKey = "btdb_delete_after";

    public AzureEventLogStorage(BlobContainerClient container, string prefix, TimeProvider? timeProvider = null)
        : this(container, prefix, timeProvider, 64 * 1024 * 1024)
    {
    }

    internal AzureEventLogStorage(BlobContainerClient container, string prefix, TimeProvider? timeProvider,
        long singleUploadLimit)
    {
        _container = container ?? throw new ArgumentNullException(nameof(container));
        ArgumentNullException.ThrowIfNull(prefix);
        if (prefix.TrimEnd('/').Length == 0)
            throw new ArgumentException("The event log needs its own nonempty prefix.", nameof(prefix));
        _root = prefix.TrimEnd('/') + "/";
        _time = timeProvider ?? TimeProvider.System;
        _singleUploadLimit = singleUploadLimit;
    }

    BlockBlobClient Blob(string key) => _container.GetBlockBlobClient(_root + key);

    static bool IsTransient(Exception error, CancellationToken cancellation) =>
        AzureLeaderStorage.IsTransient(error, cancellation);

    static string Version(ETag etag) => etag.ToString().Trim('"');
    static ETag Tag(string version) => new("\"" + version.Trim('"') + "\"");

    static EventLogOwner? Owner(IDictionary<string, string> metadata) =>
        metadata.TryGetValue(OwnerKey, out var session) && metadata.TryGetValue(EndpointKey, out var endpoint)
            ? new(session, Uri.UnescapeDataString(endpoint)) : null;

    static Dictionary<string, string> Metadata(EventLogOwner? owner, string? sha256)
    {
        var metadata = new Dictionary<string, string>();
        if (owner != null)
        {
            metadata[OwnerKey] = owner.Session;
            metadata[EndpointKey] = Uri.EscapeDataString(owner.Endpoint);
        }
        if (sha256 != null) metadata[Sha256Key] = sha256;
        return metadata;
    }

    static DateTimeOffset? DeletionTime(IDictionary<string, string> metadata)
    {
        if (!metadata.TryGetValue(DeleteAfterKey, out var value)) return null;
        if (!DateTimeOffset.TryParseExact(value, "O", CultureInfo.InvariantCulture, DateTimeStyles.None, out var time))
            throw new InvalidDataException("Invalid deletion deadline metadata.");
        return time;
    }

    public async ValueTask<EventLogBlob?> ReadAsync(string key, CancellationToken cancellation)
    {
        try
        {
            var result = await Blob(key).DownloadContentAsync(cancellation).ConfigureAwait(false);
            return new(Version(result.Value.Details.ETag), result.Value.Content.ToMemory(),
                Owner(result.Value.Details.Metadata));
        }
        catch (RequestFailedException error) when (error.Status == 404) { return null; }
        catch (Exception error) when (IsTransient(error, cancellation))
        { throw new IOException("Azure event log read failed.", error); }
    }

    public async ValueTask<EventLogBlobProperties?> GetPropertiesAsync(string key, CancellationToken cancellation)
    {
        try
        {
            var result = await Blob(key).GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false);
            return new(Version(result.Value.ETag), result.Value.ContentLength, Owner(result.Value.Metadata));
        }
        catch (RequestFailedException error) when (error.Status == 404) { return null; }
        catch (Exception error) when (IsTransient(error, cancellation))
        { throw new IOException("Azure event log properties read failed.", error); }
    }

    public async ValueTask ReadRangeAsync(string key, string version, long offset, Memory<byte> destination,
        CancellationToken cancellation)
    {
        if (destination.IsEmpty) return;
        try
        {
            var result = await Blob(key).DownloadStreamingAsync(new BlobDownloadOptions
            {
                Range = new HttpRange(offset, destination.Length),
                Conditions = new BlobRequestConditions { IfMatch = Tag(version) }
            }, cancellation).ConfigureAwait(false);
            await using var stream = result.Value.Content;
            await stream.ReadExactlyAsync(destination, cancellation).ConfigureAwait(false);
        }
        catch (RequestFailedException error) when (error.Status is 404 or 412 or 416)
        { throw new EventLogVersionChangedException(key); }
        catch (Exception error) when (IsTransient(error, cancellation))
        { throw new IOException("Azure event log range read failed.", error); }
    }

    public async ValueTask<EventLogWriteResult> WriteAsync(string key, string? expectedVersion,
        ReadOnlyMemory<byte> content, EventLogOwner? owner, string? sha256, CancellationToken cancellation)
    {
        var conditions = expectedVersion == null
            ? new BlobRequestConditions { IfNoneMatch = ETag.All }
            : new BlobRequestConditions { IfMatch = Tag(expectedVersion) };
        try
        {
            var result = await Blob(key).UploadAsync(BinaryData.FromBytes(content).ToStream(), new BlobUploadOptions
            {
                Conditions = conditions, Metadata = Metadata(owner, sha256),
                TransferOptions = new StorageTransferOptions
                {
                    InitialTransferSize = _singleUploadLimit,
                    MaximumTransferSize = Math.Min(_singleUploadLimit, 16 * 1024 * 1024)
                }
            }, cancellation).ConfigureAwait(false);
            return new(EventLogWriteOutcome.Applied, Version(result.Value.ETag));
        }
        catch (RequestFailedException error) when (error.Status is 404 or 409 or 412) { return EventLogWriteResult.Rejected; }
        catch (Exception error) when (IsTransient(error, cancellation)) { return EventLogWriteResult.Ambiguous; }
    }

    public async ValueTask<EventLogWriteResult> SetOwnerAsync(string key, string expectedVersion, EventLogOwner owner,
        CancellationToken cancellation)
    {
        // Writable splits carry only owner metadata, so replacing all metadata preserves everything else.
        try
        {
            var result = await Blob(key).SetMetadataAsync(Metadata(owner, null),
                new BlobRequestConditions { IfMatch = Tag(expectedVersion) }, cancellation).ConfigureAwait(false);
            return new(EventLogWriteOutcome.Applied, Version(result.Value.ETag));
        }
        catch (RequestFailedException error) when (error.Status is 404 or 412) { return EventLogWriteResult.Rejected; }
        catch (Exception error) when (IsTransient(error, cancellation)) { return EventLogWriteResult.Ambiguous; }
    }

    public async IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        await using var blobs = _container.GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, _root + prefix,
            cancellation).GetAsyncEnumerator(cancellation);
        while (true)
        {
            bool next;
            try { next = await blobs.MoveNextAsync().ConfigureAwait(false); }
            catch (Exception error) when (IsTransient(error, cancellation))
            { throw new IOException("Azure event log listing failed.", error); }
            if (!next) yield break;
            var blob = blobs.Current;
            yield return new(blob.Name[_root.Length..], Version(blob.Properties.ETag!.Value),
                blob.Properties.ContentLength ?? 0, DeletionTime(blob.Metadata));
        }
    }

    public async ValueTask<string?> ScheduleDeletionAsync(string key, string version, TimeSpan delay,
        CancellationToken cancellation)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(delay.Ticks);
        var blob = Blob(key);
        try
        {
            var properties = (await blob.GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false)).Value;
            if (Version(properties.ETag) != version.Trim('"')) return null;
            if (DeletionTime(properties.Metadata) != null) return version;
            var metadata = new Dictionary<string, string>(properties.Metadata)
            {
                [DeleteAfterKey] = (_time.GetUtcNow() + delay).ToString("O", CultureInfo.InvariantCulture)
            };
            var result = await blob.SetMetadataAsync(metadata, new BlobRequestConditions { IfMatch = properties.ETag },
                cancellation).ConfigureAwait(false);
            return Version(result.Value.ETag);
        }
        catch (RequestFailedException error) when (error.Status is 404 or 412) { return null; }
        catch (Exception error) when (IsTransient(error, cancellation))
        { throw new IOException("Deletion marking is unresolved; reread before retry.", error); }
    }

    public async ValueTask<bool> DeleteAsync(string key, string version, CancellationToken cancellation)
    {
        var blob = Blob(key);
        try
        {
            var properties = (await blob.GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false)).Value;
            if (Version(properties.ETag) != version.Trim('"') || DeletionTime(properties.Metadata) is not { } deadline ||
                _time.GetUtcNow() < deadline) return false;
            var result = await blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots,
                new BlobRequestConditions { IfMatch = properties.ETag }, cancellation).ConfigureAwait(false);
            return result.Value;
        }
        catch (RequestFailedException error) when (error.Status is 404 or 412) { return false; }
        catch (Exception error) when (IsTransient(error, cancellation))
        { throw new IOException("Event log cleanup result is unresolved.", error); }
    }
}
