using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace BTDB.Replication.Azure;

/// <summary>Database-scoped canonical TRLs. Committed prefix blocks are reused; only the partial final block
/// and appended bytes are staged. Unique block IDs keep concurrent/stale staging from changing a winning intent.
/// One conditional block-list commit atomically installs bytes and authority metadata.</summary>
internal sealed class AzureCanonicalTrlStorage(BlobContainerClient container, string prefix) : ICanonicalTrlStorage
{
    const int BlockSize = 4 * 1024 * 1024;
    BlockBlobClient Blob(string key)
    {
        TrlMetadata.Validate(new(key, 1));
        return container.GetBlockBlobClient(string.IsNullOrEmpty(prefix) ? key : prefix.TrimEnd('/') + "/" + key);
    }

    public async ValueTask<TrlObjectState?> ReadAsync(string key, CancellationToken cancellation)
    {
        try
        {
            var result = await Blob(key).GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false);
            return new(result.Value.ETag.ToString(), checked((uint)result.Value.ContentLength),
                TrlMetadata.Decode(new Dictionary<string, string>(result.Value.Metadata)));
        }
        catch (RequestFailedException error) when (error.Status == 404) { return null; }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        { throw new IOException("Azure canonical TRL metadata read failed.", error); }
    }

    public async ValueTask ReadRangeAsync(string key, string token, uint offset, Memory<byte> destination,
        CancellationToken cancellation)
    {
        if (destination.IsEmpty) return;
        try
        {
            var result = await Blob(key).DownloadStreamingAsync(new BlobDownloadOptions
            {
                Range = new HttpRange(offset, destination.Length),
                Conditions = new BlobRequestConditions { IfMatch = new ETag(token) }
            }, cancellation).ConfigureAwait(false);
            using var stream = result.Value.Content;
            await stream.ReadExactlyAsync(destination, cancellation).ConfigureAwait(false);
        }
        catch (RequestFailedException error) when (error.Status is 404 or 412 or 416)
        { throw new IOException("Selected canonical TRL version is no longer available.", error); }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        { throw new IOException("Azure canonical TRL range read failed.", error); }
    }

    public async ValueTask<TrlWriteResult> WriteAsync(TrlWrite write, CancellationToken cancellation)
    {
        var blob = Blob(write.Key);
        var conditions = new BlobRequestConditions();
        var ids = new List<string>();
        ulong offset = 0;
        var commitDispatched = false;
        try
        {
            if (write.ExpectedToken is { } token)
            {
                conditions.IfMatch = new ETag(token);
                var blocks = await blob.GetBlockListAsync(BlockListTypes.Committed, cancellationToken: cancellation)
                    .ConfigureAwait(false);
                if (blocks.GetRawResponse().Headers.ETag?.ToString().Trim('"') != token.Trim('"')) return new(TrlWriteOutcome.Rejected);
                if (blocks.Value.CommittedBlocks.Sum(b => b.SizeLong) != write.ExpectedLength)
                    return new(TrlWriteOutcome.Rejected);
                foreach (var block in blocks.Value.CommittedBlocks)
                {
                    // Retain a partial block too for metadata-only adoption. Appends replace it using the
                    // caller's verified native prefix, avoiding unbounded numbers of tiny committed blocks.
                    if (block.SizeLong != BlockSize && write.Length != write.ExpectedLength) break;
                    ids.Add(block.Name);
                    offset += (ulong)block.SizeLong;
                }
            }
            else
            {
                if (write.ExpectedLength != 0) throw new ArgumentException("A new TRL cannot have a previous length.");
                conditions.IfNoneMatch = ETag.All;
            }
            if (write.Length < write.ExpectedLength) throw new ArgumentException("Canonical TRLs cannot shrink.");
            var buffer = ArrayPool<byte>.Shared.Rent(BlockSize);
            try
            {
                while (offset < write.Length)
                {
                    var count = (int)Math.Min((ulong)BlockSize, write.Length - offset);
                    write.Source.RandomRead(buffer.AsSpan(0, count), offset, false);
                    var id = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
                    using var stream = new MemoryStream(buffer, 0, count, false);
                    await blob.StageBlockAsync(id, stream, cancellationToken: cancellation).ConfigureAwait(false);
                    ids.Add(id);
                    offset += (uint)count;
                }
            }
            finally { ArrayPool<byte>.Shared.Return(buffer); }
            commitDispatched = true;
            var result = await blob.CommitBlockListAsync(ids, new CommitBlockListOptions
            {
                Conditions = conditions,
                Metadata = new Dictionary<string, string>(write.Metadata.Encode())
            }, cancellation).ConfigureAwait(false);
            return new(TrlWriteOutcome.Applied,
                new(result.Value.ETag.ToString(), write.Length, write.Metadata));
        }
        catch (RequestFailedException error) when (error.Status is 404 or 409 or 412)
        { return new(TrlWriteOutcome.Rejected); }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        {
            if (commitDispatched) return new(TrlWriteOutcome.Ambiguous);
            throw new IOException("Azure canonical TRL staging failed.", error);
        }
    }
}
