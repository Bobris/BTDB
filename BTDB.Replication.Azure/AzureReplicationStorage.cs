using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.Replication.Azure;

/// <summary>Database-scoped canonical TRLs and immutable PVL/KVI storage. Bind a selected inventory and
/// optional leadership authority for restore and maintenance; each binding retains its own immutable session.</summary>
public sealed class AzureReplicationStorage : IReplicationStorage
{
    readonly BlobContainerClient container;
    readonly string prefix;
    readonly CanonicalTrlInventory? canonical;
    readonly LeaseAuthority? authority;

    public AzureReplicationStorage(BlobContainerClient container, string prefix)
    {
        this.container = container ?? throw new ArgumentNullException(nameof(container));
        this.prefix = prefix ?? throw new ArgumentNullException(nameof(prefix));
    }

    AzureReplicationStorage(AzureReplicationStorage storage, CanonicalTrlInventory inventory, LeaseAuthority? authority)
        : this(storage.container, storage.prefix)
    {
        canonical = inventory;
        this.authority = authority;
    }

    /// <summary>Create a distinct session view. Omitting authority permits restore only, not maintenance.</summary>
    public AzureReplicationStorage Bind(CanonicalTrlInventory inventory, LeaseAuthority? authority = null)
    {
        ArgumentNullException.ThrowIfNull(inventory);
        if (!inventory.IsFrom(this)) throw new ArgumentException("Inventory belongs to another storage instance.", nameof(inventory));
        return new(this, inventory, authority);
    }

    CanonicalTrlInventory Inventory => canonical ??
        throw new InvalidOperationException("Bind a selected canonical inventory before restoring files.");

    // Reuse committed prefix blocks; replace only a partial last block and append. Unique staged IDs keep
    // stale requests from changing a winning intent. The final CAS installs bytes and metadata atomically.
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
                Metadata = new Dictionary<string, string>(write.Metadata.Encode()) { ["btdb_file_id"] = write.FileId.ToString(System.Globalization.CultureInfo.InvariantCulture) }
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
    string Directory => string.IsNullOrEmpty(prefix) ? "files/" : prefix.TrimEnd('/') + "/files/";
    BlockBlobClient Blob(uint id, string extension) => container.GetBlockBlobClient(Directory + id.ToString(CultureInfo.InvariantCulture) + extension);

    public async IAsyncEnumerable<RemoteFile> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        await foreach (var file in Inventory.EnumerateAsync(cancellation).ConfigureAwait(false)) yield return file;
        await foreach (var blob in container.GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, prefix: Directory,
                           cancellationToken: cancellation).ConfigureAwait(false))
        {
            var name = blob.Name[Directory.Length..];
            var dot = name.LastIndexOf('.');
            if (dot <= 0 || !uint.TryParse(name.AsSpan(0, dot), NumberStyles.None, CultureInfo.InvariantCulture, out var id) || id == 0)
                continue;
            var type = name[dot..] switch { ".pvl" => KVFileType.PureValues, ".kvi" => KVFileType.KeyIndex, _ => KVFileType.Unknown };
            if (type == KVFileType.Unknown) continue;
            yield return new(id, type, checked((ulong)blob.Properties.ContentLength!.Value),
                blob.Properties.ETag!.Value.ToString(), true, blob.Metadata.TryGetValue("btdb_sha256", out var sha) ? sha : null);
        }
    }

    string Root => string.IsNullOrEmpty(prefix) ? "" : prefix.TrimEnd('/') + "/";

    void RequireAuthority(CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        if (authority is not { IsValid: true }) throw new InvalidOperationException("Remote maintenance requires live authority.");
    }

    public async IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        await foreach (var blob in container.GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, prefix: Root,
                           cancellationToken: cancellation).ConfigureAwait(false))
        {
            var key = blob.Name[Root.Length..];
            if (key.StartsWith("files/", StringComparison.Ordinal))
            {
                var name = key[6..];
                var dot = name.LastIndexOf('.');
                if (dot <= 0 || !uint.TryParse(name.AsSpan(0, dot), NumberStyles.None, CultureInfo.InvariantCulture, out var id) || id == 0) continue;
                var type = name[dot..] switch { ".pvl" => KVFileType.PureValues, ".kvi" => KVFileType.KeyIndex, _ => KVFileType.Unknown };
                if (type != KVFileType.Unknown) yield return new(key, id, type, blob.Properties.ETag!.Value.ToString());
            }
            else if (blob.Metadata.ContainsKey("btdb_term") && blob.Metadata.TryGetValue("btdb_file_id", out var value) &&
                     uint.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var id) && id != 0)
                // This adapter discovers canonical history from a supplied root. Keep its links traversable.
                yield return new(key, id, KVFileType.TransactionLog, blob.Properties.ETag!.Value.ToString(), true);
        }
    }

    public async ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation)
    {
        TrlMetadata.Validate(new(file.Key, file.FileId));
        RequireAuthority(cancellation);
        try
        {
            await container.GetBlobClient(Root + file.Key).DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots,
                new BlobRequestConditions { IfMatch = new ETag(file.Version) }, cancellation).ConfigureAwait(false);
        }
        catch (RequestFailedException error) when (error.Status is 404 or 412) { }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        { throw new IOException("Remote cleanup result is unresolved.", error); }
    }

    public async ValueTask<bool> ProtectPureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation)
    {
        RequireAuthority(cancellation);
        var blob = Blob(id, ".pvl");
        try
        {
            var properties = (await blob.GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false)).Value;
            if ((ulong)properties.ContentLength != source.Length || !properties.Metadata.ContainsKey("btdb_sha256"))
                throw new RemoteFileConflictException();
            // Contents were verified on upload/download. Metadata-only CAS invalidates any old delete token.
            RequireAuthority(cancellation);
            await blob.SetMetadataAsync(properties.Metadata, new BlobRequestConditions { IfMatch = properties.ETag }, cancellation)
                .ConfigureAwait(false);
            return true;
        }
        catch (RequestFailedException error) when (error.Status == 404) { return false; }
        catch (RequestFailedException error) when (error.Status == 412)
        { throw new IOException("PVL protection raced another version; retry the checkpoint.", error); }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        { throw new IOException("PVL protection is unresolved.", error); }
    }

    public async ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation)
    {
        if (file.FileType == KVFileType.TransactionLog)
            return await Inventory.ReadAsync(file, offset, buffer, cancellation).ConfigureAwait(false);
        if (offset > file.Length) throw new ArgumentOutOfRangeException(nameof(offset));
        var count = (int)Math.Min((ulong)buffer.Length, file.Length - offset);
        cancellation.ThrowIfCancellationRequested();
        if (count == 0) return 0;
        try
        {
            var response = await Blob(file.FileId, file.FileType == KVFileType.PureValues ? ".pvl" : ".kvi")
                .DownloadStreamingAsync(new BlobDownloadOptions
                {
                    Range = new HttpRange(checked((long)offset), count),
                    Conditions = new BlobRequestConditions { IfMatch = new ETag(file.Version) }
                }, cancellation).ConfigureAwait(false);
            using var stream = response.Value.Content;
            await stream.ReadExactlyAsync(buffer[..count], cancellation).ConfigureAwait(false);
            return count;
        }
        catch (RequestFailedException error) when (error.Status is 404 or 412 or 416)
        { throw new IOException("Selected checkpoint file is no longer available.", error); }
    }

    public async ValueTask EnsurePureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation)
    {
        using var upload = new Upload(Blob(id, ".pvl"), authority, cancellation);
        var buffer = ArrayPool<byte>.Shared.Rent(256 * 1024);
        try
        {
            for (ulong offset = 0; offset < source.Length;)
            {
                var count = (int)Math.Min((ulong)buffer.Length, source.Length - offset);
                source.File.RandomRead(buffer.AsSpan(0, count), offset, false);
                upload.Write(buffer.AsSpan(0, count), offset);
                offset += (uint)count;
            }
            await upload.CommitAsync().ConfigureAwait(false);
        }
        finally { ArrayPool<byte>.Shared.Return(buffer); }
    }

    public async ValueTask PublishKeyIndexAsync(uint id, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> map,
        CancellationToken cancellation)
    {
        using var upload = new Upload(Blob(id, ".kvi"), authority, cancellation);
        // Native serialization is synchronous. Its bounded stream stages blocks on this publication lane,
        // without a local staging file, a second serializer or an application-commit dependency.
        using (var writer = new PositionLessStreamWriter(upload, () => { })) snapshot.WriteTo(writer, 0, map, cancellation);
        await upload.CommitAsync().ConfigureAwait(false);
    }

    sealed class Upload(BlockBlobClient blob, LeaseAuthority? authority, CancellationToken cancellation) : IPositionLessStream
    {
        const int BlockSize = 4 * 1024 * 1024;
        readonly byte[] _buffer = ArrayPool<byte>.Shared.Rent(BlockSize);
        readonly List<string> _blocks = new();
        readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        int _filled;
        ulong _length;

        void RequireAuthority()
        {
            cancellation.ThrowIfCancellationRequested();
            if (authority is not { IsValid: true }) throw new InvalidOperationException("Checkpoint publication requires live authority.");
        }

        public void Write(ReadOnlySpan<byte> data, ulong position)
        {
            RequireAuthority();
            if (position != _length) throw new NotSupportedException("Checkpoint output must be sequential.");
            _hash.AppendData(data);
            _length += (uint)data.Length;
            while (!data.IsEmpty)
            {
                var count = Math.Min(data.Length, BlockSize - _filled);
                data[..count].CopyTo(_buffer.AsSpan(_filled));
                _filled += count;
                data = data[count..];
                if (_filled == BlockSize) Flush();
            }
        }

        public void Flush()
        {
            if (_filled == 0) return;
            RequireAuthority();
            var id = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
            using var stream = new MemoryStream(_buffer, 0, _filled, false);
            blob.StageBlock(id, stream, cancellationToken: cancellation);
            _blocks.Add(id);
            _filled = 0;
        }

        public async ValueTask CommitAsync()
        {
            Flush();
            RequireAuthority();
            var sha = Convert.ToHexString(_hash.GetHashAndReset());
            try
            {
                await blob.CommitBlockListAsync(_blocks, new CommitBlockListOptions
                {
                    Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All },
                    Metadata = new Dictionary<string, string> { ["btdb_sha256"] = sha }
                }, cancellation).ConfigureAwait(false);
                return;
            }
            catch (RequestFailedException error) when (error.Status is 409 or 412) { }
            catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation)) { }
            // A lost reply or conditional rejection can both mean our intended immutable file already exists.
            var observed = await blob.GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false);
            if (observed.Value.ContentLength != checked((long)_length) ||
                !observed.Value.Metadata.TryGetValue("btdb_sha256", out var actual) || !sha.Equals(actual, StringComparison.OrdinalIgnoreCase))
            {
                authority!.Fence();
                throw new RemoteFileConflictException();
            }
        }

        public int Read(Span<byte> data, ulong position) => throw new NotSupportedException();
        public void SetSize(ulong size) { if (size != 0 || _length != 0) throw new NotSupportedException(); }
        public ulong GetSize() => _length;
        public void HardFlush() => Flush();
        public void Dispose() { _hash.Dispose(); ArrayPool<byte>.Shared.Return(_buffer); }
    }
}
