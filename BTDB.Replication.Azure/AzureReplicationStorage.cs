using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
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
    // "{prefix}/": maintenance lists this whole namespace, so it must never be shared with another database.
    readonly string root;
    readonly CanonicalTrlInventory? canonical;
    readonly LeaseAuthority? authority;
    readonly TimeProvider timeProvider;
    const string DeleteAfterKey = "btdb_delete_after";
    const string RecoveryKey = "btdb_recovery_key";
    const string RecoveryId = "btdb_recovery_id";

    public AzureReplicationStorage(BlobContainerClient container, string prefix, TimeProvider? timeProvider = null)
    {
        this.container = container ?? throw new ArgumentNullException(nameof(container));
        this.prefix = prefix ?? throw new ArgumentNullException(nameof(prefix));
        if (prefix.TrimEnd('/').Length == 0)
            throw new ArgumentException("A database needs its own nonempty prefix; cleanup lists everything below it.", nameof(prefix));
        root = prefix.TrimEnd('/') + "/";
        this.timeProvider = timeProvider ?? TimeProvider.System;
    }

    AzureReplicationStorage(AzureReplicationStorage storage, CanonicalTrlInventory inventory, LeaseAuthority? authority)
        : this(storage.container, storage.prefix, storage.timeProvider)
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

    // Reuse committed blocks and stage only the appended suffix. Partial trailing blocks are merged into full blocks
    // once they would reach BlockSize or MaxTrailingBlocks, bounding both the block count and re-uploaded bytes.
    // Unique staged IDs keep stale requests from changing a winning intent. The final CAS installs bytes and
    // metadata atomically.
    const int BlockSize = 4 * 1024 * 1024;
    const int MaxTrailingBlocks = 64;

    // The committed block list of this adapter's latest canonical TRL commit. The publisher's next append expects
    // exactly that version, so it skips re-reading the list; a stale entry only fails that append's CAS.
    sealed record CommittedBlocks(string Key, string Token, (string Name, long Size)[] Blocks);
    volatile CommittedBlocks? _lastCommit;

    BlockBlobClient Blob(string key)
    {
        TrlMetadata.Validate(new(key, 1));
        return container.GetBlockBlobClient(root + key);
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
        var blocks = new List<(string Name, long Size)>();
        ulong offset = 0;
        var commitDispatched = false;
        try
        {
            if (write.ExpectedToken is { } token)
            {
                conditions.IfMatch = new ETag(token);
                (string Name, long Size)[] committed;
                if (_lastCommit is { } cached && cached.Key == write.Key && cached.Token == token) committed = cached.Blocks;
                else
                {
                    var list = await blob.GetBlockListAsync(BlockListTypes.Committed, cancellationToken: cancellation)
                        .ConfigureAwait(false);
                    if (list.GetRawResponse().Headers.ETag?.ToString().Trim('"') != token.Trim('"')) return new(TrlWriteOutcome.Rejected);
                    committed = list.Value.CommittedBlocks.Select(b => (b.Name, b.SizeLong)).ToArray();
                }
                if (committed.Sum(b => b.Size) != (long)write.ExpectedLength) return new(TrlWriteOutcome.Rejected);
                var fullBlocks = 0;
                while (fullBlocks < committed.Length && committed[fullBlocks].Size == BlockSize) fullBlocks++;
                var trailingBytes = committed.Skip(fullBlocks).Sum(b => b.Size);
                // Metadata-only adoption keeps every block. A merge restages the trailing bytes from the caller's
                // verified native prefix.
                var kept = write.Length == write.ExpectedLength ||
                           (committed.Length - fullBlocks < MaxTrailingBlocks && (ulong)trailingBytes + write.AppendLength < BlockSize)
                    ? committed.Length : fullBlocks;
                for (var i = 0; i < kept; i++)
                {
                    blocks.Add(committed[i]);
                    offset += (ulong)committed[i].Size;
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
                    blocks.Add((id, count));
                    offset += (uint)count;
                }
            }
            finally { ArrayPool<byte>.Shared.Return(buffer); }
            commitDispatched = true;
            var result = await blob.CommitBlockListAsync(blocks.Select(b => b.Name), new CommitBlockListOptions
            {
                Conditions = conditions,
                Metadata = new Dictionary<string, string>(write.Metadata.Encode()) { ["btdb_file_id"] = write.FileId.ToString(System.Globalization.CultureInfo.InvariantCulture) }
            }, cancellation).ConfigureAwait(false);
            var applied = result.Value.ETag.ToString();
            _lastCommit = new(write.Key, applied, blocks.ToArray());
            return new(TrlWriteOutcome.Applied, new(applied, write.Length, write.Metadata));
        }
        catch (RequestFailedException error) when (error.Status is 404 or 409 or 412)
        { return new(TrlWriteOutcome.Rejected); }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        {
            if (commitDispatched) return new(TrlWriteOutcome.Ambiguous);
            throw new IOException("Azure canonical TRL staging failed.", error);
        }
    }
    string Directory => root + "files/";

    // Immutable files are "{id}.pvl" or "{id}.kvi" with a nonzero decimal ID; anything else is ignored.
    static bool TryParseFileName(string name, out uint id, out KVFileType type)
    {
        var dot = name.LastIndexOf('.');
        type = dot <= 0 ? KVFileType.Unknown : name[dot..] switch
        {
            ".pvl" => KVFileType.PureValues, ".kvi" => KVFileType.KeyIndex, _ => KVFileType.Unknown
        };
        id = 0;
        return type != KVFileType.Unknown &&
               uint.TryParse(name.AsSpan(0, dot), NumberStyles.None, CultureInfo.InvariantCulture, out id) && id != 0;
    }
    BlockBlobClient Blob(uint id, string extension) => container.GetBlockBlobClient(Directory + id.ToString(CultureInfo.InvariantCulture) + extension);

    public async IAsyncEnumerable<RemoteFile> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        await foreach (var file in Inventory.EnumerateAsync(cancellation).ConfigureAwait(false)) yield return file;
        await foreach (var blob in ListAsync(Directory, cancellation).ConfigureAwait(false))
        {
            if (!TryParseFileName(blob.Name[Directory.Length..], out var id, out var type)) continue;
            yield return new(id, type, checked((ulong)blob.Properties.ContentLength!.Value),
                blob.Properties.ETag!.Value.ToString(), true, blob.Metadata.TryGetValue("btdb_sha256", out var sha) ? sha : null);
        }
    }

    // Listing pages are remote requests too: transient failures must stay retryable I/O, never fatal SDK exceptions.
    async IAsyncEnumerable<BlobItem> ListAsync(string listPrefix, [EnumeratorCancellation] CancellationToken cancellation)
    {
        await using var blobs = container.GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, prefix: listPrefix,
            cancellationToken: cancellation).GetAsyncEnumerator(cancellation);
        while (true)
        {
            bool next;
            try { next = await blobs.MoveNextAsync().ConfigureAwait(false); }
            catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
            { throw new IOException("Azure blob listing failed.", error); }
            if (!next) yield break;
            yield return blobs.Current;
        }
    }

    void RequireAuthority(CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        if (authority is not { IsValid: true }) throw new InvalidOperationException("Remote maintenance requires live authority.");
    }

    public async IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        await foreach (var blob in ListAsync(root, cancellation).ConfigureAwait(false))
        {
            var key = blob.Name[root.Length..];
            if (key.StartsWith("files/", StringComparison.Ordinal))
            {
                if (TryParseFileName(key[6..], out var id, out var type))
                    yield return new(key, id, type, blob.Properties.ETag!.Value.ToString(), false, DeletionTime(blob.Metadata));
            }
            else if (blob.Metadata.ContainsKey("btdb_term") && blob.Metadata.TryGetValue("btdb_file_id", out var value) &&
                     uint.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var id) && id != 0)
                yield return new(key, id, KVFileType.TransactionLog, blob.Properties.ETag!.Value.ToString(), false, DeletionTime(blob.Metadata));
        }
    }

    static DateTimeOffset? DeletionTime(IDictionary<string, string> metadata)
    {
        if (!metadata.TryGetValue(DeleteAfterKey, out var value)) return null;
        if (!DateTimeOffset.TryParseExact(value, "O", CultureInfo.InvariantCulture, DateTimeStyles.None, out var time))
            throw new InvalidDataException("Invalid deletion deadline metadata.");
        return time;
    }

    public async ValueTask<RemoteMaintenanceFile> ScheduleDeletionAsync(RemoteMaintenanceFile file, TimeSpan delay, CancellationToken cancellation)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(delay.Ticks);
        TrlMetadata.Validate(new(file.Key, file.FileId));
        RequireAuthority(cancellation);
        var blob = container.GetBlobClient(root + file.Key);
        try
        {
            var properties = (await blob.GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false)).Value;
            if (properties.ETag.ToString().Trim('"') != file.Version.Trim('"')) return file;
            if (DeletionTime(properties.Metadata) is { } existing) return file with { DeleteAfter = existing };
            var deadline = timeProvider.GetUtcNow() + delay;
            var metadata = new Dictionary<string, string>(properties.Metadata) { [DeleteAfterKey] = deadline.ToString("O", CultureInfo.InvariantCulture) };
            RequireAuthority(cancellation);
            var result = await blob.SetMetadataAsync(metadata, new BlobRequestConditions { IfMatch = properties.ETag }, cancellation).ConfigureAwait(false);
            return file with { Version = result.Value.ETag.ToString(), DeleteAfter = deadline };
        }
        catch (RequestFailedException error) when (error.Status is 404 or 412) { return file; }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        { throw new IOException("Deletion marking is unresolved; reread before retry.", error); }
    }

    public async ValueTask CancelDeletionAsync(RemoteMaintenanceFile file, CancellationToken cancellation)
    {
        RequireAuthority(cancellation);
        var blob = Blob(file.Key);
        try
        {
            var properties = (await blob.GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false)).Value;
            if (properties.ETag.ToString().Trim('"') != file.Version.Trim('"')) throw new IOException("Retained file changed during protection.");
            // Rewriting unmarked metadata would only change the version that restores and validations read by.
            if (DeletionTime(properties.Metadata) == null) return;
            var metadata = new Dictionary<string, string>(properties.Metadata);
            metadata.Remove(DeleteAfterKey);
            RequireAuthority(cancellation);
            await blob.SetMetadataAsync(metadata, new BlobRequestConditions { IfMatch = properties.ETag }, cancellation).ConfigureAwait(false);
        }
        catch (RequestFailedException error) when (error.Status is 404 or 412)
        { throw new IOException("Retained file disappeared or changed during protection.", error); }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        { throw new IOException("Deletion cancellation is unresolved.", error); }
    }

    public async ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation)
    {
        TrlMetadata.Validate(new(file.Key, file.FileId));
        RequireAuthority(cancellation);
        if (file.DeleteAfter is not { } due || timeProvider.GetUtcNow() < due) return;
        var blob = container.GetBlobClient(root + file.Key);
        try
        {
            var properties = (await blob.GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false)).Value;
            if (properties.ETag.ToString().Trim('"') != file.Version.Trim('"') || DeletionTime(properties.Metadata) is not { } deadline ||
                timeProvider.GetUtcNow() < deadline) return;
            RequireAuthority(cancellation);
            await blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots,
                new BlobRequestConditions { IfMatch = properties.ETag }, cancellation).ConfigureAwait(false);
        }
        catch (RequestFailedException error) when (error.Status is 404 or 412) { }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        { throw new IOException("Remote cleanup result is unresolved.", error); }
    }

    public async ValueTask<TrlSuccessor> ResolveRecoveryRootAsync(TrlSuccessor genesis, CancellationToken cancellation)
    {
        // Only an atomically published immutable KVI can select a new retained root. Never pick a maximum TRL ID.
        uint latest = 0;
        TrlSuccessor? root = null;
        await foreach (var blob in ListAsync(Directory, cancellation).ConfigureAwait(false))
        {
            if (!TryParseFileName(blob.Name[Directory.Length..], out var id, out var type) ||
                type != KVFileType.KeyIndex || id <= latest) continue;
            latest = id;
            root = null;
            if (blob.Metadata.TryGetValue(RecoveryKey, out var key) && blob.Metadata.TryGetValue(RecoveryId, out var value) &&
                uint.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var rootId) && rootId != 0 &&
                blob.Metadata.ContainsKey("btdb_sha256"))
            {
                try { root = new(Encoding.UTF8.GetString(Convert.FromBase64String(key)), rootId); }
                catch (FormatException error) { throw new InvalidDataException("Invalid checkpoint recovery root.", error); }
                TrlMetadata.Validate(root);
            }
        }
        // Legacy KVI lacks a retained-root hint: preserve the original discovery contract.
        if (latest != 0 && root == null && await ReadAsync(genesis.Key, cancellation).ConfigureAwait(false) == null)
            throw new IOException("A checkpoint exists but its legacy discovery root is missing.");
        return root ?? genesis;
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
            await blob.SetMetadataAsync(properties.Metadata.Where(p => p.Key != DeleteAfterKey).ToDictionary(), new BlobRequestConditions { IfMatch = properties.ETag }, cancellation)
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
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        { throw new IOException("Azure checkpoint file read failed.", error); }
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
        var first = snapshot.Sources.Where(s => s.FileType == KVFileType.TransactionLog).Select(s => s.FileId)
            .Append(snapshot.TransactionLogFileId).Min();
        var selected = await CanonicalTrlInventory.DiscoverAsync(this, Inventory.Root, cancellation).ConfigureAwait(false);
        var recoveryRoot = selected.GetHead(first);
        // A promoted follower may reference older sealed TRLs too. Clear deletion marks on the retained chain before
        // publishing a KVI that depends on it. Unmarked files keep their version, so concurrent restores reading
        // them by ETag are not invalidated; a positive deletion delay keeps any late stale mark from becoming due
        // before later cleanup of this leader clears it.
        var marked = new Dictionary<string, RemoteMaintenanceFile>(StringComparer.Ordinal);
        await foreach (var file in EnumerateMaintenanceAsync(cancellation).ConfigureAwait(false))
            if (file.FileType == KVFileType.TransactionLog && file.DeleteAfter != null) marked[file.Key] = file;
        await foreach (var file in selected.EnumerateAsync(cancellation).ConfigureAwait(false))
            if (file.FileId >= first && file.IsSealed && marked.TryGetValue(selected.GetHead(file.FileId).Key, out var mark))
                await CancelDeletionAsync(mark, cancellation).ConfigureAwait(false);
        using var upload = new Upload(Blob(id, ".kvi"), authority, cancellation,
            new Dictionary<string, string> { [RecoveryKey] = Convert.ToBase64String(Encoding.UTF8.GetBytes(recoveryRoot.Key)), [RecoveryId] = first.ToString(CultureInfo.InvariantCulture) });
        // Native serialization is synchronous. Its bounded stream stages blocks on this publication lane,
        // without a local staging file, a second serializer or an application-commit dependency.
        using (var writer = new PositionLessStreamWriter(upload, () => { })) snapshot.WriteTo(writer, 0, map, cancellation);
        await upload.CommitAsync().ConfigureAwait(false);
    }

    sealed class Upload(BlockBlobClient blob, LeaseAuthority? authority, CancellationToken cancellation,
        Dictionary<string, string>? extraMetadata = null) : IPositionLessStream
    {
        const int BlockSize = 4 * 1024 * 1024;
        // Blocks staged concurrently; each staged block keeps its own pooled buffer until its request completes.
        const int ParallelStages = 4;
        byte[] _buffer = ArrayPool<byte>.Shared.Rent(BlockSize);
        readonly List<string> _blocks = new();
        readonly Queue<(Task Stage, byte[] Buffer)> _staging = new();
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

        // Callers write synchronously (native KVI serialization); only a full staging window waits.
        public void Flush()
        {
            if (_filled == 0) return;
            RequireAuthority();
            if (_staging.Count == ParallelStages) Complete(_staging.Dequeue()).GetAwaiter().GetResult();
            var id = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
            _staging.Enqueue((StageAsync(id, _buffer, _filled), _buffer));
            _blocks.Add(id);
            _buffer = ArrayPool<byte>.Shared.Rent(BlockSize);
            _filled = 0;
        }

        async Task StageAsync(string id, byte[] buffer, int count)
        {
            using var stream = new MemoryStream(buffer, 0, count, false);
            try { await blob.StageBlockAsync(id, stream, cancellationToken: cancellation).ConfigureAwait(false); }
            catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
            { throw new IOException("Azure immutable file staging failed.", error); }
        }

        static async Task Complete((Task Stage, byte[] Buffer) staged)
        {
            try { await staged.Stage.ConfigureAwait(false); }
            finally { ArrayPool<byte>.Shared.Return(staged.Buffer); }
        }

        public async ValueTask CommitAsync()
        {
            Flush();
            while (_staging.TryDequeue(out var staged)) await Complete(staged).ConfigureAwait(false);
            RequireAuthority();
            var sha = Convert.ToHexString(_hash.GetHashAndReset());
            try
            {
                await blob.CommitBlockListAsync(_blocks, new CommitBlockListOptions
                {
                    Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All },
                    Metadata = new Dictionary<string, string>(extraMetadata ?? new()) { ["btdb_sha256"] = sha }
                }, cancellation).ConfigureAwait(false);
                return;
            }
            catch (RequestFailedException error) when (error.Status is 409 or 412) { }
            catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation)) { }
            // A lost reply or conditional rejection can both mean our intended immutable file already exists.
            Response<BlobProperties> observed;
            try { observed = await blob.GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false); }
            catch (RequestFailedException error) when (error.Status == 404)
            { throw new IOException("Immutable file commit is unresolved; retry the same identity.", error); }
            catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
            { throw new IOException("Immutable file commit is unresolved; retry the same identity.", error); }
            if (observed.Value.ContentLength != checked((long)_length) ||
                !observed.Value.Metadata.TryGetValue("btdb_sha256", out var actual) || !sha.Equals(actual, StringComparison.OrdinalIgnoreCase) ||
                (extraMetadata != null && extraMetadata.Any(p => !observed.Value.Metadata.TryGetValue(p.Key, out var value) || value != p.Value)))
            {
                authority?.Fence();
                throw new RemoteFileConflictException();
            }
        }

        public int Read(Span<byte> data, ulong position) => throw new NotSupportedException();
        public void SetSize(ulong size) { if (size != 0 || _length != 0) throw new NotSupportedException(); }
        public ulong GetSize() => _length;
        public void HardFlush() => Flush();
        public void Dispose()
        {
            // A failed upload still drains its outstanding requests before their buffers return to the pool.
            while (_staging.TryDequeue(out var staged))
            {
                ((Task)staged.Stage).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
                ArrayPool<byte>.Shared.Return(staged.Buffer);
            }
            _hash.Dispose();
            ArrayPool<byte>.Shared.Return(_buffer);
        }
    }
}
