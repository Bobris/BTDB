using System;
using System.Buffers;
using System.Collections.Concurrent;
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
    // Newer versions of listed PVL/KVI objects verified to hold the same content (see ReadAsync).
    readonly ConcurrentDictionary<(uint, KVFileType), ETag> followed = new();
    const string DeleteAfterKey = "btdb_delete_after";
    const string Sha256Key = "btdb_sha256";

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
        TrlFileName.ValidateKey(key);
        return container.GetBlockBlobClient(root + key);
    }

    public async ValueTask<TrlObjectState?> ReadAsync(string key, CancellationToken cancellation)
    {
        TrlFileName.FileIdFromKey(key);
        try
        {
            var result = await Blob(key).GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false);
            return new(result.Value.ETag.ToString(), checked((uint)result.Value.ContentLength),
                result.Value.Metadata.TryGetValue(Sha256Key, out var sha) ? sha : null);
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
        TrlFileName.Validate(new(write.Key, write.FileId));
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
                // Unchanged-content adoption keeps every block. A merge restages the trailing bytes from the caller's
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
            // Stage several blocks at once: one request at a time capped a backlog at about 110 MiB/s (measured).
            var staging = new Queue<(Task Stage, byte[] Buffer)>();
            try
            {
                while (offset < write.Length)
                {
                    if (staging.Count == ParallelStages) await CompleteStage(staging.Dequeue()).ConfigureAwait(false);
                    var count = (int)Math.Min((ulong)BlockSize, write.Length - offset);
                    var buffer = ArrayPool<byte>.Shared.Rent(BlockSize);
                    write.Source.RandomRead(buffer.AsSpan(0, count), offset, false);
                    var id = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
                    staging.Enqueue((StageBlockAsync(blob, id, buffer, count, cancellation), buffer));
                    blocks.Add((id, count));
                    offset += (uint)count;
                }
                while (staging.TryDequeue(out var staged)) await CompleteStage(staged).ConfigureAwait(false);
            }
            finally
            {
                // A failed stage still drains its siblings before their buffers return to the pool.
                while (staging.TryDequeue(out var staged))
                {
                    await staged.Stage.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                    ArrayPool<byte>.Shared.Return(staged.Buffer);
                }
            }
            commitDispatched = true;
            // Every commit replaces the metadata: only the write that seals the TRL records its checksum.
            var result = await blob.CommitBlockListAsync(blocks.Select(b => b.Name), new CommitBlockListOptions
            {
                Conditions = conditions,
                Metadata = write.Sha256 is { } sha ? new Dictionary<string, string> { [Sha256Key] = sha } : new Dictionary<string, string>()
            }, cancellation).ConfigureAwait(false);
            var applied = result.Value.ETag.ToString();
            _lastCommit = new(write.Key, applied, blocks.ToArray());
            return new(TrlWriteOutcome.Applied, new(applied, write.Length, write.Sha256));
        }
        catch (RequestFailedException error) when (error.Status is 404 or 409 or 412)
        { return new(TrlWriteOutcome.Rejected); }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        {
            if (commitDispatched) return new(TrlWriteOutcome.Ambiguous);
            throw new IOException("Azure canonical TRL staging failed.", error);
        }
    }
    const int ParallelStages = 4;

    static async Task StageBlockAsync(BlockBlobClient blob, string id, byte[] buffer, int count, CancellationToken cancellation)
    {
        using var stream = new MemoryStream(buffer, 0, count, false);
        await blob.StageBlockAsync(id, stream, cancellationToken: cancellation).ConfigureAwait(false);
    }

    static async Task CompleteStage((Task Stage, byte[] Buffer) staged)
    {
        try { await staged.Stage.ConfigureAwait(false); }
        finally { ArrayPool<byte>.Shared.Return(staged.Buffer); }
    }

    // Native files use a nonzero decimal ID and .trl, .pvl or .kvi.
    static bool TryParseFileName(string name, out uint id, out KVFileType type)
    {
        var dot = name.LastIndexOf('.');
        type = dot <= 0 ? KVFileType.Unknown : name[dot..] switch
        {
            ".pvl" => KVFileType.PureValues, ".kvi" => KVFileType.KeyIndex, ".trl" => KVFileType.TransactionLog, _ => KVFileType.Unknown
        };
        id = 0;
        return type != KVFileType.Unknown &&
               uint.TryParse(name.AsSpan(0, dot), NumberStyles.None, CultureInfo.InvariantCulture, out id) && id != 0;
    }
    BlockBlobClient Blob(uint id, string extension) => container.GetBlockBlobClient(root + id.ToString(CultureInfo.InvariantCulture) + extension);

    public async IAsyncEnumerable<RemoteFile> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        await foreach (var file in Inventory.EnumerateAsync(cancellation).ConfigureAwait(false)) yield return file;
        await foreach (var blob in ListAsync(root, cancellation).ConfigureAwait(false))
        {
            if (!TryParseFileName(blob.Name[root.Length..], out var id, out var type) || type == KVFileType.TransactionLog) continue;
            yield return new(id, type, checked((ulong)blob.Properties.ContentLength!.Value),
                blob.Properties.ETag!.Value.ToString(), true, blob.Metadata.TryGetValue(Sha256Key, out var sha) ? sha : null);
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
            if (MaintenanceFile(blob) is { } file) yield return file;
    }

    RemoteMaintenanceFile? MaintenanceFile(BlobItem blob)
    {
        var key = blob.Name[root.Length..];
        return TryParseFileName(key, out var id, out var type)
            ? new(key, id, type, blob.Properties.ETag!.Value.ToString(), false, DeletionTime(blob.Metadata)) : null;
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
        TrlFileName.ValidateKey(file.Key);
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
        TrlFileName.ValidateKey(file.Key);
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

    public async IAsyncEnumerable<TrlHead> EnumerateTrlsAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        await foreach (var blob in ListAsync(root, cancellation).ConfigureAwait(false))
        {
            var key = blob.Name[root.Length..];
            if (!TryParseFileName(blob.Name[root.Length..], out var id, out var type) || type != KVFileType.TransactionLog) continue;
            if (TrlFileName.FileIdFromKey(key) != id) throw new InvalidDataException("Invalid TRL object name.");
            yield return new(id, key, new(blob.Properties.ETag!.Value.ToString(), checked((uint)blob.Properties.ContentLength!.Value),
                blob.Metadata.TryGetValue(Sha256Key, out var sha) ? sha : null));
        }
    }

    public async ValueTask<TrlSuccessor> ResolveRecoveryRootAsync(TrlSuccessor genesis, CancellationToken cancellation)
    {
        TrlHead? first = null;
        var hasCheckpoint = false;
        await foreach (var blob in ListAsync(root, cancellation).ConfigureAwait(false))
        {
            if (!TryParseFileName(blob.Name[root.Length..], out var id, out var type)) continue;
            hasCheckpoint |= type == KVFileType.KeyIndex;
            if (type == KVFileType.TransactionLog && (first == null || id < first.FileId))
                first = new(id, TrlFileName.Key(id), new(blob.Properties.ETag!.Value.ToString(), checked((uint)blob.Properties.ContentLength!.Value)));
        }
        if (first == null && hasCheckpoint) throw new IOException("A published checkpoint has no retained TRL history.");
        return first == null ? genesis : new(first.Key, first.FileId);
    }

    public async ValueTask<bool> ProtectPureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation)
    {
        RequireAuthority(cancellation);
        var blob = Blob(id, ".pvl");
        try
        {
            var properties = (await blob.GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false)).Value;
            if ((ulong)properties.ContentLength != source.Length || !properties.Metadata.ContainsKey(Sha256Key))
                throw new RemoteFileConflictException();
            // Contents were verified on upload/download. An unmarked file keeps its version, so restores reading it by
            // ETag continue; an old delete requires a mark, and a late stale mark cannot become due before this
            // leader's cleanup clears it on the retained dependency.
            if (DeletionTime(properties.Metadata) == null) return true;
            // Clearing a mark changes the version, so an older delete of the marked version cannot match.
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
        var blob = Blob(file.FileId, file.FileType == KVFileType.PureValues ? ".pvl" : ".kvi");
        var key = (file.FileId, file.FileType);
        try
        {
            try
            {
                await DownloadAsync(followed.GetValueOrDefault(key, new ETag(file.Version))).ConfigureAwait(false);
            }
            catch (RequestFailedException error) when (error.Status == 412 && file.Sha256 != null)
            {
                // The content is immutable; a deletion mark or its removal changes only metadata and thus the version.
                // The same length and whole-file SHA-256 identify the listed bytes, so a restore overlapping cleanup
                // continues instead of starting over. Any other change still fails the attempt.
                var current = (await blob.GetPropertiesAsync(cancellationToken: cancellation).ConfigureAwait(false)).Value;
                if ((ulong)current.ContentLength != file.Length || !current.Metadata.TryGetValue(Sha256Key, out var sha) ||
                    !sha.Equals(file.Sha256, StringComparison.OrdinalIgnoreCase)) throw;
                followed[key] = current.ETag;
                await DownloadAsync(current.ETag).ConfigureAwait(false);
            }
            return count;
        }
        catch (RequestFailedException error) when (error.Status is 404 or 412 or 416)
        { throw new IOException("Selected checkpoint file is no longer available.", error); }
        catch (Exception error) when (AzureLeaderStorage.IsTransient(error, cancellation))
        { throw new IOException("Azure checkpoint file read failed.", error); }

        async Task DownloadAsync(ETag version)
        {
            var response = await blob.DownloadStreamingAsync(new BlobDownloadOptions
            {
                Range = new HttpRange(checked((long)offset), count),
                Conditions = new BlobRequestConditions { IfMatch = version }
            }, cancellation).ConfigureAwait(false);
            using var stream = response.Value.Content;
            await stream.ReadExactlyAsync(buffer[..count], cancellation).ConfigureAwait(false);
        }
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
        // Native KVI references define its closure. Protect retained TRLs from old deletion marks before publication.
        var required = snapshot.Sources.Where(s => s.FileType == KVFileType.TransactionLog).Select(s => s.FileId)
            .Append(snapshot.TransactionLogFileId).ToHashSet();
        await foreach (var file in EnumerateMaintenanceAsync(cancellation).ConfigureAwait(false))
        {
            if (file.FileType != KVFileType.TransactionLog) continue;
            required.Remove(file.FileId);
            if (file.FileId >= first && file.DeleteAfter != null)
                await CancelDeletionAsync(file, cancellation).ConfigureAwait(false);
        }
        if (required.Count != 0) throw new FileNotFoundException("A required checkpoint TRL is missing.");
        using var upload = new Upload(Blob(id, ".kvi"), authority, cancellation);
        // Native serialization is synchronous. Its bounded stream stages blocks on this publication lane,
        // without a local staging file, a second serializer or an application-commit dependency.
        using (var writer = new PositionLessStreamWriter(upload, () => { })) snapshot.WriteTo(writer, 0, map, cancellation);
        await upload.CommitAsync().ConfigureAwait(false);
    }

    sealed class Upload(BlockBlobClient blob, LeaseAuthority? authority, CancellationToken cancellation) : IPositionLessStream
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
                    Metadata = new Dictionary<string, string> { [Sha256Key] = sha }
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
                !observed.Value.Metadata.TryGetValue(Sha256Key, out var actual) || !sha.Equals(actual, StringComparison.OrdinalIgnoreCase))
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
