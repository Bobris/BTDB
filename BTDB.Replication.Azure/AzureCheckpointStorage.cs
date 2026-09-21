using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
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

/// <summary>Immutable numeric PVL/KVI files plus a separately selected canonical TRL inventory.
/// Publication is conditional create with atomic SHA metadata. Restore requires no authority.</summary>
internal sealed class AzureCheckpointStorage(BlobContainerClient container, string prefix,
    IRemoteFileCollection canonical, LeaseAuthority? authority = null) : ICheckpointStorage
{
    string Directory => string.IsNullOrEmpty(prefix) ? "files/" : prefix.TrimEnd('/') + "/files/";
    BlockBlobClient Blob(uint id, string extension) => container.GetBlockBlobClient(Directory + id.ToString(CultureInfo.InvariantCulture) + extension);

    public async IAsyncEnumerable<RemoteFile> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        await foreach (var file in canonical.EnumerateAsync(cancellation).ConfigureAwait(false)) yield return file;
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

    public async ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation)
    {
        if (file.FileType == KVFileType.TransactionLog)
            return await canonical.ReadAsync(file, offset, buffer, cancellation).ConfigureAwait(false);
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
