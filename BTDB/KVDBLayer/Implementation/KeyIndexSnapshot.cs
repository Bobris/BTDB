using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using BTDB.BTreeLib;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

/// <summary>A KVI dependency retained by its snapshot root. Length is the complete PVL size or the required canonical TRL prefix.</summary>
public sealed record KeyIndexFileSource(uint FileId, KVFileType FileType, ulong Length, long Generation,
    IFileCollectionFile File);

/// <summary>
/// A fixed native KVI export root retaining source files through its root. Does not publish a local KVI or alter live value pointers.
/// Use only while the database is alive; serialize publication/disposal externally.
/// </summary>
public sealed class KeyIndexSnapshot : IDisposable
{
    readonly BTreeKeyValueDB _db;
    readonly IRootNode _root;
    bool _disposed;

    internal KeyIndexSnapshot(BTreeKeyValueDB db, IRootNode root, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        _db = db;
        _root = root;
        var lengths = new Dictionary<uint, ulong>();
        if (root.TrLogFileId != 0) lengths.Add(root.TrLogFileId, root.TrLogOffset);
        var context = new KeyValueIterateCtx { CancellationToken = cancellation };
        root.KeyValueIterate(ref context, (ref KeyValueIterateCtx ctx) =>
        {
            var value = ctx.CurrentValue;
            var id = MemoryMarshal.Read<uint>(value);
            if (id == 0) return;
            var offset = MemoryMarshal.Read<uint>(value[4..]);
            var size = MemoryMarshal.Read<int>(value[8..]);
            var end = offset + (ulong)Math.Abs((long)size);
            lengths[id] = Math.Max(lengths.GetValueOrDefault(id), end);
        });
        var sources = new List<KeyIndexFileSource>();
        foreach (var (id, length) in lengths.OrderBy(p => p.Key))
        {
            cancellation.ThrowIfCancellationRequested();
            var info = db.FileCollection.FileInfoByIdx(id);
            if (info?.FileType is not (KVFileType.PureValues or KVFileType.TransactionLog))
                throw new BTDBException("KVI dependency is missing or has an unexpected file type.");
            var file = db.FileCollection.GetFile(id);
            var fileSize = file.GetSize();
            if (fileSize < length) throw new BTDBException("KVI dependency is truncated.");
            sources.Add(new(id, info.FileType, info.FileType == KVFileType.PureValues ? fileSize : length,
                info.Generation, file));
        }
        Sources = sources.AsReadOnly();
    }

    public IReadOnlyList<KeyIndexFileSource> Sources { get; }
    public ulong CommitUlong => _root.CommitUlong;
    public uint TransactionLogFileId => _root.TrLogFileId;
    public uint TransactionLogOffset => _root.TrLogOffset;

    /// <summary>Write native KVI with whole-PVL file-ID substitutions. Offsets and all TRL references stay unchanged.
    /// The forward-only destination can stream chunks directly to Blob Storage without local disk staging.</summary>
    public void WriteTo(IMemWriter destination, long generation,
        IReadOnlyDictionary<uint, uint> pureValueFileIds, CancellationToken cancellation = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellation.ThrowIfCancellationRequested();
        var destinations = new HashSet<uint>();
        var pvlCount = 0;
        foreach (var source in Sources)
        {
            if (generation <= source.Generation) throw new ArgumentOutOfRangeException(nameof(generation));
            var id = source.FileId;
            if (source.FileType == KVFileType.PureValues)
            {
                pvlCount++;
                if (!pureValueFileIds.TryGetValue(id, out id) || id == 0)
                    throw new ArgumentException("Every PVL needs a nonzero remote file ID.", nameof(pureValueFileIds));
            }
            if (!destinations.Add(id))
                throw new ArgumentException("Remote file IDs collide.", nameof(pureValueFileIds));
        }
        if (pureValueFileIds.Count != pvlCount)
            throw new ArgumentException("Only PVL file IDs can be remapped.", nameof(pureValueFileIds));
        _db.WriteKeyIndexFile(_root, destination, generation, cancellation, true, pureValueFileIds);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _db.DereferenceRoot(_root);
    }
}
