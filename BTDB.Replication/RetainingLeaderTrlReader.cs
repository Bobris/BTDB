using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>
/// Retains the leader TRL bytes one follower session received (inline with polls or by range), so a comparison that
/// stops at a lagging local end never fetches the same bytes again in later steps. Complete leader bytes never change
/// within one leader session; the owner creates a new reader per peer session and releases bytes the comparison has
/// passed. Retention is bounded; reads beyond it go to the inner reader. Not thread-safe: the owner compares serially.
/// </summary>
internal sealed class RetainingLeaderTrlReader(ILeaderTrlReader inner, int capacity = 4 * 1024 * 1024) : ILeaderTrlReader
{
    readonly List<(uint FileId, ulong Offset, ReadOnlyMemory<byte> Bytes)> _chunks = new();
    // Inline chunks continue a later file only after serving the previous one to its end, so a file switch proves
    // that file's end and successor. The comparison's end-of-file probe and the next poll's start then need no
    // peer round trip.
    readonly Dictionary<uint, (ulong End, uint Next)> _ends = new();
    int _retained;

    public int Available => capacity - _retained;

    /// <summary>Retain bytes the leader returned with a poll requested from <paramref name="from"/>, within the same capacity.</summary>
    public void Retain(TransactionLogPosition from, IReadOnlyList<ReplicationPeerTrlChunk> chunks)
    {
        // A first chunk in a later file means the requested file already ended at the requested offset.
        if (chunks.Count != 0 && chunks[0].FileId != from.FileId) _ends[from.FileId] = (from.Offset, chunks[0].FileId);
        for (var i = 0; i + 1 < chunks.Count; i++)
            if (chunks[i + 1].FileId != chunks[i].FileId)
                _ends[chunks[i].FileId] = ((ulong)chunks[i].Offset + (uint)chunks[i].Bytes.Length, chunks[i + 1].FileId);
        foreach (var chunk in chunks)
        {
            if (chunk.Bytes.Length > Available) return;
            Add(chunk.FileId, chunk.Offset, chunk.Bytes);
        }
    }

    /// <summary>Drop chunks that end at or before position; the comparison never reads them again.</summary>
    public void Release(TransactionLogPosition position)
    {
        _chunks.RemoveAll(chunk =>
        {
            var release = chunk.FileId < position.FileId ||
                chunk.FileId == position.FileId && chunk.Offset + (ulong)chunk.Bytes.Length <= position.Offset;
            if (release) _retained -= chunk.Bytes.Length;
            return release;
        });
        foreach (var fileId in _ends.Keys.Where(id => id < position.FileId).ToArray()) _ends.Remove(fileId);
    }

    /// <summary>The first position at or after from not already retained, continuing into a file's known successor.</summary>
    public TransactionLogPosition ContiguousEnd(TransactionLogPosition from)
    {
        var fileId = from.FileId;
        var offset = (ulong)from.Offset;
        while (true)
        {
            for (var extended = true; extended;)
            {
                extended = false;
                foreach (var (chunkFileId, chunkOffset, bytes) in _chunks)
                {
                    var end = chunkOffset + (ulong)bytes.Length;
                    if (chunkFileId != fileId || chunkOffset > offset || end <= offset) continue;
                    offset = end;
                    extended = true;
                }
            }
            if (!_ends.TryGetValue(fileId, out var known) || known.End != offset) return new(fileId, (uint)offset);
            fileId = known.Next;
            offset = 0;
        }
    }

    public async ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        foreach (var (chunkFileId, chunkOffset, bytes) in _chunks)
        {
            if (chunkFileId != fileId || offset < chunkOffset || offset - chunkOffset >= (ulong)bytes.Length) continue;
            var start = (int)(offset - chunkOffset);
            var count = Math.Min(destination.Length, bytes.Length - start);
            bytes.Span.Slice(start, count).CopyTo(destination.Span);
            return count;
        }
        if (_ends.TryGetValue(fileId, out var known) && known.End == offset) return 0;
        var read = await inner.ReadAsync(fileId, offset, destination, cancellation).ConfigureAwait(false);
        if (read > 0 && read <= destination.Length && read <= Available)
            Add(fileId, offset, destination[..read].ToArray());
        return read;
    }

    void Add(uint fileId, ulong offset, ReadOnlyMemory<byte> bytes)
    {
        _chunks.Add((fileId, offset, bytes));
        _retained += bytes.Length;
    }
}
