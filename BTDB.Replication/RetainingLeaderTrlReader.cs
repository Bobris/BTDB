using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>
/// Retains the leader TRL bytes one follower session received (inline with polls or by range), so the schema scan,
/// the byte comparison and later steps never fetch the same bytes twice, even while local execution lags. Complete
/// leader bytes never change within one leader session; the owner creates a new reader per peer session and releases
/// bytes both consumers have passed. Retention is bounded; reads beyond it go to the inner reader. Not thread-safe:
/// the owner runs the scan and the comparison sequentially.
/// </summary>
internal sealed class RetainingLeaderTrlReader(ILeaderTrlReader inner, int capacity = 4 * 1024 * 1024) : ILeaderTrlReader
{
    readonly List<(uint FileId, ulong Offset, ReadOnlyMemory<byte> Bytes)> _chunks = new();
    int _retained;

    public int Available => capacity - _retained;

    /// <summary>Retain bytes the leader already returned with a poll, within the same capacity.</summary>
    public void Retain(IReadOnlyList<ReplicationPeerTrlChunk> chunks)
    {
        foreach (var chunk in chunks)
        {
            if (chunk.Bytes.Length > Available) return;
            Add(chunk.FileId, chunk.Offset, chunk.Bytes);
        }
    }

    /// <summary>Drop chunks that end at or before position; neither the scan nor the comparison reads them again.</summary>
    public void Release(TransactionLogPosition position)
    {
        _chunks.RemoveAll(chunk =>
        {
            var release = chunk.FileId < position.FileId ||
                chunk.FileId == position.FileId && chunk.Offset + (ulong)chunk.Bytes.Length <= position.Offset;
            if (release) _retained -= chunk.Bytes.Length;
            return release;
        });
    }

    /// <summary>The first position at or after from, within the same file, not already retained.</summary>
    public TransactionLogPosition ContiguousEnd(TransactionLogPosition from)
    {
        var offset = (ulong)from.Offset;
        for (var extended = true; extended;)
        {
            extended = false;
            foreach (var (fileId, chunkOffset, bytes) in _chunks)
            {
                var end = chunkOffset + (ulong)bytes.Length;
                if (fileId != from.FileId || chunkOffset > offset || end <= offset) continue;
                offset = end;
                extended = true;
            }
        }
        return new(from.FileId, (uint)offset);
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
