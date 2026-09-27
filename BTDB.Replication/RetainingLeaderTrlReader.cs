using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>
/// Retains leader TRL bytes fetched during one follower step, so the schema scan and the byte comparison of the same
/// range cross the network once. Complete leader bytes never change within one leader session, and the owner clears
/// the retained bytes after each step, so every later step fetches (and rechecks the session) again. Retention is
/// bounded; reads beyond it go to the inner reader. Not thread-safe: the owner runs the scan and the comparison
/// sequentially.
/// </summary>
internal sealed class RetainingLeaderTrlReader(ILeaderTrlReader inner, int capacity = 4 * 1024 * 1024) : ILeaderTrlReader
{
    readonly List<(uint FileId, ulong Offset, byte[] Bytes)> _chunks = new();
    int _retained;

    public void Clear()
    {
        _chunks.Clear();
        _retained = 0;
    }

    public async ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        foreach (var (chunkFileId, chunkOffset, bytes) in _chunks)
        {
            if (chunkFileId != fileId || offset < chunkOffset || offset - chunkOffset >= (ulong)bytes.Length) continue;
            var start = (int)(offset - chunkOffset);
            var count = Math.Min(destination.Length, bytes.Length - start);
            bytes.AsSpan(start, count).CopyTo(destination.Span);
            return count;
        }
        var read = await inner.ReadAsync(fileId, offset, destination, cancellation).ConfigureAwait(false);
        if (read > 0 && read <= destination.Length && _retained + read <= capacity)
        {
            _chunks.Add((fileId, offset, destination[..read].ToArray()));
            _retained += read;
        }
        return read;
    }
}
