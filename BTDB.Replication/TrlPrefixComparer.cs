using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

internal enum TrlCompareResult { Matched, LocalBehind, Diverged }

/// <summary>A range reader bound to one authenticated leader/database session. Reads must reject stale sessions
/// and unavailable files, return at most the requested length, and return zero only at file EOF. The owner retains
/// requested native bytes and cancels outstanding calls when the leader session changes. No Blob inventory is used.</summary>
internal interface ILeaderTrlReader
{
    ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation);
}

/// <summary>
/// One follower database/session and capture consumer. Compare only against the current leader's advertised complete
/// cut. A byte match is not a confirmation grant or Blob durability acknowledgement. Blob validation belongs to
/// becoming leader (and bootstrap/recovery), not routine follower comparison. No native headers/commands are decoded.
/// </summary>
internal sealed class TrlPrefixComparer(IFileCollection local, TransactionLogCapture capture)
{
    readonly SemaphoreSlim _lane = new(1);
    bool _diverged;

    public async ValueTask<TrlCompareResult> CompareAsync(ILeaderTrlReader leader, TransactionLogPosition end,
        CancellationToken cancellation = default)
    {
        if (end.FileId == 0 || end.Offset == 0) throw new ArgumentOutOfRangeException(nameof(end));
        await _lane.WaitAsync(cancellation).ConfigureAwait(false);
        byte[]? localBuffer = null;
        byte[]? remoteBuffer = null;
        try
        {
            if (_diverged) return TrlCompareResult.Diverged;
            var start = capture.Acknowledged;
            if (Order(end) <= Order(start)) return TrlCompareResult.Matched;
            if (Order(end) > Order(capture.Completed)) return TrlCompareResult.LocalBehind;
            if (start.FileId == 0) throw new InvalidOperationException("Comparison requires a retained native starting position.");

            const int blockSize = 64 * 1024;
            localBuffer = ArrayPool<byte>.Shared.Rent(blockSize);
            remoteBuffer = ArrayPool<byte>.Shared.Rent(blockSize);
            var fileId = start.FileId;
            while (true)
            {
                cancellation.ThrowIfCancellationRequested();
                var source = local.GetFile(fileId)
                    ?? throw new FileNotFoundException("Missing retained local TRL for comparison.", fileId.ToString());
                var offset = fileId == start.FileId ? (ulong)start.Offset : 0;
                var limit = fileId == end.FileId ? end.Offset : source.GetSize();
                if (offset > limit || source.GetSize() < limit) return Diverged();
                while (offset < limit)
                {
                    var count = (int)Math.Min((ulong)blockSize, limit - offset);
                    source.RandomRead(localBuffer.AsSpan(0, count), offset, false);
                    var filled = 0;
                    while (filled < count)
                    {
                        cancellation.ThrowIfCancellationRequested();
                        var read = await leader.ReadAsync(fileId, offset + (uint)filled,
                            remoteBuffer.AsMemory(filled, count - filled), cancellation).ConfigureAwait(false);
                        cancellation.ThrowIfCancellationRequested();
                        if (read <= 0 || read > count - filled)
                            throw new IOException("Remote TRL comparison read is truncated or invalid.");
                        filled += read;
                    }
                    if (!localBuffer.AsSpan(0, count).SequenceEqual(remoteBuffer.AsSpan(0, count))) return Diverged();
                    offset += (uint)count;
                }
                if (fileId == end.FileId) break;
                // The prior file is sealed on both nodes. A longer leader file is a different byte stream,
                // not permission to silently skip its remaining bytes when moving to the next native ID.
                var extra = await leader.ReadAsync(fileId, limit, remoteBuffer.AsMemory(0, 1), cancellation)
                    .ConfigureAwait(false);
                cancellation.ThrowIfCancellationRequested();
                if (extra < 0 || extra > 1) throw new IOException("Invalid leader TRL read length.");
                if (extra != 0) return Diverged();
                var next = (ulong)fileId + ((fileId & 1) == 0 ? 1ul : 2ul);
                if (next > end.FileId) throw new IOException("Leader cut is outside the native TRL sequence.");
                fileId = (uint)next;
            }
            cancellation.ThrowIfCancellationRequested();
            capture.Acknowledge(end);
            return TrlCompareResult.Matched;
        }
        finally
        {
            if (localBuffer != null) ArrayPool<byte>.Shared.Return(localBuffer);
            if (remoteBuffer != null) ArrayPool<byte>.Shared.Return(remoteBuffer);
            _lane.Release();
        }
    }

    TrlCompareResult Diverged()
    {
        _diverged = true;
        return TrlCompareResult.Diverged;
    }

    static ulong Order(TransactionLogPosition position) => ((ulong)position.FileId << 32) | position.Offset;
}
