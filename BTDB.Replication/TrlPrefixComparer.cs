using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

internal enum TrlCompareResult { Matched, LocalBehind, Diverged }

/// <summary>A range reader bound to one authenticated leader/database session. Reads must reject stale sessions
/// and unavailable files, return at most the requested length, and return zero only at the end of the available complete prefix. The owner retains
/// requested native bytes and cancels outstanding calls when the leader session changes. No Blob inventory is used.</summary>
internal interface ILeaderTrlReader
{
    ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation);
}

/// <summary>
/// One follower database/session, compared serially by its owner. Compare only against the current leader's advertised
/// complete cut, starting at a retained local position. A byte match is not a confirmation grant, Blob durability or
/// canonical history: the owner derives acknowledgement and retention itself. Blob validation belongs to becoming
/// leader (and bootstrap/recovery), not routine follower comparison. No native headers/commands are decoded.
/// </summary>
internal sealed class TrlPrefixComparer(Func<uint, IFileCollectionFile?> getFile, TransactionLogCapture capture,
    TransactionLogPosition compareFrom)
{
    bool _diverged;
    TransactionLogPosition _comparisonPosition = compareFrom;

    /// <summary>Byte position already matched with this leader; not necessarily a transaction boundary. A new
    /// comparison with the same leader may resume here, including after a cancelled comparison.</summary>
    public TransactionLogPosition Position => _comparisonPosition;

    /// <summary>The end of the latest completed comparison: a complete transaction cut, either the leader's advertised
    /// end or, while local execution lags, the complete local prefix that end already covers.</summary>
    public TransactionLogPosition? MatchedThrough { get; private set; }

    public async ValueTask<TrlCompareResult> CompareAsync(ILeaderTrlReader leader, TransactionLogPosition end,
        CancellationToken cancellation = default)
    {
        if (end.FileId == 0 || end.Offset == 0) throw new ArgumentOutOfRangeException(nameof(end));
        byte[]? localBuffer = null;
        byte[]? remoteBuffer = null;
        try
        {
            if (_diverged) return TrlCompareResult.Diverged;
            var start = _comparisonPosition;
            if (end <= start) return TrlCompareResult.Matched;
            // A lagging follower still compares the complete local prefix the leader's cut already covers, so its
            // canonical progress and local TRL retention keep advancing under continuous load.
            var local = capture.Completed;
            var target = end <= local ? end : local;
            if (target <= start) return TrlCompareResult.LocalBehind;
            if (start.FileId == 0) throw new InvalidOperationException("Comparison requires a retained native starting position.");

            // Matches the HTTP transport's maximum range, so each block is one peer round trip.
            const int blockSize = 256 * 1024;
            localBuffer = ArrayPool<byte>.Shared.Rent(blockSize);
            remoteBuffer = ArrayPool<byte>.Shared.Rent(blockSize);
            var fileId = start.FileId;
            uint[]? successors = null;
            var nextSuccessor = 0;
            while (true)
            {
                cancellation.ThrowIfCancellationRequested();
                var source = getFile(fileId)
                    ?? throw new FileNotFoundException("Missing retained local TRL for comparison.", fileId.ToString());
                var offset = fileId == start.FileId ? (ulong)start.Offset : 0;
                var limit = fileId == target.FileId ? target.Offset : source.GetSize();
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
                        // A later advertised file proves this leader file is sealed. Its shorter end is
                        // divergence, not a transient read failure that reconnecting could repair.
                        if (read == 0 && fileId < end.FileId) return Diverged();
                        if (read <= 0 || read > count - filled)
                            throw new IOException("Remote TRL comparison read is truncated or invalid.");
                        // Compare short reads immediately: a different-length value may otherwise reach
                        // leader EOF before the whole local block is filled, hiding an existing mismatch.
                        if (!localBuffer.AsSpan(filled, read).SequenceEqual(remoteBuffer.AsSpan(filled, read)))
                            return Diverged();
                        filled += read;
                    }
                    offset += (uint)count;
                    _comparisonPosition = new(fileId, (uint)offset);
                }
                if (fileId == target.FileId) break;
                // The prior file is sealed on both nodes. A longer leader file is a different byte stream,
                // not permission to silently skip its remaining bytes when moving to the next native ID.
                var extra = await leader.ReadAsync(fileId, limit, remoteBuffer.AsMemory(0, 1), cancellation)
                    .ConfigureAwait(false);
                cancellation.ThrowIfCancellationRequested();
                if (extra < 0 || extra > 1) throw new IOException("Invalid leader TRL read length.");
                if (extra != 0) return Diverged();
                // The local prefix covers the target file; its headers give the continuation without guessing IDs.
                successors ??= await TrlLineage.SuccessorsAsync(fileId, target.FileId,
                    id => ValueTask.FromResult(TrlLineage.LocalPrevious(getFile, id))).ConfigureAwait(false);
                fileId = successors[nextSuccessor++];
            }
            cancellation.ThrowIfCancellationRequested();
            _comparisonPosition = target;
            MatchedThrough = target;
            return target == end ? TrlCompareResult.Matched : TrlCompareResult.LocalBehind;
        }
        finally
        {
            if (localBuffer != null) ArrayPool<byte>.Shared.Return(localBuffer);
            if (remoteBuffer != null) ArrayPool<byte>.Shared.Return(remoteBuffer);
        }
    }

    TrlCompareResult Diverged()
    {
        _diverged = true;
        return TrlCompareResult.Diverged;
    }
}
