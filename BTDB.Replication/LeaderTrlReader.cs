using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>
/// Native range endpoint for one authenticated database connection under selected leader authority.
/// The owner serializes authority changes with requests and closes the endpoint before replacing the connection.
/// Files are not pinned for peers: a missing retained range requires follower recovery, never a Blob fallback.
/// </summary>
internal sealed class LeaderTrlReader(BTreeKeyValueDB database, TransactionLogCapture capture,
    LeaseAuthority authority) : ILeaderTrlReader
{
    bool _closed;

    public void Close() => _closed = true;

    public ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination,
        CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        RequireAuthority();
        // Local writers may already have appended an unfinished transaction. Only complete captured bytes
        // are available to peers, even when the physical file is longer.
        var end = capture.Completed;
        if (fileId == 0 || fileId > end.FileId)
            throw new IOException("The requested TRL is beyond the complete leader prefix.");
        if (database.FileCollection.FileInfoByIdx(fileId) is not IFileTransactionLog)
            throw new FileNotFoundException("The leader does not retain the requested TRL.");
        var source = database.FileCollection.GetFile(fileId)
            ?? throw new FileNotFoundException("The leader does not retain the requested TRL.");
        var limit = fileId == end.FileId ? end.Offset : source.GetSize();
        if (offset > limit) throw new IOException("The requested range is beyond the complete leader prefix.");
        var count = (int)Math.Min((ulong)destination.Length, limit - offset);
        source.RandomRead(destination.Span[..count], offset, false);
        cancellation.ThrowIfCancellationRequested();
        RequireAuthority();
        return ValueTask.FromResult(count);
    }

    void RequireAuthority()
    {
        if (_closed || !authority.IsValid) throw new IOException("The leader TRL session is no longer authoritative.");
    }
}
