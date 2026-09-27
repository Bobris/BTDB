using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>
/// Native range endpoint for one authenticated database connection under selected leader authority.
/// The owner closes the endpoint before replacing the connection. Close may race with a read in progress: every
/// read rechecks closure and authority after copying bytes, so a closed endpoint never returns them.
/// Files are not pinned for peers: a missing retained range requires follower recovery, never a Blob fallback.
/// </summary>
internal sealed class LeaderTrlReader(BTreeKeyValueDB database, TransactionLogCapture capture,
    LeaseAuthority authority) : ILeaderTrlReader
{
    volatile bool _closed;

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

    /// <summary>Complete bytes from from through end (a complete transaction cut), at most budget bytes, one chunk per
    /// native file in lineage order. A later file's chunk follows only when the previous file was served to its end. Every read rechecks closure and authority. A range this leader cannot serve (not
    /// retained, or lineage it cannot follow) ends the chunks early; the follower then reads the rest by range.</summary>
    public async ValueTask<IReadOnlyList<ReplicationPeerTrlChunk>> ReadInlineAsync(TransactionLogPosition from,
        TransactionLogPosition end, int budget, CancellationToken cancellation)
    {
        var chunks = new List<ReplicationPeerTrlChunk>();
        budget = Math.Min(budget, ReplicationPeerPoll.MaximumInlineBytes);
        if (budget <= 0 || from.FileId == 0 || from >= end) return chunks;
        uint[] files;
        try
        {
            files = from.FileId == end.FileId ? [from.FileId]
                : [from.FileId, .. await TrlLineage.SuccessorsAsync(from.FileId, end.FileId,
                    id => ValueTask.FromResult(PreviousOf(id))).ConfigureAwait(false)];
        }
        catch (Exception error) when (error is FileNotFoundException or InvalidDataException) { return chunks; }
        foreach (var fileId in files)
        {
            var offset = fileId == from.FileId ? from.Offset : 0u;
            // Sealed files are complete to their size; the end file only to the advertised cut. A missing file ends
            // the chunks: skipping it would leave a gap before the next file's bytes.
            ulong limit = end.Offset;
            if (fileId != end.FileId)
            {
                if (database.FileCollection.GetFile(fileId) is not { } source) return chunks;
                limit = source.GetSize();
            }
            if (limit <= offset) continue;
            // Read straight into the chunk's own array: it is sent (or handed in-process) without another copy.
            var bytes = new byte[(int)Math.Min((ulong)budget, limit - offset)];
            var filled = 0;
            while (filled < bytes.Length)
            {
                int read;
                try { read = await ReadAsync(fileId, offset + (ulong)filled, bytes.AsMemory(filled), cancellation).ConfigureAwait(false); }
                // Only lost authority fails the poll; a range this leader cannot serve just ends the inline bytes.
                catch (IOException) when (!_closed && authority.IsValid) { return chunks; }
                if (read == 0) break;
                filled += read;
            }
            if (filled != 0) chunks.Add(new(fileId, offset, bytes.AsMemory(0, filled)));
            if (filled < bytes.Length) break;
            budget -= filled;
            if (budget == 0) break;
        }
        return chunks;
    }

    uint PreviousOf(uint fileId) => database.FileCollection.FileInfoByIdx(fileId) is IFileTransactionLog log
        ? log.PreviousFileId : throw new FileNotFoundException("The leader does not retain the requested TRL.");

    void RequireAuthority()
    {
        if (_closed || !authority.IsValid) throw new IOException("The leader TRL session is no longer authoritative.");
    }
}
