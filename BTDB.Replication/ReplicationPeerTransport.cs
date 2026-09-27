using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

internal sealed record ReplicationPeerIdentity(string ClusterId, ulong Term, string SessionId, string Endpoint, string ApiKey)
{
    public override string ToString() => $"{ClusterId}/{Term}/{SessionId}";
}

/// <summary>One database of a poll. From is where the follower's schema scan or comparison resumes; the leader returns
/// its complete TRL bytes from there inline. A zero FileId asks for progress only.</summary>
internal readonly record struct ReplicationPeerPollRequest(string Database, TransactionLogPosition From = default);

/// <summary>Complete leader TRL bytes of one file returned inline with a poll.</summary>
internal sealed record ReplicationPeerTrlChunk(uint FileId, uint Offset, ReadOnlyMemory<byte> Bytes);

/// <summary>Published is the leader's confirmed canonical Blob cut for the database, null before its first publication.
/// A follower treats bytes it compared up to min(compared, Published) as canonical history. Chunks continue the
/// request's From position in native file order, never past Progress.</summary>
internal sealed record ReplicationPeerDatabaseProgress(string Database, LeaderTrlProgress? Progress,
    TransactionLogPosition? Published = null, IReadOnlyList<ReplicationPeerTrlChunk>? Chunks = null);

/// <summary>One poll: a single challenge/grant for the whole request and the progress of every requested database, in
/// request order. An empty request is an authority-only heartbeat.</summary>
internal sealed record ReplicationPeerPoll(long Challenge, bool Granted, IReadOnlyList<ReplicationPeerDatabaseProgress> Databases)
{
    /// <summary>Upper bound a leader honors for the inline bytes of one poll.</summary>
    internal const int MaximumInlineBytes = 4 * 1024 * 1024;

    /// <summary>Throws unless this answers exactly the requested challenge and databases with valid cuts, and inline
    /// bytes stay within the budget, continue each From position and never pass the advertised progress.</summary>
    public void Validate(long challenge, IReadOnlyList<ReplicationPeerPollRequest> requested, int inlineBudget)
    {
        if (Challenge != challenge || Databases is not { } answers || answers.Count != requested.Count)
            throw new IOException("Stale or incomplete peer poll response.");
        long inline = 0;
        for (var i = 0; i < answers.Count; i++)
        {
            if (answers[i] is not { } answer || answer.Database != requested[i].Database ||
                answer.Progress is { } progress && (progress.TrlFileId == 0 || progress.TrlPosition == 0) ||
                answer.Published is { FileId: 0 })
                throw new IOException("Invalid peer progress.");
            if (answer.Chunks is not { Count: > 0 } chunks) continue;
            if (answer.Progress is not { } end || requested[i].From.FileId == 0)
                throw new IOException("Unrequested inline TRL bytes.");
            var expected = requested[i].From;
            foreach (var chunk in chunks)
            {
                if (chunk is null || chunk.Bytes.IsEmpty ||
                    !(chunk.FileId == expected.FileId && chunk.Offset == expected.Offset ||
                      chunk.FileId > expected.FileId && chunk.Offset == 0))
                    throw new IOException("Inline TRL bytes do not continue the requested position.");
                var chunkEnd = (ulong)chunk.Offset + (uint)chunk.Bytes.Length;
                if (chunkEnd > uint.MaxValue || chunk.FileId > end.TrlFileId ||
                    chunk.FileId == end.TrlFileId && chunkEnd > end.TrlPosition)
                    throw new IOException("Inline TRL bytes pass the advertised progress.");
                inline += chunk.Bytes.Length;
                expected = new(chunk.FileId, (uint)chunkEnd);
            }
        }
        if (inline > inlineBudget) throw new IOException("Inline TRL bytes exceed the requested budget.");
    }
}
public sealed record PreparedHandoff(ulong ApplicationGeneration, string TransferId);

/// <summary>One authenticated leader connection. Implementations bind every request to that connection and
/// honor cancellation even when a remote effect or response may still arrive. Poll returns latest coalesced progress.</summary>
internal interface IReplicationPeerSession : IDisposable
{
    // One request per follower step. No databases is an authority-only heartbeat, e.g. after every local database
    // was detached or removed. InlineBudget caps the TRL bytes returned with the poll (0 disables them).
    ValueTask<ReplicationPeerPoll> PollAsync(IReadOnlyList<ReplicationPeerPollRequest> databases, long challenge,
        TimeSpan duration, int inlineBudget, CancellationToken cancellation);
    ILeaderTrlReader Reader(string database);
    ValueTask OfferHandoffAsync(PreparedHandoff offer, CancellationToken cancellation);
}

internal interface IReplicationPeerTransport
{
    IDisposable Listen(string endpoint, Func<ReplicationPeerIdentity, IReplicationPeerSession> accept);
    ValueTask<IReplicationPeerSession> ConnectAsync(ReplicationPeerIdentity identity, CancellationToken cancellation);
}
