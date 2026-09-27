using System;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

internal sealed record ReplicationPeerIdentity(string ClusterId, ulong Term, string SessionId, string Endpoint, string ApiKey)
{
    public override string ToString() => $"{ClusterId}/{Term}/{SessionId}";
}

/// <summary>Published is the leader's confirmed canonical Blob cut for the database, null before its first publication.
/// A follower treats bytes it compared up to min(compared, Published) as canonical history.</summary>
internal sealed record ReplicationPeerProgress(long Challenge, bool Granted, LeaderTrlProgress? Progress,
    TransactionLogPosition? Published = null);
public sealed record PreparedHandoff(ulong ApplicationGeneration, string TransferId);

/// <summary>One authenticated leader connection. Implementations bind every request to that connection and
/// honor cancellation even when a remote effect or response may still arrive. Poll returns latest coalesced progress.</summary>
internal interface IReplicationPeerSession : IDisposable
{
    // A null database is an authority-only heartbeat, including after every local database was detached/removed.
    ValueTask<ReplicationPeerProgress> PollAsync(string? database, long challenge, TimeSpan duration, CancellationToken cancellation);
    ILeaderTrlReader Reader(string database);
    ValueTask OfferHandoffAsync(PreparedHandoff offer, CancellationToken cancellation);
}

internal interface IReplicationPeerTransport
{
    IDisposable Listen(string endpoint, Func<ReplicationPeerIdentity, IReplicationPeerSession> accept);
    ValueTask<IReplicationPeerSession> ConnectAsync(ReplicationPeerIdentity identity, CancellationToken cancellation);
}
