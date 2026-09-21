using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

internal sealed record ReplicationPeerIdentity(string ClusterId, ulong Term, string SessionId, string Endpoint, string ApiKey)
{
    public override string ToString() => $"{ClusterId}/{Term}/{SessionId}";
}

internal sealed record ReplicationPeerProgress(long Challenge, bool Granted, LeaderTrlProgress? Progress);
internal sealed record PreparedHandoff(ulong ApplicationGeneration, string TransferId);

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

/// <summary>An isolated registry, shared only by nodes in one in-process cluster. The same server authentication
/// and session endpoints are used as by other transport adapters; no global registry or Blob fallback exists.</summary>
internal sealed class InProcessReplicationPeerTransport : IReplicationPeerTransport
{
    readonly ConcurrentDictionary<string, Func<ReplicationPeerIdentity, IReplicationPeerSession>> _listeners = new();

    public IDisposable Listen(string endpoint, Func<ReplicationPeerIdentity, IReplicationPeerSession> accept)
    {
        if (!_listeners.TryAdd(endpoint, accept)) throw new InvalidOperationException("Peer endpoint already registered.");
        return new Registration(() => _listeners.TryRemove(new(endpoint, accept)));
    }

    public ValueTask<IReplicationPeerSession> ConnectAsync(ReplicationPeerIdentity identity, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        if (!_listeners.TryGetValue(identity.Endpoint, out var accept)) throw new IOException("Leader endpoint is unavailable.");
        return ValueTask.FromResult(accept(identity));
    }

    sealed class Registration(Action remove) : IDisposable
    {
        public void Dispose() => remove();
    }
}
