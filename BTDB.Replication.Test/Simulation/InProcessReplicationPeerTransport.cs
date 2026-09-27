using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.Test.Simulation;

/// <summary>An isolated registry, shared only by nodes in one in-process cluster. The same server authentication
/// and session endpoints are used as by other transport adapters; no global registry or Blob fallback exists.</summary>
internal sealed class InProcessReplicationPeerTransport : IReplicationPeerTransport
{
    readonly ConcurrentDictionary<string, Func<ReplicationPeerIdentity, IReplicationPeerSession>> _listeners = new();

    public IDisposable Listen(string endpoint, Func<string, bool> authenticate,
        Func<ReplicationPeerIdentity, IReplicationPeerSession> accept)
    {
        Func<ReplicationPeerIdentity, IReplicationPeerSession> authenticated = identity =>
        {
            if (!authenticate(identity.ApiKey)) throw new IOException("Peer authentication failed.");
            return accept(identity);
        };
        if (!_listeners.TryAdd(endpoint, authenticated)) throw new InvalidOperationException("Peer endpoint already registered.");
        return new Registration(() => _listeners.TryRemove(new(endpoint, authenticated)));
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
