using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;

namespace BTDB.Replication.Http;

/// <summary>The peer transport of a node that is alone: nobody connects to it and it reaches nobody. Connecting to a
/// leader named by an earlier session fails like an unreachable peer, so the node acquires the lease itself.</summary>
internal sealed class SingleNodeReplicationPeerTransport : IReplicationPeerTransport
{
    public IDisposable Listen(string endpoint, Func<string, bool> authenticate,
        Func<ReplicationPeerIdentity, IReplicationPeerSession> accept) => new NoListener();

    public ValueTask<IReplicationPeerSession> ConnectAsync(ReplicationPeerIdentity identity,
        CancellationToken cancellation) =>
        ValueTask.FromException<IReplicationPeerSession>(SingleNode.Unreachable());

    sealed class NoListener : IDisposable
    {
        public void Dispose() { }
    }
}

/// <summary>The event-log peer transport of a node that is alone. A topic whose tail names another endpoint is taken
/// over after <see cref="EventLogOptions.OwnerTimeout"/>; one owned by this endpoint's previous session at once.</summary>
internal sealed class SingleNodeEventLogPeerTransport : IEventLogPeerTransport
{
    public ValueTask<EventLogSubmitResponse> SubmitAsync(string endpoint, EventLogSubmitRequest request,
        CancellationToken cancellation) => ValueTask.FromException<EventLogSubmitResponse>(SingleNode.Unreachable());

    public ValueTask<EventLogBoundsResponse> GetBoundsAsync(string endpoint, string topic,
        CancellationToken cancellation) => ValueTask.FromException<EventLogBoundsResponse>(SingleNode.Unreachable());

    public async IAsyncEnumerable<EventLogLiveMessage> SubscribeAsync(string endpoint, string topic, ulong from,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        throw SingleNode.Unreachable();
#pragma warning disable CS0162 // An async iterator needs a yield.
        yield break;
#pragma warning restore CS0162
    }
}

static class SingleNode
{
    /// <summary>The identity of a single node; peers never see it.</summary>
    internal const string Endpoint = "single-node";

    internal static IOException Unreachable() =>
        new("A single-node registration has no peers; another node's endpoint is unreachable.");
}
