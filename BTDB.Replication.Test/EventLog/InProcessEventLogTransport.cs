using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;

namespace BTDB.Replication.Test.EventLog;

internal sealed class PeerUnavailableException(string endpoint) : Exception($"Peer {endpoint} is unreachable.");

/// <summary>Routes peer calls to in-process handlers. A partitioned endpoint is unreachable in both directions, and its
/// open subscriptions fail.</summary>
internal sealed class InProcessEventLogTransport
{
    readonly ConcurrentDictionary<string, IEventLogPeerHandler> _handlers = new();
    readonly ConcurrentDictionary<string, bool> _partitioned = new();

    public void Register(string endpoint, IEventLogPeerHandler handler) => _handlers[endpoint] = handler;
    public void Partition(string endpoint) => _partitioned[endpoint] = true;
    public void Heal(string endpoint) => _partitioned.TryRemove(endpoint, out _);

    public IEventLogPeerTransport From(string caller) => new Client(this, caller);

    IEventLogPeerHandler Route(string caller, string endpoint)
    {
        if (_partitioned.ContainsKey(caller) || _partitioned.ContainsKey(endpoint) ||
            !_handlers.TryGetValue(endpoint, out var handler)) throw new PeerUnavailableException(endpoint);
        return handler;
    }

    bool Reachable(string caller, string endpoint) =>
        !_partitioned.ContainsKey(caller) && !_partitioned.ContainsKey(endpoint);

    sealed class Client(InProcessEventLogTransport transport, string caller) : IEventLogPeerTransport
    {
        public async ValueTask<EventLogSubmitResponse> SubmitAsync(string endpoint, EventLogSubmitRequest request,
            CancellationToken cancellation)
        {
            var response = await transport.Route(caller, endpoint).SubmitAsync(request, cancellation);
            if (!transport.Reachable(caller, endpoint)) throw new PeerUnavailableException(endpoint); // lost response
            return response;
        }

        public ValueTask<EventLogBoundsResponse> GetBoundsAsync(string endpoint, string topic,
            CancellationToken cancellation) => transport.Route(caller, endpoint).GetBoundsAsync(topic, cancellation);

        public async IAsyncEnumerable<EventLogLiveMessage> SubscribeAsync(string endpoint, string topic, ulong from,
            [EnumeratorCancellation] CancellationToken cancellation)
        {
            var handler = transport.Route(caller, endpoint);
            await foreach (var message in handler.SubscribeAsync(topic, from, cancellation))
            {
                if (!transport.Reachable(caller, endpoint)) throw new PeerUnavailableException(endpoint);
                yield return message;
            }
        }
    }
}
