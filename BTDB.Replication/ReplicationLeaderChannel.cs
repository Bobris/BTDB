using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>
/// Application requests to the active replication leader over the existing authenticated peer session. Every node
/// registers its handlers with <see cref="Serve"/>; a handler runs only on the node whose leader session is active, the
/// same session that runs <see cref="ReplicationLeadership.RunWhileLeaderAsync"/> callbacks, and its token is cancelled
/// when that session ends. <see cref="SendAsync"/> calls the local handler on that node and otherwise the leader this
/// follower is connected to. Requests are not retried, persisted or ordered; payloads are opaque bytes.
/// </summary>
public sealed class ReplicationLeaderChannel
{
    /// <summary>Largest request or response payload, in bytes.</summary>
    public const int MaximumMessageBytes = 1024 * 1024;
    /// <summary>Longest channel name, in characters.</summary>
    public const int MaximumNameLength = 256;

    readonly object _lock = new();
    readonly Dictionary<string, Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>> _handlers =
        new(StringComparer.Ordinal);
    IReplicationPeerSession? _peer;

    public ReplicationLeaderChannel(ReplicationLeadership leadership)
    {
        Leadership = leadership ?? throw new ArgumentNullException(nameof(leadership));
    }

    internal ReplicationLeadership Leadership { get; }

    /// <summary>Handle requests for <paramref name="channel"/> while this node is the active leader. The token is cancelled
    /// when the caller disconnects or the leader session ends; a result produced after the session ended is discarded.
    /// Disposing the registration stops new requests without cancelling running ones.</summary>
    public IDisposable Serve(string channel,
        Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> handler)
    {
        ValidateName(channel);
        ArgumentNullException.ThrowIfNull(handler);
        lock (_lock)
            if (!_handlers.TryAdd(channel, handler))
                throw new InvalidOperationException($"Channel '{channel}' is already served.");
        return new Registration(this, channel, handler);
    }

    /// <summary>Send one request to the active leader's handler and return its response. Throws IOException when no
    /// active leader is reachable, the leader has no such handler, its session ended, or the handler failed; the request
    /// may have taken effect in every case, so a retry must be safe. Caller cancellation throws
    /// OperationCanceledException.</summary>
    public async ValueTask<ReadOnlyMemory<byte>> SendAsync(string channel, ReadOnlyMemory<byte> request,
        CancellationToken cancellation = default)
    {
        ValidateName(channel);
        if (request.Length > MaximumMessageBytes) throw new ArgumentOutOfRangeException(nameof(request));
        cancellation.ThrowIfCancellationRequested();
        if (Leadership.ActiveToken != null)
        {
            // Never run the handler in the caller's ambient context, such as an open query scope.
            if (ExecutionContext.IsFlowSuppressed())
                return await Task.Run(() => ServeLocalAsync(channel, request, cancellation)).ConfigureAwait(false);
            Task<ReadOnlyMemory<byte>> local;
            using (ExecutionContext.SuppressFlow())
                local = Task.Run(() => ServeLocalAsync(channel, request, cancellation));
            return await local.ConfigureAwait(false);
        }
        IReplicationPeerSession peer;
        lock (_lock) peer = _peer ?? throw new IOException("No replication leader is connected.");
        try
        {
            return await peer.SendApplicationAsync(channel, request, cancellation).ConfigureAwait(false);
        }
        catch (OperationCanceledException error) when (!cancellation.IsCancellationRequested)
        {
            throw new IOException("The replication leader connection closed.", error);
        }
    }

    async Task<ReadOnlyMemory<byte>> ServeLocalAsync(string channel, ReadOnlyMemory<byte> request,
        CancellationToken cancellation)
    {
        try
        {
            return await ServeAsync(channel, request, cancellation).ConfigureAwait(false);
        }
        catch (Exception error) when (error is not (IOException or OperationCanceledException))
        {
            // A remote handler failure reaches the caller as an HTTP error; keep one failure type for both paths.
            throw new IOException($"The leader handler of channel '{channel}' failed.", error);
        }
    }

    /// <summary>Leader side of a request, local or from a peer: rejects it unless this node's session is active.</summary>
    internal async ValueTask<ReadOnlyMemory<byte>> ServeAsync(string channel, ReadOnlyMemory<byte> request,
        CancellationToken cancellation)
    {
        var session = Leadership.ActiveToken ?? throw new IOException("Node is not an active replication leader.");
        Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? handler;
        lock (_lock) _handlers.TryGetValue(channel, out handler);
        if (handler == null) throw new IOException($"The replication leader does not serve channel '{channel}'.");
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, session);
        ReadOnlyMemory<byte> response;
        try
        {
            response = await handler(request, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException error) when (session.IsCancellationRequested &&
                                                       !cancellation.IsCancellationRequested)
        {
            throw new IOException("The replication leader session ended.", error);
        }
        cancellation.ThrowIfCancellationRequested();
        if (session.IsCancellationRequested) throw new IOException("The replication leader session ended.");
        if (response.Length > MaximumMessageBytes)
            throw new InvalidOperationException($"The response of channel '{channel}' exceeds its size limit.");
        return response;
    }

    internal void Connected(IReplicationPeerSession peer)
    {
        lock (_lock) _peer = peer;
    }

    internal void Disconnected(IReplicationPeerSession peer)
    {
        lock (_lock)
            if (ReferenceEquals(_peer, peer)) _peer = null;
    }

    static void ValidateName(string channel)
    {
        ArgumentException.ThrowIfNullOrEmpty(channel);
        if (channel.Length > MaximumNameLength) throw new ArgumentOutOfRangeException(nameof(channel));
    }

    sealed class Registration(ReplicationLeaderChannel owner, string channel,
        Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> handler) : IDisposable
    {
        public void Dispose()
        {
            lock (owner._lock)
                if (owner._handlers.TryGetValue(channel, out var current) && current == handler)
                    owner._handlers.Remove(channel);
        }
    }
}
