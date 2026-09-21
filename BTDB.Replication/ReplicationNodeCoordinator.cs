using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

internal enum ReplicationNodeRole { Restoring, Follower, Activating, Leader, RestartRequired, Stopped }

internal sealed record ReplicationNodeOptions(string ClusterId, string Endpoint, TimeSpan PollInterval,
    TimeSpan LeaseRetryInterval, TimeSpan RequestTimeout, TimeSpan ConfirmationDuration);

internal interface IReplicationNodeHost
{
    // Reuse ordinary initialization/open; dispose a failed attempt before allowing a restore retry.
    ValueTask<IReadOnlyList<ActivationDatabase>> RestoreAsync(CancellationToken cancellation);
    // Fresh identity/key per lease acquisition, with database names matching the restored set.
    LeaderCandidate CreateCandidate();
    // Atomic snapshot supplied after complete local work, before starting the next application transaction.
    LeaderTrlProgress? GetProgress(string database);
    void RequestRestart(string reason);
    void ReportStatus(ReplicationNodeRole role);
}

/// <summary>Owns one node's restore/follow/activate/publish transitions. The host owns databases, ordered input
/// and restart execution. One transition lane consumes capture; lease maintenance is independent. Shutdown and
/// divergence stop replication without disposing databases or cancelling ordinary local application work.</summary>
internal sealed class ReplicationNodeCoordinator(ReplicationNodeOptions options, IReplicationNodeHost host,
    ILeaderRecordStorage records, LeaseSessionController leases, IReplicationPeerTransport transport,
    IReplicationScheduler scheduler)
{
    readonly Dictionary<string, FollowerComparisonSession> _followers = new(StringComparer.Ordinal);
    IReplicationPeerSession? _peer;
    LeaseAuthority? _sessionAuthority;
    LeaderCandidate? _candidate;
    LeadershipSession? _leadership;
    ServingLeader? _serving;
    volatile bool _restart;

    public ReplicationNodeRole Role { get; private set; } = ReplicationNodeRole.Restoring;

    public async Task RunAsync(CancellationToken cancellation)
    {
        foreach (var duration in new[] { options.PollInterval, options.LeaseRetryInterval, options.RequestTimeout, options.ConfirmationDuration })
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(duration.Ticks);
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        Task? maintenance = null;
        IDisposable? listener = null;
        try
        {
            IReadOnlyList<ActivationDatabase> databases;
            while (true)
            {
                cancellation.ThrowIfCancellationRequested();
                try { databases = await host.RestoreAsync(cancellation).ConfigureAwait(false); break; }
                catch (IOException) { await WaitAsync(cancellation).ConfigureAwait(false); }
            }
            listener = transport.Listen(options.Endpoint, Accept);
            maintenance = leases.RunAsync(options.LeaseRetryInterval, _ => { }, lifetime.Token, options.RequestTimeout);
            Role = ReplicationNodeRole.Follower;
            while (!_restart)
            {
                cancellation.ThrowIfCancellationRequested();
                if (maintenance.IsCompleted) await maintenance.ConfigureAwait(false);
                using var request = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
                using var timeout = scheduler.Schedule(options.RequestTimeout, () => request.Cancel(), "replication request timeout");
                try { await StepAsync(databases, request.Token).ConfigureAwait(false); }
                catch (InvalidDataException) { Restart("Local history or database configuration requires canonical restore."); }
                catch (FileNotFoundException) { Restart("Required TRL bytes are no longer retained; canonical restore is required."); }
                catch (IOException) { Disconnect(); }
                catch (OperationCanceledException) when (!cancellation.IsCancellationRequested) { Disconnect(); }
                catch (InvalidOperationException) when (_sessionAuthority is { IsValid: false }) { DropLeadership(); }
                if (!_restart) await WaitAsync(cancellation).ConfigureAwait(false);
            }
        }
        finally
        {
            lifetime.Cancel();
            leases.Close();
            Disconnect();
            DropLeadership();
            listener?.Dispose();
            if (maintenance != null)
            {
                try { await maintenance.ConfigureAwait(false); }
                catch (OperationCanceledException) when (lifetime.IsCancellationRequested) { }
            }
            Role = _restart ? ReplicationNodeRole.RestartRequired : ReplicationNodeRole.Stopped;
            host.ReportStatus(Role);
        }
    }

    async ValueTask StepAsync(IReadOnlyList<ActivationDatabase> databases, CancellationToken cancellation)
    {
        if (leases.Current is { } authority)
        {
            Disconnect();
            if (!ReferenceEquals(_sessionAuthority, authority))
            {
                DropLeadership();
                _sessionAuthority = authority;
                _candidate = host.CreateCandidate();
                if (_candidate.ClusterId != options.ClusterId || _candidate.PeerEndpoint != options.Endpoint)
                    throw new InvalidDataException("Candidate identity differs from the configured node.");
                _leadership = new(new LeaderSelection(records, leases, authority, _candidate), databases);
            }
            Role = _serving == null ? ReplicationNodeRole.Activating : ReplicationNodeRole.Leader;
            var publishers = await _leadership!.ActivateAsync(cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (publishers == null) return;
            if (!authority.IsValid) { DropLeadership(); return; }
            if (_serving == null)
            {
                var identity = new ReplicationPeerIdentity(options.ClusterId, _leadership.Selected!.Term,
                    _candidate!.SessionId, options.Endpoint, _candidate.ApiKey);
                Volatile.Write(ref _serving, new(identity, authority, databases, host, scheduler));
            }
            foreach (var publisher in publishers)
            {
                var result = await publisher.PublishNextAsync(true, cancellation).ConfigureAwait(false);
                if (result is TrlPublishResult.Conflict or TrlPublishResult.AuthorityLost)
                {
                    authority.Fence();
                    DropLeadership();
                    return;
                }
            }
            Role = authority.IsValid ? ReplicationNodeRole.Leader : ReplicationNodeRole.Follower;
            return;
        }

        DropLeadership();
        Role = ReplicationNodeRole.Follower;
        if (_peer == null)
        {
            var record = await records.ReadAsync(cancellation).ConfigureAwait(false);
            var json = JsonNode.Parse(record.Json)!;
            if (json["clusterId"]?.GetValue<string>() != options.ClusterId)
                throw new InvalidDataException("Leader discovery returned another cluster.");
            if ((json["term"]?.GetValue<ulong>() ?? 0) == 0) return;
            var identity = new ReplicationPeerIdentity(options.ClusterId, json["term"]!.GetValue<ulong>(),
                json["sessionId"]!.GetValue<string>(), json["peerEndpoint"]!.GetValue<string>(), json["apiKey"]!.GetValue<string>());
            _peer = await transport.ConnectAsync(identity, cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            foreach (var database in databases)
                _followers.Add(database.Name, new(database.Database.FileCollection.GetFile, database.Capture, _peer.Reader(database.Name),
                    scheduler, () => Restart("Follower native history diverged from its selected leader."),
                    database.RestoredBase));
        }
        foreach (var (name, follower) in _followers)
        {
            var challenge = follower.BeginChallenge(options.ConfirmationDuration);
            var status = await _peer.PollAsync(name, challenge, options.ConfirmationDuration, cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (status.Challenge != challenge) throw new IOException("Stale peer challenge response.");
            if (status.Granted) follower.AcceptChallenge(challenge);
            if (status.Progress is { } progress) follower.NotifyProgress(progress);
            await follower.CompareLatestAsync(cancellation).ConfigureAwait(false);
            if (_restart) return;
        }
    }

    async Task WaitAsync(CancellationToken cancellation)
    {
        var ready = new TaskCompletionSource();
        using var timer = scheduler.Schedule(options.PollInterval, () => ready.TrySetResult(), "replication node poll");
        using var registration = cancellation.Register(() => ready.TrySetCanceled(cancellation));
        host.ReportStatus(Role);
        await ready.Task.ConfigureAwait(false);
    }

    IReplicationPeerSession Accept(ReplicationPeerIdentity identity)
    {
        var serving = Volatile.Read(ref _serving) ?? throw new IOException("Node is not an active leader.");
        return serving.Connect(identity);
    }

    void Restart(string reason)
    {
        if (_restart) return;
        _restart = true;
        leases.Close();
        host.RequestRestart(reason);
    }

    void Disconnect()
    {
        foreach (var follower in _followers.Values) follower.Close();
        _followers.Clear();
        _peer?.Dispose();
        _peer = null;
    }

    void DropLeadership()
    {
        var serving = Interlocked.Exchange(ref _serving, null);
        serving?.Close();
        _leadership?.Dispose();
        _sessionAuthority = null;
        _leadership = null;
        _candidate = null;
    }

    sealed class ServingLeader(ReplicationPeerIdentity identity, LeaseAuthority authority,
        IReadOnlyList<ActivationDatabase> databases,
        IReplicationNodeHost host, IReplicationScheduler scheduler)
    {
        readonly object _lock = new();
        readonly IReplicationNodeHost _host = host;
        readonly ConfirmationGrants _grants = new(scheduler, authority);
        readonly Dictionary<string, LeaderTrlReader> _readers = databases.ToDictionary(d => d.Name,
            d => new LeaderTrlReader(d.Database, d.Capture, authority), StringComparer.Ordinal);
        bool _closed;

        public IReplicationPeerSession Connect(ReplicationPeerIdentity requested)
        {
            lock (_lock)
            {
                RequireActive();
                if (requested.ClusterId != identity.ClusterId || requested.Term != identity.Term ||
                    requested.SessionId != identity.SessionId || requested.Endpoint != identity.Endpoint ||
                    !CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(requested.ApiKey), Encoding.UTF8.GetBytes(identity.ApiKey)))
                    throw new IOException("Peer authentication or leader session is invalid.");
                return new Connection(this);
            }
        }

        void RequireActive()
        {
            if (_closed || !authority.IsValid) throw new IOException("Leader session is no longer active.");
        }

        public void Close()
        {
            lock (_lock)
            {
                _closed = true;
                foreach (var reader in _readers.Values) reader.Close();
            }
        }

        sealed class Connection(ServingLeader leader) : IReplicationPeerSession
        {
            bool _closed;
            ServingLeader Owner => leader;
            public void Dispose() { lock (leader._lock) _closed = true; }
            void Check() { if (_closed) throw new IOException("Peer connection is closed."); leader.RequireActive(); }
            public ValueTask<ReplicationPeerProgress> PollAsync(string database, long challenge, TimeSpan duration, CancellationToken cancellation)
            {
                cancellation.ThrowIfCancellationRequested();
                lock (leader._lock)
                {
                    Check();
                    if (!leader._readers.ContainsKey(database)) throw new IOException("Unknown peer database.");
                    return ValueTask.FromResult(new ReplicationPeerProgress(challenge, leader._grants.TryIssue(duration),
                        leader._host.GetProgress(database)));
                }
            }
            public ILeaderTrlReader Reader(string database)
            {
                lock (leader._lock)
                {
                    Check();
                    if (!leader._readers.ContainsKey(database)) throw new IOException("Unknown peer database.");
                    return new ReaderProxy(this, database);
                }
            }
            sealed class ReaderProxy(Connection connection, string database) : ILeaderTrlReader
            {
                public ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation)
                {
                    lock (connection.Owner._lock)
                    {
                        connection.Check();
                        return connection.Owner._readers[database].ReadAsync(fileId, offset, destination, cancellation);
                    }
                }
            }
        }
    }
}
