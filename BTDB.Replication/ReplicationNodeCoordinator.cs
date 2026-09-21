using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

internal enum ReplicationNodeRole { Restoring, Follower, Activating, Leader, RestartRequired, Stopped }

internal sealed record ReplicationNodeOptions(string ClusterId, string Endpoint, TimeSpan PollInterval,
    TimeSpan LeaseRetryInterval, TimeSpan RequestTimeout, TimeSpan ConfirmationDuration, ulong ApplicationGeneration, TimeSpan? CompactionInterval = null);

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
    // The database remains owned by the application; only cluster coordination stops.
    void DatabaseRemoved(string database);
    // Construct a leader-session adapter with this authority; reuse the database's existing file set and export path.
    ReplicationMaintenance? CreateMaintenance(ActivationDatabase database, CanonicalTrlPublisher publisher,
        LeaseAuthority authority) => null;
    void SchemaDetached(string database) { }
    // Return only after compatibility, retained input, continued database lag and additions are prepared.
    PreparedHandoff? PreparedUpgrade => null;
    // The returned cursor precedes the first input this new database must consume.
    ValueTask<ulong> CaptureInitializationCursorAsync(string database, CancellationToken cancellation) =>
        throw new NotSupportedException("The host must supply the current input end for new databases.");
    // Reuse ObjectDB InitializeRelations here. Implementations revalidate authority before their writer and commit.
    ValueTask PrepareSchemaAsync(ActivationDatabase database, LeaseAuthority authority, CancellationToken cancellation) => ValueTask.CompletedTask;

}

/// <summary>Owns one node's restore/follow/activate/publish transitions. The host owns databases, ordered input
/// and restart execution. One transition lane consumes capture; lease maintenance is independent. Shutdown and
/// divergence stop replication without disposing databases or cancelling ordinary local application work.</summary>
internal sealed class ReplicationNodeCoordinator(ReplicationNodeOptions options, IReplicationNodeHost host,
    ILeaderRecordStorage records, LeaseSessionController leases, IReplicationPeerTransport transport,
    IReplicationScheduler scheduler)
{
    readonly Dictionary<string, FollowerComparisonSession> _followers = new(StringComparer.Ordinal);
    readonly HashSet<string> _removed = new(StringComparer.Ordinal);
    readonly HashSet<string> _detached = new(StringComparer.Ordinal);
    readonly Dictionary<string, SchemaTrlScanner> _scanners = new(StringComparer.Ordinal);
    TimeSpan _leaderEvidenceUntil;
    long _monitorChallenge;
    readonly object _remoteLock = new();
    CancellationTokenSource? _remoteWork;
    readonly List<ReplicationMaintenance> _remoteMaintenance = new();
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
        Task? localMaintenance = null;
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
            localMaintenance = RunLocalMaintenanceAsync(databases, lifetime.Token);
            listener = transport.Listen(options.Endpoint, Accept);
            // Observe the durable generation floor before the first lease request.
            while (true)
            {
                using var request = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
                using var timeout = scheduler.Schedule(options.RequestTimeout, () => request.Cancel(), "leader discovery timeout");
                try { await DiscoverAsync(databases, request.Token).ConfigureAwait(false); break; }
                catch (IOException) { }
                catch (OperationCanceledException) when (!cancellation.IsCancellationRequested) { }
                await WaitAsync(cancellation).ConfigureAwait(false);
            }
            maintenance = leases.RunAsync(options.LeaseRetryInterval, current =>
            {
                lock (_remoteLock)
                    if (_sessionAuthority != null && !ReferenceEquals(_sessionAuthority, current)) _remoteWork?.Cancel();
            }, lifetime.Token, options.RequestTimeout);
            Role = ReplicationNodeRole.Follower;
            while (!_restart)
            {
                cancellation.ThrowIfCancellationRequested();
                if (maintenance.IsCompleted) await maintenance.ConfigureAwait(false);
                if (localMaintenance.IsCompleted) await localMaintenance.ConfigureAwait(false);
                using var request = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
                using var timeout = scheduler.Schedule(options.RequestTimeout, () => request.Cancel(), "replication request timeout");
                try { await StepAsync(databases, request.Token).ConfigureAwait(false); }
                catch (InvalidDataException) { Restart("Local history or database configuration requires canonical restore."); }
                catch (FileNotFoundException) { Restart("Required TRL bytes are no longer retained; canonical restore is required."); }
                catch (IOException) { Disconnect(); }
                catch (OperationCanceledException) when (!cancellation.IsCancellationRequested) { Disconnect(); }
                catch (InvalidOperationException) when (_sessionAuthority is { IsValid: false }) { DropLeadership(); }
                if (_detached.Count != 0 && scheduler.Elapsed - _leaderEvidenceUntil >= TimeSpan.FromMinutes(15))
                    Restart("Detached node has had no valid leader for fifteen minutes.");
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
            if (localMaintenance != null)
            {
                try { await localMaintenance.ConfigureAwait(false); }
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
                lock (_remoteLock)
                {
                    _sessionAuthority = authority;
                    _remoteWork = new();
                }
                _candidate = host.CreateCandidate();
                if (_candidate.ClusterId != options.ClusterId || _candidate.PeerEndpoint != options.Endpoint ||
                    _candidate.ApplicationGeneration != options.ApplicationGeneration)
                    throw new InvalidDataException("Candidate identity differs from the configured node.");
                _leadership = new(new LeaderSelection(records, leases, authority, _candidate), databases, PrepareDatabaseAsync);
            }
            using var remoteRequest = CancellationTokenSource.CreateLinkedTokenSource(cancellation, _remoteWork!.Token);
            cancellation = remoteRequest.Token;
            Role = _serving == null ? ReplicationNodeRole.Activating : ReplicationNodeRole.Leader;
            if (_serving?.Handoff is { } handoff)
            {
                if (_serving.IsDrained)
                {
                    try { await leases.TransferAsync(handoff.TransferId, cancellation).ConfigureAwait(false); }
                    finally { DropLeadership(); }
                }
                return; // No publication after the drain starts; application work remains independent.
            }
            var publishers = await _leadership!.ActivateAsync(cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (publishers == null) return;
            if (!authority.IsValid) { DropLeadership(); return; }
            if (_serving == null)
            {
                var identity = new ReplicationPeerIdentity(options.ClusterId, _leadership.Selected!.Term,
                    _candidate!.SessionId, options.Endpoint, _candidate.ApiKey);
                try
                {
                    for (var i = 0; i < databases.Count; i++)
                        if (host.CreateMaintenance(databases[i], publishers[i], authority) is { } job) _remoteMaintenance.Add(job);
                }
                catch
                {
                    foreach (var job in _remoteMaintenance) job.Dispose();
                    _remoteMaintenance.Clear();
                    throw;
                }
                Volatile.Write(ref _serving, new(identity, authority, databases, host, scheduler, options.ApplicationGeneration));
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
            foreach (var job in _remoteMaintenance)
            {
                if (!authority.IsValid || _serving?.Handoff != null) break;
                await job.RunDueAsync(cancellation).ConfigureAwait(false);
            }
            Role = authority.IsValid ? ReplicationNodeRole.Leader : ReplicationNodeRole.Follower;
            return;
        }

        DropLeadership();
        Role = ReplicationNodeRole.Follower;
        if (_peer == null)
        {
            var json = await DiscoverAsync(databases, cancellation).ConfigureAwait(false);
            if ((json["term"]?.GetValue<ulong>() ?? 0) == 0) return;
            var identity = new ReplicationPeerIdentity(options.ClusterId, json["term"]!.GetValue<ulong>(),
                json["sessionId"]!.GetValue<string>(), json["peerEndpoint"]!.GetValue<string>(), json["apiKey"]!.GetValue<string>());
            _peer = await transport.ConnectAsync(identity, cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            foreach (var database in databases)
            {
                if (_removed.Contains(database.Name) || _detached.Contains(database.Name) || database.RestoredBase.FileId == 0 || !json["databaseNames"]!.AsArray().Any(n => n!.GetValue<string>() == database.Name))
                    continue;
                _scanners.Add(database.Name, new(database.RestoredBase, database.Database.FileCollection.Guid));
                _followers.Add(database.Name, new(database.Database.FileCollection.GetFile, database.Capture, _peer.Reader(database.Name),
                    scheduler, () => Restart("Follower native history diverged from its selected leader."),
                    database.RestoredBase));
            }
        }
        foreach (var (name, follower) in _followers.ToArray())
        {
            var dispatched = scheduler.Elapsed;
            var challenge = follower.BeginChallenge(options.ConfirmationDuration);
            var status = await _peer.PollAsync(name, challenge, options.ConfirmationDuration, cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (status.Challenge != challenge) throw new IOException("Stale peer challenge response.");
            if (status.Granted && follower.AcceptChallenge(challenge))
                _leaderEvidenceUntil = dispatched + options.ConfirmationDuration;
            if (status.Progress is { } progress)
            {
                if (await _scanners[name].ContainsSchemaAsync(_peer.Reader(name), progress.Position, cancellation).ConfigureAwait(false))
                {
                    cancellation.ThrowIfCancellationRequested();
                    leases.Disqualify();
                    if (_detached.Count == 0 && _leaderEvidenceUntil < scheduler.Elapsed)
                        _leaderEvidenceUntil = scheduler.Elapsed;
                    _detached.Add(name);
                    follower.Close();
                    _followers.Remove(name);
                    host.SchemaDetached(name);
                    continue;
                }
                follower.NotifyProgress(progress);
            }
            await follower.CompareLatestAsync(cancellation).ConfigureAwait(false);
            if (_restart) return;
        }
        foreach (var database in databases)
        {
            if (database.RestoredBase.FileId != 0) continue;
            if (_removed.Contains(database.Name)) continue;
            var dispatched = scheduler.Elapsed;
            var challenge = checked(++_monitorChallenge);
            var status = await _peer.PollAsync(database.Name, challenge, options.ConfirmationDuration, cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (status.Challenge == challenge && status.Granted && scheduler.Elapsed < dispatched + options.ConfirmationDuration)
                _leaderEvidenceUntil = dispatched + options.ConfirmationDuration;
            if (database.RestoredBase.FileId == 0 && status.Progress != null)
            { Restart("New database initialization is published; restore its fixed input cursor."); return; }
        }
        if (_detached.Count != 0 && _leaderEvidenceUntil <= scheduler.Elapsed)
        {
            var dispatched = scheduler.Elapsed;
            var challenge = checked(++_monitorChallenge);
            var status = await _peer.PollAsync(null, challenge, options.ConfirmationDuration, cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (status.Challenge == challenge && status.Granted && scheduler.Elapsed < dispatched + options.ConfirmationDuration)
                _leaderEvidenceUntil = dispatched + options.ConfirmationDuration;
        }
        if (_detached.Count == 0 && host.PreparedUpgrade is { } offer && offer.ApplicationGeneration == options.ApplicationGeneration)
        {
            leases.ProposeTransfer(offer.TransferId);
            await _peer.OfferHandoffAsync(offer, cancellation).ConfigureAwait(false);
        }
    }

    async ValueTask PrepareDatabaseAsync(ActivationDatabase database, LeaseAuthority authority, CancellationToken cancellation)
    {
        if (database.RestoredBase.FileId == 0 && database.Capture.Completed.FileId == 0)
        {
            var cursor = await host.CaptureInitializationCursorAsync(database.Name, cancellation).ConfigureAwait(false);
            if (!authority.IsValid) throw new InvalidOperationException("Initialization requires selected authority.");
            using var transaction = await database.Database.StartWritingTransaction(false, cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (!authority.IsValid) throw new InvalidOperationException("Initialization authority expired before commit.");
            transaction.SetCommitUlong(cursor);
            // A zero predecessor cursor is otherwise an empty no-op writer. Use the native complete-cut operation.
            transaction.NextCommitTemporaryCloseTransactionLog();
            transaction.Commit();
        }
        await host.PrepareSchemaAsync(database, authority, cancellation).ConfigureAwait(false);
    }

    async ValueTask<JsonObject> DiscoverAsync(IReadOnlyList<ActivationDatabase> databases, CancellationToken cancellation)
    {
        var record = await records.ReadAsync(cancellation).ConfigureAwait(false);
        cancellation.ThrowIfCancellationRequested();
        var json = JsonNode.Parse(record.Json)?.AsObject() ?? throw new InvalidDataException("Missing leader record.");
        if (json["clusterId"]?.GetValue<string>() != options.ClusterId || json["format"]?.GetValue<int>() != 1)
            throw new InvalidDataException("Leader discovery returned another cluster or format.");
        var generation = json["applicationGeneration"]?.GetValue<ulong>() ?? 0;
        if (generation > options.ApplicationGeneration) leases.Disqualify();
        if (generation >= options.ApplicationGeneration && json["databaseNames"] is JsonArray names)
        {
            var selected = names.Select(n => n!.GetValue<string>()).ToHashSet(StringComparer.Ordinal);
            if (generation == options.ApplicationGeneration && !selected.SetEquals(databases.Select(d => d.Name)))
                throw new InvalidDataException("The same application generation must select the same database set.");
            foreach (var database in databases)
                if (!selected.Contains(database.Name) && _removed.Add(database.Name)) host.DatabaseRemoved(database.Name);
        }
        return json;
    }

    async Task RunLocalMaintenanceAsync(IReadOnlyList<ActivationDatabase> databases, CancellationToken cancellation)
    {
        var interval = options.CompactionInterval ?? TimeSpan.FromMinutes(5);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(interval.Ticks);
        while (true)
        {
            var ready = new TaskCompletionSource();
            using (var timer = scheduler.Schedule(interval, () => ready.TrySetResult(), "local replication compaction"))
            using (var registration = cancellation.Register(() => ready.TrySetCanceled(cancellation)))
                await ready.Task.ConfigureAwait(false);
            foreach (var database in databases)
            {
                cancellation.ThrowIfCancellationRequested();
                await database.Database.Compact(cancellation).ConfigureAwait(false);
            }
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
        _scanners.Clear();
        _peer?.Dispose();
        _peer = null;
    }

    void DropLeadership()
    {
        var serving = Interlocked.Exchange(ref _serving, null);
        serving?.Close();
        foreach (var job in _remoteMaintenance) job.Dispose();
        _remoteMaintenance.Clear();
        _leadership?.Dispose();
        lock (_remoteLock)
        {
            _remoteWork?.Cancel();
            _remoteWork?.Dispose();
            _remoteWork = null;
            _sessionAuthority = null;
        }
        _leadership = null;
        _candidate = null;
    }

    sealed class ServingLeader(ReplicationPeerIdentity identity, LeaseAuthority authority,
        IReadOnlyList<ActivationDatabase> databases,
        IReplicationNodeHost host, IReplicationScheduler scheduler, ulong generation)
    {
        readonly object _lock = new();
        readonly IReplicationNodeHost _host = host;
        readonly ulong _generation = generation;
        readonly ConfirmationGrants _grants = new(scheduler, authority);
        readonly Dictionary<string, LeaderTrlReader> _readers = databases.ToDictionary(d => d.Name,
            d => new LeaderTrlReader(d.Database, d.Capture, authority), StringComparer.Ordinal);
        bool _closed;
        PreparedHandoff? _handoff;
        public PreparedHandoff? Handoff { get { lock (_lock) return _handoff; } }
        public bool IsDrained { get { lock (_lock) return _grants.IsDrained; } }

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
            public ValueTask<ReplicationPeerProgress> PollAsync(string? database, long challenge, TimeSpan duration, CancellationToken cancellation)
            {
                cancellation.ThrowIfCancellationRequested();
                lock (leader._lock)
                {
                    Check();
                    if (database != null && !leader._readers.ContainsKey(database)) throw new IOException("Unknown peer database.");
                    return ValueTask.FromResult(new ReplicationPeerProgress(challenge, leader._grants.TryIssue(duration),
                        database == null ? null : leader._host.GetProgress(database)));
                }
            }
            public ValueTask OfferHandoffAsync(PreparedHandoff offer, CancellationToken cancellation)
            {
                cancellation.ThrowIfCancellationRequested();
                lock (leader._lock)
                {
                    Check();
                    if (!Guid.TryParse(offer.TransferId, out _)) throw new ArgumentException("Transfer ID must be a UUID.");
                    if (offer.ApplicationGeneration > leader._generation &&
                        (leader._handoff == null || offer.ApplicationGeneration > leader._handoff.ApplicationGeneration))
                    {
                        leader._handoff = offer;
                        leader._grants.BeginDrain();
                    }
                }
                return ValueTask.CompletedTask;
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
