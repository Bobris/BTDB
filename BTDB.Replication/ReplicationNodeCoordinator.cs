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

public enum ReplicationNodeRole { Restoring, Follower, Activating, Leader, RestartRequired, Stopped }

/// <summary>RequestTimeout bounds each discovery and follower control/comparison step. Leader activation, publication
/// and checkpoint maintenance transfer bulk data, so they are bounded by lease authority and optional ProgressTimeouts.</summary>
public sealed record ReplicationNodeOptions(string ClusterId, string Endpoint, TimeSpan PollInterval,
    TimeSpan LeaseRetryInterval, TimeSpan RequestTimeout, TimeSpan ConfirmationDuration, ulong ApplicationGeneration, TimeSpan? CompactionInterval = null,
    ReplicationProgressTimeouts? ProgressTimeouts = null);

/// <summary>Application-owned restore, event progress and lifecycle integration. Databases and input processing
/// remain owned by the host. Callbacks may run concurrently with local application work; publish progress atomically.
/// Keep executing ordered inputs while Activating: a lagging lease winner waits for local execution to reach
/// published history before adopting it.</summary>
public interface IReplicationNodeHost
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
    // The publisher is borrowed and owned by the coordinator. Do not dispose it or run a separate publication loop.
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
    IReplicationScheduler scheduler, ReplicationStatus? status = null)
{
    IReadOnlyList<ActivationDatabase> _databases = Array.Empty<ActivationDatabase>();
    IReadOnlyList<CanonicalTrlPublisher>? _publishers;
    readonly Dictionary<string, FollowerComparisonSession> _followers = new(StringComparer.Ordinal);
    readonly HashSet<string> _removed = new(StringComparer.Ordinal);
    readonly HashSet<string> _detached = new(StringComparer.Ordinal);
    readonly Dictionary<string, SchemaTrlScanner> _scanners = new(StringComparer.Ordinal);
    // Latest local cut known to be canonical: restored, published by this node, or compared with a leader that had
    // already published it. Rechecks, activation validation and local TRL retention start here, never at the
    // startup cut, whose files local compaction may already have removed.
    readonly Dictionary<string, TransactionLogPosition> _canonicalBase = new(StringComparer.Ordinal);
    // Comparison progress with one leader session survives reconnects, timeouts included; a new leader rechecks from
    // the canonical base because a predecessor's unpublished bytes may differ from its history.
    (ulong Term, string SessionId)? _comparedLeader;
    readonly Dictionary<string, TransactionLogPosition> _resumeComparison = new(StringComparer.Ordinal);
    TimeSpan _leaderEvidenceUntil;
    long _monitorChallenge;
    readonly object _remoteLock = new();
    CancellationTokenSource? _remoteWork;
    readonly List<ReplicationMaintenance> _remoteMaintenance = new();
    Task? _maintenanceRun;
    Task _maintenanceDrain = Task.CompletedTask;
    ReplicationProgressWatchdog? _activationWatchdog;
    readonly List<(ReplicationProgressWatchdog? Watchdog, TransactionLogPosition Position)> _publicationWatchdogs = new();
    int _recoveryState; // 0 running, 1 fatal recovery won, 2 graceful shutdown/restart won.
    IReplicationPeerSession? _peer;
    LeaseAuthority? _sessionAuthority;
    LeaderCandidate? _candidate;
    LeadershipSession? _leadership;
    ServingLeader? _serving;
    readonly List<ReplicationMaintenanceWatchdog> _maintenanceWatchdogs = new();
    readonly TaskCompletionSource _fatalRestart = new(TaskCreationOptions.RunContinuationsAsynchronously);
    volatile bool _restart;

    public ReplicationNodeRole Role { get; private set; } = ReplicationNodeRole.Restoring;

    internal SelectedLeadership? ApplicationDataLeadership()
    {
        lock (_remoteLock)
        {
            var serving = Volatile.Read(ref _serving);
            if (_restart || serving == null || serving.Handoff != null ||
                _sessionAuthority is not { IsValid: true } authority ||
                !ReferenceEquals(leases.Current, authority)) return null;
            return _leadership?.Selected;
        }
    }

    public async Task RunAsync(CancellationToken cancellation)
    {
        foreach (var duration in new[] { options.PollInterval, options.LeaseRetryInterval, options.RequestTimeout, options.ConfirmationDuration })
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(duration.Ticks);
        options.ProgressTimeouts?.Validate();
        if (options.ProgressTimeouts != null && host is not IReplicationFatalRecovery)
            throw new ArgumentException("Progress deadlines require a host implementing IReplicationFatalRecovery.");
        using var shutdown = cancellation.Register(StopWatchdogs);
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
            _databases = databases;
            foreach (var database in databases)
                if (database.RestoredBase.FileId != 0) _canonicalBase[database.Name] = database.RestoredBase;
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
                try { await StepAsync(databases, request.Token, cancellation).ConfigureAwait(false); }
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
            StopWatchdogs();
            if (Volatile.Read(ref _recoveryState) == 1) await _fatalRestart.Task.ConfigureAwait(false);
            status?.Stop();
            lifetime.Cancel();
            leases.Close();
            Disconnect();
            DropLeadership();
            await _maintenanceDrain.ConfigureAwait(false);
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
            ReportStatus();
        }
    }

    // requestCancellation carries the per-step RequestTimeout; nodeCancellation only stops the node.
    async ValueTask StepAsync(IReadOnlyList<ActivationDatabase> databases, CancellationToken requestCancellation,
        CancellationToken nodeCancellation)
    {
        var cancellation = requestCancellation;
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
                    if (options.ProgressTimeouts is { } deadlines && Volatile.Read(ref _recoveryState) == 0)
                    {
                        _activationWatchdog = new(scheduler, deadlines.Activation,
                            () => FatalRecovery(authority, "Replication activation made no forward progress."), "replication activation watchdog");
                        _activationWatchdog.Progress();
                    }
                }
                _candidate = host.CreateCandidate();
                if (_candidate.ClusterId != options.ClusterId || _candidate.PeerEndpoint != options.Endpoint ||
                    _candidate.ApplicationGeneration != options.ApplicationGeneration)
                    throw new InvalidDataException("Candidate identity differs from the configured node.");
                lock (_remoteLock)
                {
                    // Validation starts at each canonical base: the local bytes before it are already canonical.
                    var candidates = databases.Select(d => _canonicalBase.TryGetValue(d.Name, out var canonical)
                        ? d with { RestoredBase = canonical } : d).ToArray();
                    _leadership = new(new LeaderSelection(records, leases, authority, _candidate), candidates,
                        PrepareDatabaseAsync, _activationWatchdog == null ? null : _activationWatchdog.Progress);
                }
            }
            // Validation, publication backlogs and uploads may legitimately outlast one request timeout. A timeout
            // would discard their progress and retry from scratch forever; lease loss still cancels _remoteWork.
            using var remoteRequest = CancellationTokenSource.CreateLinkedTokenSource(nodeCancellation, _remoteWork!.Token);
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
            if (Role == ReplicationNodeRole.Activating) ReportStatus();
            var publishers = await _leadership!.ActivateAsync(cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (publishers == null) return;
            _publishers = publishers;
            if (!authority.IsValid) { DropLeadership(); return; }
            if (_serving == null)
            {
                var identity = new ReplicationPeerIdentity(options.ClusterId, _leadership.Selected!.Term,
                    _candidate!.SessionId, options.Endpoint, _candidate.ApiKey);
                try
                {
                    for (var i = 0; i < databases.Count; i++)
                        if (host.CreateMaintenance(databases[i], publishers[i], authority) is { } job)
                        {
                            _remoteMaintenance.Add(job);
                            if (options.ProgressTimeouts?.Maintenance is { } budget)
                                lock (_remoteLock)
                                {
                                    if (Volatile.Read(ref _recoveryState) != 0) continue;
                                    var watchdog = new ReplicationMaintenanceWatchdog(scheduler, budget,
                                        () => FatalRecovery(authority, "Replication checkpoint maintenance made no forward progress."));
                                    _maintenanceWatchdogs.Add(watchdog);
                                    job.Watchdog = watchdog;
                                }
                        }
                }
                catch
                {
                    foreach (var job in _remoteMaintenance) job.Dispose();
                    _remoteMaintenance.Clear();
                    lock (_remoteLock)
                    {
                        foreach (var watchdog in _maintenanceWatchdogs) watchdog.Dispose();
                        _maintenanceWatchdogs.Clear();
                    }
                    throw;
                }
                if (!authority.IsValid) { DropLeadership(); return; }
                Volatile.Write(ref _serving, new(identity, authority, databases, publishers, host, scheduler, options.ApplicationGeneration));
            }
            lock (_remoteLock)
            {
                _activationWatchdog?.Dispose();
                _activationWatchdog = null;
            }
            ObservePublication(databases, publishers, authority);
            for (var i = 0; i < publishers.Count; i++)
            {
                var publisher = publishers[i];
                var result = await publisher.PublishNextAsync(true, cancellation).ConfigureAwait(false);
                ObservePublication(databases, publishers, authority, i);
                AdvanceCanonicalBase(databases[i], publisher.PublishedPosition);
                if (result is TrlPublishResult.Conflict or TrlPublishResult.AuthorityLost)
                {
                    authority.Fence();
                    DropLeadership();
                    return;
                }
            }
            // Checkpoint uploads can span many publication rounds; they must never hold back canonical TRL publication.
            if (_maintenanceRun is { IsCompleted: true } finished)
            {
                _maintenanceRun = null;
                await finished.ConfigureAwait(false); // Failures use the ordinary step error handling.
            }
            if (_maintenanceRun == null && _remoteMaintenance.Count != 0 && authority.IsValid && _serving?.Handoff == null)
                _maintenanceRun = RunMaintenanceAsync(_remoteMaintenance.ToArray(), authority, _remoteWork!.Token);
            Role = authority.IsValid ? ReplicationNodeRole.Leader : ReplicationNodeRole.Follower;
            return;
        }

        DropLeadership();
        Role = ReplicationNodeRole.Follower;
        if (_peer == null)
        {
            var json = await DiscoverAsync(databases, cancellation).ConfigureAwait(false);
            var term = LeaderJson.OptionalUInt64(json, "term");
            if (term == 0) return;
            var identity = new ReplicationPeerIdentity(options.ClusterId, term, LeaderJson.RequiredString(json, "sessionId"),
                LeaderJson.RequiredString(json, "peerEndpoint"), LeaderJson.RequiredString(json, "apiKey"));
            var leaderDatabases = LeaderJson.OptionalNames(json, "databaseNames") ?? [];
            _peer = await transport.ConnectAsync(identity, cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (_comparedLeader != (identity.Term, identity.SessionId))
            {
                _comparedLeader = (identity.Term, identity.SessionId);
                _resumeComparison.Clear();
                _scanners.Clear();
            }
            foreach (var database in databases)
            {
                if (_removed.Contains(database.Name) || _detached.Contains(database.Name) || database.RestoredBase.FileId == 0 || !leaderDatabases.Contains(database.Name, StringComparer.Ordinal))
                    continue;
                var canonical = _canonicalBase[database.Name];
                if (!_scanners.ContainsKey(database.Name))
                    _scanners.Add(database.Name, new(canonical, database.Database.FileCollection.Guid));
                _followers.Add(database.Name, new(database.Database.FileCollection.GetFile, database.Capture, _peer.Reader(database.Name),
                    scheduler, () => Restart("Follower native history diverged from its selected leader."),
                    _resumeComparison.GetValueOrDefault(database.Name, canonical), acknowledge: false));
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
            if (status.Published is { } published && follower.Compared is { } compared)
                AdvanceCanonicalBase(databases.First(d => d.Name == name),
                    Order(compared.Position) < Order(published) ? compared.Position : published);
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

    // Starts inline and continues independently of the transition lane after its first incomplete remote operation.
    async Task RunMaintenanceAsync(ReplicationMaintenance[] jobs, LeaseAuthority authority, CancellationToken cancellation)
    {
        foreach (var job in jobs)
        {
            if (!authority.IsValid || Volatile.Read(ref _serving)?.Handoff != null) return;
            await job.RunDueAsync(cancellation).ConfigureAwait(false);
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
        var json = LeaderJson.Parse(record.Json);
        if (LeaderJson.OptionalString(json, "clusterId") != options.ClusterId || LeaderJson.OptionalInt32(json, "format") != 1)
            throw new InvalidDataException("Leader discovery returned another cluster or format.");
        var generation = LeaderJson.OptionalUInt64(json, "applicationGeneration");
        if (generation > options.ApplicationGeneration) leases.Disqualify();
        if (generation >= options.ApplicationGeneration && LeaderJson.OptionalNames(json, "databaseNames") is { } names)
        {
            var selected = names.ToHashSet(StringComparer.Ordinal);
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
        ReportStatus();
        await ready.Task.ConfigureAwait(false);
    }

    void ReportStatus()
    {
        if (status != null)
        {
            var databases = new ReplicationDatabaseStatus[_databases.Count];
            var initialized = true;
            for (var i = 0; i < databases.Length; i++)
            {
                var name = _databases[i].Name;
                initialized &= _removed.Contains(name) || _databases[i].RestoredBase.FileId != 0 ||
                    _publishers != null && _publishers[i].PublishedPosition.FileId != 0;
                databases[i] = new(name, host.GetProgress(name),
                    _followers.TryGetValue(name, out var follower) ? follower.Compared : null,
                    _publishers is { } publishers ? publishers[i].PublishedPosition : null,
                    _removed.Contains(name), _detached.Contains(name));
            }
            var ready = initialized && !_restart && _detached.Count == 0 &&
                Role is ReplicationNodeRole.Follower or ReplicationNodeRole.Leader;
            status.Update(new(Role, ready, scheduler.Elapsed, Array.AsReadOnly(databases)));
        }
        host.ReportStatus(Role);
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
        StopWatchdogs();
        status?.Stop();
        leases.Close();
        host.RequestRestart(reason);
    }

    static ulong Order(TransactionLogPosition position) => ((ulong)position.FileId << 32) | position.Offset;

    // Canonical bytes need no recheck and no local retention for peers; release older local TRLs to compaction.
    void AdvanceCanonicalBase(ActivationDatabase database, TransactionLogPosition position)
    {
        if (position.FileId == 0 || !_canonicalBase.TryGetValue(database.Name, out var current) ||
            Order(position) <= Order(current)) return;
        _canonicalBase[database.Name] = position;
        if (Order(position) > Order(database.Capture.Acknowledged)) database.Capture.Acknowledge(position);
    }

    void Disconnect()
    {
        foreach (var (name, follower) in _followers)
        {
            follower.Close();
            if (follower.ResumePosition is { } resume) _resumeComparison[name] = resume;
        }
        _followers.Clear();
        _peer?.Dispose();
        _peer = null;
    }

    void ObservePublication(IReadOnlyList<ActivationDatabase> databases,
        IReadOnlyList<CanonicalTrlPublisher> publishers, LeaseAuthority authority, int? databaseIndex = null)
    {
        if (options.ProgressTimeouts is not { } deadlines) return;
        lock (_remoteLock)
        {
            if (Volatile.Read(ref _recoveryState) != 0) return;
            while (_publicationWatchdogs.Count < publishers.Count) _publicationWatchdogs.Add(default);
            var end = databaseIndex is { } index ? index + 1 : publishers.Count;
            for (var i = databaseIndex ?? 0; i < end; i++)
            {
                var position = publishers[i].PublishedPosition;
                var previous = _publicationWatchdogs[i];
                var completed = databases[i].Capture.Completed;
                var pending = completed.FileId > position.FileId ||
                    completed.FileId == position.FileId && completed.Offset > position.Offset;
                if (!pending)
                {
                    previous.Watchdog?.Dispose();
                    _publicationWatchdogs[i] = (null, position);
                    continue;
                }
                var watchdog = previous.Watchdog;
                if (watchdog == null)
                {
                    watchdog = new(scheduler, deadlines.Publication,
                        () => FatalRecovery(authority, "Replication publication made no forward progress."), "replication publication watchdog");
                    watchdog.Progress();
                }
                else if (position != previous.Position) watchdog.Progress();
                _publicationWatchdogs[i] = (watchdog, position);
            }
        }
    }

    void FatalRecovery(LeaseAuthority authority, string reason)
    {
        CancellationTokenSource? remoteWork;
        lock (_remoteLock)
        {
            if (!ReferenceEquals(_sessionAuthority, authority) ||
                Interlocked.CompareExchange(ref _recoveryState, 1, 0) != 0) return;
            _restart = true;
            status?.Stop();
            leases.Close(); // Fence before entering the host, without waiting for cancellation callbacks.
            DisposeWatchdogs();
            remoteWork = _remoteWork;
        }
        // Leader work has no request timeout; abort its outstanding remote operations too.
        try { remoteWork?.Cancel(); }
        catch (ObjectDisposedException) { }
        // Independent of worker cleanup/cancellation. Once fatal recovery wins, shutdown cannot bypass
        // this delay by returning from RunAsync and letting the hosted service stop the process early.
        scheduler.Schedule(options.ProgressTimeouts!.RestartDelay, () =>
        {
            try { ((IReplicationFatalRecovery)host).RequestFatalRestart(reason); }
            finally { _fatalRestart.TrySetResult(); }
        }, "replication fatal restart delay");
    }

    void StopWatchdogs()
    {
        Interlocked.CompareExchange(ref _recoveryState, 2, 0);
        lock (_remoteLock) DisposeWatchdogs();
    }

    // Caller holds _remoteLock. Disposing a deadline never waits for the worker it monitors.
    void DisposeWatchdogs()
    {
        _activationWatchdog?.Dispose();
        _activationWatchdog = null;
        foreach (var (watchdog, _) in _publicationWatchdogs) watchdog?.Dispose();
        _publicationWatchdogs.Clear();
        foreach (var watchdog in _maintenanceWatchdogs) watchdog.Dispose();
        _maintenanceWatchdogs.Clear();
    }

    void DropLeadership()
    {
        lock (_remoteLock)
        {
            DisposeWatchdogs();
            _sessionAuthority = null; // Already-dispatched old deadlines cannot recover a replacement session.
        }
        var serving = Interlocked.Exchange(ref _serving, null);
        serving?.Close();
        CancellationTokenSource? remoteWork;
        lock (_remoteLock)
        {
            remoteWork = _remoteWork;
            _remoteWork = null;
            _sessionAuthority = null;
        }
        remoteWork?.Cancel();
        var jobs = _remoteMaintenance.ToArray();
        _remoteMaintenance.Clear();
        var leadership = _leadership;
        var run = _maintenanceRun;
        _maintenanceRun = null;
        // A cancelled maintenance run may still be inside its snapshot or publisher; release them only after it ends.
        if (run is null or { IsCompleted: true })
        {
            _ = run?.Exception;
            ReleaseSession(jobs, leadership, remoteWork);
        }
        else
        {
            var released = run.ContinueWith(completed =>
            {
                _ = completed.Exception;
                ReleaseSession(jobs, leadership, remoteWork);
            }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            _maintenanceDrain = Task.WhenAll(_maintenanceDrain, released);
        }
        _publishers = null;
        _leadership = null;
        _candidate = null;
    }

    static void ReleaseSession(ReplicationMaintenance[] jobs, LeadershipSession? leadership, CancellationTokenSource? remoteWork)
    {
        foreach (var job in jobs) job.Dispose();
        leadership?.Dispose();
        remoteWork?.Dispose();
    }

    sealed class ServingLeader(ReplicationPeerIdentity identity, LeaseAuthority authority,
        IReadOnlyList<ActivationDatabase> databases, IReadOnlyList<CanonicalTrlPublisher> publishers,
        IReplicationNodeHost host, IReplicationScheduler scheduler, ulong generation)
    {
        readonly object _lock = new();
        readonly IReplicationNodeHost _host = host;
        readonly ulong _generation = generation;
        readonly ConfirmationGrants _grants = new(scheduler, authority);
        readonly Dictionary<string, LeaderTrlReader> _readers = databases.ToDictionary(d => d.Name,
            d => new LeaderTrlReader(d.Database, d.Capture, authority), StringComparer.Ordinal);
        readonly Dictionary<string, CanonicalTrlPublisher> _publishers = databases.Select((d, i) => (d.Name, publishers[i]))
            .ToDictionary(p => p.Name, p => p.Item2, StringComparer.Ordinal);
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
                    var published = database == null ? default : leader._publishers[database].PublishedPosition;
                    return ValueTask.FromResult(new ReplicationPeerProgress(challenge, leader._grants.TryIssue(duration),
                        database == null ? null : leader._host.GetProgress(database), published.FileId == 0 ? null : published));
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
                // Disk reads run outside the session lock so concurrent peer reads, polls and fencing never queue
                // behind them. The reader rechecks closure and authority after reading; so does this proxy.
                public async ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation)
                {
                    LeaderTrlReader reader;
                    lock (connection.Owner._lock)
                    {
                        connection.Check();
                        reader = connection.Owner._readers[database];
                    }
                    var count = await reader.ReadAsync(fileId, offset, destination, cancellation).ConfigureAwait(false);
                    lock (connection.Owner._lock) connection.Check();
                    return count;
                }
            }
        }
    }
}
