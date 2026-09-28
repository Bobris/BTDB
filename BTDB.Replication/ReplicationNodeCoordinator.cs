using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

public enum ReplicationNodeRole { Restoring, Follower, Activating, Leader, RestartRequired, Stopped }

/// <summary>RequestTimeout bounds each discovery and follower control/comparison step. Leader activation, publication
/// and checkpoint maintenance transfer bulk data, so they are bounded by lease authority and optional ProgressTimeouts.
/// DetachedLeaderTimeout (default 15 minutes) is how long a schema-detached node waits without leader evidence before
/// requesting a restart.</summary>
public sealed record ReplicationNodeOptions(string ClusterId, string Endpoint, TimeSpan PollInterval,
    TimeSpan LeaseRetryInterval, TimeSpan RequestTimeout, TimeSpan ConfirmationDuration, ulong ApplicationGeneration, TimeSpan? CompactionInterval = null,
    ReplicationProgressTimeouts? ProgressTimeouts = null, TimeSpan? DetachedLeaderTimeout = null)
{
    internal TimeSpan EffectiveCompactionInterval => CompactionInterval ?? TimeSpan.FromMinutes(5);
    internal TimeSpan EffectiveDetachedLeaderTimeout => DetachedLeaderTimeout ?? TimeSpan.FromMinutes(15);

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ClusterId);
        foreach (var duration in new[] { PollInterval, LeaseRetryInterval, RequestTimeout, ConfirmationDuration,
                     EffectiveCompactionInterval, EffectiveDetachedLeaderTimeout })
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(duration.Ticks);
        ProgressTimeouts?.Validate();
    }
}

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
    // Atomic snapshot supplied after complete local work, before starting the next application transaction. After a
    // restore and before the first local transaction, report the restored cut (its CommitUlong and
    // BTreeKeyValueDB.ReplicationRestoredPosition): a leader without progress gives followers nothing to compare.
    LeaderTrlProgress? GetProgress(string database);
    void RequestRestart(string reason);
    // Called only when progress deadlines expire, after immediate lease fencing and the configured RestartDelay,
    // independently of the stuck worker. Initiate bounded non-graceful process termination/restart without waiting
    // for handlers, storage calls or coordinator cleanup, and never reuse this node session. Must not block.
    void RequestFatalRestart(string reason);
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
    IReplicationLeaderStorage records, LeaseSessionController leases, IReplicationPeerTransport transport,
    IReplicationScheduler scheduler, ReplicationStatus? status = null)
{
    IReadOnlyList<ActivationDatabase> _databases = Array.Empty<ActivationDatabase>();
    IReadOnlyList<CanonicalTrlPublisher>? _publishers;
    // Each follower session owns the database and retained leader bytes used by its comparison.
    sealed record FollowingDatabase(ActivationDatabase Database, FollowerComparisonSession Follower,
        RetainingLeaderTrlReader Reader);

    readonly Dictionary<string, FollowingDatabase> _followers = new(StringComparer.Ordinal);
    readonly HashSet<string> _removed = new(StringComparer.Ordinal);
    readonly HashSet<string> _detached = new(StringComparer.Ordinal);
    // Databases selected by the connected leader. An older generation does not know databases added by an upgrade.
    readonly HashSet<string> _leaderDatabases = new(StringComparer.Ordinal);
    // Latest local cut known to be canonical: restored, published by this node, or compared with a leader that had
    // already published it. Rechecks, activation validation and local TRL retention start here, never at the
    // startup cut, whose files local compaction may already have removed. A database without an entry has no
    // canonical history on this node yet; one this node initialized gains it with its first publication.
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
        options.Validate();
        var detachedTimeout = options.EffectiveDetachedLeaderTimeout;
        using var shutdown = cancellation.Register(StopWatchdogs);
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        Task? maintenance = null;
        Task? localMaintenance = null;
        IDisposable? listener = null;
        try
        {
            IReadOnlyList<ActivationDatabase> databases;
            var restoreStarted = scheduler.Elapsed;
            while (true)
            {
                cancellation.ThrowIfCancellationRequested();
                status?.RestoreStarted();
                try { databases = await host.RestoreAsync(cancellation).ConfigureAwait(false); break; }
                catch (InvalidDataException)
                {
                    Restart("Conflicting canonical history was found during restore.");
                    return;
                }
                catch (IOException)
                {
                    status?.RestoreFailed();
                    await WaitAsync(cancellation).ConfigureAwait(false);
                }
            }
            status?.Restored(scheduler.Elapsed - restoreStarted);
            _databases = databases;
            foreach (var database in databases)
                if (database.RestoredBase.FileId != 0) _canonicalBase[database.Name] = database.RestoredBase;
            localMaintenance = RunLocalMaintenanceAsync(databases, lifetime.Token);
            listener = transport.Listen(options.Endpoint, Authenticate, Accept);
            // Observe the durable generation floor before the first lease request.
            while (true)
            {
                using var request = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
                using var timeout = scheduler.Schedule(options.RequestTimeout, () => ReplicationTimeouts.Cancel(request), "leader discovery timeout");
                try { await DiscoverAsync(databases, request.Token).ConfigureAwait(false); break; }
                catch (IOException) { }
                catch (OperationCanceledException) when (!cancellation.IsCancellationRequested) { }
                await WaitAsync(cancellation).ConfigureAwait(false);
            }
            maintenance = leases.RunAsync(options.LeaseRetryInterval, current =>
            {
                lock (_remoteLock)
                    if (_sessionAuthority != null && !ReferenceEquals(_sessionAuthority, current)) _remoteWork?.Cancel();
            }, lifetime.Token, options.RequestTimeout, options.ConfirmationDuration);
            Role = ReplicationNodeRole.Follower;
            while (!_restart)
            {
                cancellation.ThrowIfCancellationRequested();
                if (maintenance.IsCompleted) await maintenance.ConfigureAwait(false);
                if (localMaintenance.IsCompleted) await localMaintenance.ConfigureAwait(false);
                using var request = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
                using var timeout = scheduler.Schedule(options.RequestTimeout, () => ReplicationTimeouts.Cancel(request), "replication request timeout");
                try { await StepAsync(databases, request.Token, cancellation).ConfigureAwait(false); }
                catch (InvalidDataException) { Restart("Local history or database configuration requires canonical restore."); }
                catch (FileNotFoundException) { Restart("Required TRL bytes are no longer retained; canonical restore is required."); }
                catch (IOException) { status?.StepFailed(); Disconnect(); }
                catch (OperationCanceledException) when (!cancellation.IsCancellationRequested) { status?.StepFailed(); Disconnect(); }
                catch (InvalidOperationException) when (_sessionAuthority is { IsValid: false }) { DropLeadership(); }
                ReleaseUncoordinatedHistory(databases);
                if (_detached.Count != 0 && scheduler.Elapsed - _leaderEvidenceUntil >= detachedTimeout)
                    Restart("Detached node has had no valid leader within the configured timeout.");
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
                status?.LeaderSessionStarted();
            }
            lock (_remoteLock)
            {
                _activationWatchdog?.Dispose();
                _activationWatchdog = null;
            }
            ObservePublication(databases, publishers, authority);
            // Each database has its own publication lane and storage; one large backlog must not delay the others.
            var publications = new Task<TrlPublishResult>[publishers.Count];
            for (var i = 0; i < publishers.Count; i++)
                publications[i] = publishers[i].PublishNextAsync(true, cancellation).AsTask();
            var conflict = false;
            try
            {
                // A conflicting database must fence the node even if another provider call never completes.
                // Keep state transitions on this lane; then drain all calls before releasing their native sources.
                var pending = new List<Task<TrlPublishResult>>(publications);
                while (pending.Count != 0)
                {
                    var publication = await Task.WhenAny(pending).ConfigureAwait(false);
                    pending.Remove(publication);
                    if (conflict || !publication.IsCompletedSuccessfully || publication.Result != TrlPublishResult.Conflict)
                        continue;
                    conflict = true;
                    authority.Fence();
                    try { Restart("Canonical TRL conflicts with local history; restore is required."); }
                    finally { await remoteRequest.CancelAsync().ConfigureAwait(false); }
                }
            }
            finally
            {
                await ((Task)Task.WhenAll(publications)).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                // Observe every failure; only the first one propagates below when no conflict won.
                foreach (var publication in publications) _ = publication.Exception;
            }
            if (conflict) return;
            var lost = false;
            for (var i = 0; i < publishers.Count; i++)
            {
                ObservePublication(databases, publishers, authority, i);
                AdvanceCanonicalBase(databases[i], publishers[i].PublishedPosition);
                if (!publications[i].IsCompletedSuccessfully) continue;
                lost |= publications[i].Result == TrlPublishResult.AuthorityLost;
            }
            if (lost)
            {
                authority.Fence();
                DropLeadership();
                return;
            }
            // Every database recorded its progress; the first failure takes the ordinary step error handling.
            foreach (var publication in publications) await publication.ConfigureAwait(false);
            // Checkpoint uploads can span many publication rounds; they must never hold back canonical TRL publication.
            if (_maintenanceRun is { IsCompleted: true } finished)
            {
                _maintenanceRun = null;
                await finished.ConfigureAwait(false); // Failures use the ordinary step error handling.
            }
            if (_maintenanceRun == null && _remoteMaintenance.Count != 0 && authority.IsValid && _serving?.Handoff == null)
                _maintenanceRun = ReplicationMaintenance.RunDueAsync(_remoteMaintenance.ToArray(), _remoteWork!.Token);
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
            _leaderDatabases.Clear();
            _leaderDatabases.UnionWith(LeaderJson.OptionalNames(json, "databaseNames") ?? []);
            _peer = await transport.ConnectAsync(identity, cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (_comparedLeader != (identity.Term, identity.SessionId))
            {
                _comparedLeader = (identity.Term, identity.SessionId);
                _resumeComparison.Clear();
            }
            foreach (var database in databases)
            {
                if (_removed.Contains(database.Name) || _detached.Contains(database.Name) ||
                    !_canonicalBase.TryGetValue(database.Name, out var canonical) || !_leaderDatabases.Contains(database.Name))
                    continue;
                var reader = new RetainingLeaderTrlReader(_peer.Reader(database.Name));
                var name = database.Name;
                _followers.Add(name, new(database, new(database.Database.FileCollection.GetFile, database.Capture, reader,
                    () => Restart("Follower native history diverged from its selected leader."),
                    _resumeComparison.GetValueOrDefault(name, canonical), () => host.GetProgress(name)), reader));
            }
        }
        if (!await PollLeaderAsync(databases, cancellation).ConfigureAwait(false)) return;
        if (_detached.Count == 0 && host.PreparedUpgrade is { } offer && offer.ApplicationGeneration == options.ApplicationGeneration)
        {
            leases.ProposeTransfer(offer.TransferId);
            await _peer.OfferHandoffAsync(offer, cancellation).ConfigureAwait(false);
        }
    }

    // Inline TRL bytes one poll may carry, the most a leader honours; the rest of a larger backlog is read by range.
    // Measured: a 1 MiB budget cost several range round trips per poll above about 20 MiB/s of TRL.
    const int InlineBudget = ReplicationPeerPoll.MaximumInlineBytes;

    /// <summary>One poll per follower step: every compared database, every new database the leader selects, and the
    /// authority heartbeat share a single challenge and grant. A detached node without databases still polls for
    /// evidence. False means the step must stop (restart requested).</summary>
    async ValueTask<bool> PollLeaderAsync(IReadOnlyList<ActivationDatabase> databases, CancellationToken cancellation)
    {
        // Followers come first in the request. From asks the leader for the TRL bytes the comparison still needs,
        // starting after the bytes this session already retains.
        var polled = new List<ReplicationPeerPollRequest>();
        var followers = _followers.Values.ToArray();
        foreach (var (database, follower, reader) in followers)
        {
            var from = reader.Available >= InlineBudget ? reader.ContiguousEnd(follower.ResumePosition) : default;
            polled.Add(new(database.Name, from));
        }
        foreach (var database in databases)
            if (!_canonicalBase.ContainsKey(database.Name) && !_removed.Contains(database.Name) &&
                _leaderDatabases.Contains(database.Name)) polled.Add(new(database.Name));
        if (polled.Count == 0 && (_detached.Count == 0 || _leaderEvidenceUntil > scheduler.Elapsed)) return true;
        var dispatched = scheduler.Elapsed;
        var challenge = checked(++_monitorChallenge);
        var poll = await _peer!.PollAsync(polled, challenge, options.ConfirmationDuration, InlineBudget, cancellation)
            .ConfigureAwait(false);
        cancellation.ThrowIfCancellationRequested();
        poll.Validate(challenge, polled, InlineBudget);
        if (poll.Granted && scheduler.Elapsed < dispatched + options.ConfirmationDuration)
            _leaderEvidenceUntil = dispatched + options.ConfirmationDuration;
        for (var i = 0; i < followers.Length; i++)
        {
            var (database, follower, leaderReader) = followers[i];
            var name = database.Name;
            var status = poll.Databases[i];
            // A schema commit after the canonical base is never applied or compared; one covered by it is a duplicate.
            if (status.Schema is { } schema && schema > _canonicalBase[name])
            {
                leases.Disqualify();
                if (_detached.Count == 0 && _leaderEvidenceUntil < scheduler.Elapsed)
                    _leaderEvidenceUntil = scheduler.Elapsed;
                _detached.Add(name);
                follower.Close();
                _followers.Remove(name);
                host.SchemaDetached(name);
                continue;
            }
            try
            {
                if (status.Chunks is { } chunks) leaderReader.Retain(polled[i].From, chunks);
                if (status.Progress is { } progress)
                    await follower.CompareAsync(progress, cancellation).ConfigureAwait(false);
            }
            finally
            {
                // Keep bytes a lagging comparison still needs for a later step.
                if (_followers.ContainsKey(name)) leaderReader.Release(follower.ResumePosition);
            }
            if (_restart) return false;
            if (status.Published is { } published && follower.ComparedPosition is { } compared)
                AdvanceCanonicalBase(database, compared < published ? compared : published);
        }
        for (var i = followers.Length; i < polled.Count; i++)
            if (poll.Databases[i].Progress != null)
            { Restart("New database initialization is published; restore its fixed input cursor."); return false; }
        return true;
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
            // A zero predecessor cursor is otherwise an empty no-op writer. This forces a writing commit; replication
            // writes no temporary close marker.
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
        var interval = options.EffectiveCompactionInterval;
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
                initialized &= _removed.Contains(name) || _canonicalBase.ContainsKey(name);
                databases[i] = new(name, host.GetProgress(name),
                    _followers.TryGetValue(name, out var following) ? following.Follower.Compared : null,
                    _publishers is { } publishers ? publishers[i].PublishedPosition : null,
                    _removed.Contains(name), _detached.Contains(name));
            }
            var ready = initialized && !_restart && _detached.Count == 0 &&
                Role is ReplicationNodeRole.Follower or ReplicationNodeRole.Leader;
            status.Update(new(Role, ready, scheduler.Elapsed, Array.AsReadOnly(databases)));
        }
        host.ReportStatus(Role);
    }

    bool Authenticate(string apiKey) => Volatile.Read(ref _serving)?.Authenticate(apiKey) == true;

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

    // Canonical bytes need no recheck and no local retention for peers; release older local TRLs to compaction.
    // The first publication of a database this node initialized establishes its base, so losing the lease later
    // follows the next leader instead of restoring the whole node.
    void AdvanceCanonicalBase(ActivationDatabase database, TransactionLogPosition position)
    {
        if (position.FileId == 0 || _canonicalBase.TryGetValue(database.Name, out var current) && position <= current)
            return;
        _canonicalBase[database.Name] = position;
        if (position > database.Capture.Acknowledged) database.Capture.Acknowledge(position);
    }

    // Removed and detached databases are never compared or published again, yet keep executing locally. Nothing reads
    // their TRL for replication, so acknowledge it all; otherwise compaction retains every later TRL until shutdown.
    void ReleaseUncoordinatedHistory(IReadOnlyList<ActivationDatabase> databases)
    {
        foreach (var database in databases)
        {
            if (!_removed.Contains(database.Name) && !_detached.Contains(database.Name)) continue;
            var completed = database.Capture.Completed;
            if (completed > database.Capture.Acknowledged) database.Capture.Acknowledge(completed);
        }
    }

    void Disconnect()
    {
        foreach (var (name, following) in _followers)
        {
            following.Follower.Close();
            _resumeComparison[name] = following.Follower.ResumePosition;
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
                if (databases[i].Capture.Completed <= position)
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
            try { host.RequestFatalRestart(reason); }
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
        CancellationTokenSource? remoteWork;
        lock (_remoteLock)
        {
            DisposeWatchdogs();
            _sessionAuthority = null; // Already-dispatched old deadlines cannot recover a replacement session.
            remoteWork = _remoteWork;
            _remoteWork = null;
        }
        if (Interlocked.Exchange(ref _serving, null) is { } serving)
        {
            serving.Close();
            status?.LeaderSessionEnded();
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
}
