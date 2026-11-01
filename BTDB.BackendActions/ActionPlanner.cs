namespace BTDB.BackendActions;

/// <summary>Planning limits; the defaults follow the reference implementation.</summary>
public sealed class ActionPlannerOptions
{
    /// <summary>How long a started attempt stays assigned without its worker reporting it.</summary>
    public TimeSpan Lease { get; set; } = TimeSpan.FromMinutes(2);
    /// <summary>A worker silent this long is gone, with all its attempts.</summary>
    public TimeSpan WorkerTimeout { get; set; } = TimeSpan.FromSeconds(30);
    /// <summary>After this many starts, not counting attempts their stopping worker interrupted, an action whose attempt
    /// is lost finishes as Interrupted.</summary>
    public ulong MaxAttempts { get; set; } = 5;
    /// <summary>Share of a worker's capacity that actions of each priority may fill.</summary>
    public Dictionary<ActionPriority, double> CapacityShares { get; set; } = new()
    {
        [ActionPriority.RealTime] = 1.0, [ActionPriority.Normal] = 0.8, [ActionPriority.Low] = 0.6,
        [ActionPriority.Idle] = 0.6
    };
    /// <summary>Per second, a tenant's past consumption decays by this factor before its running CPU is added.</summary>
    public double ConsumptionDecay { get; set; } = 0.95;
    /// <summary>Replaces an action type's CPU cost, given its name and attribute cost, for example from
    /// configuration; evaluated when the coordinator learns of an action.</summary>
    public Func<string, int, int>? CpuOverride { get; set; }
}

/// <summary>One action as the coordinator sees it: its replicated row plus the coordinator's assignment.</summary>
internal sealed class PlannedAction
{
    internal required QualifiedActionId Id { get; init; }
    internal required ActionType Type { get; init; }
    internal ActionPriority Priority { get; set; }
    internal ulong Order { get; set; }
    internal ulong RunAttempts { get; set; }
    /// <summary>Attempts their stopping worker interrupted, which do not count toward MaxAttempts.</summary>
    internal ulong InterruptedAttempts { get; set; }
    internal bool IsCancelled { get; set; }
    internal bool IsStarted { get; set; }
    internal ulong StartEventId { get; set; }
    /// <summary>The recorded attempt's worker.</summary>
    internal Guid RecordedSession { get; set; }
    internal string RecordedNode { get; set; } = "";
    /// <summary>Worker session holding the action's resources, while starting or running.</summary>
    internal Guid? Worker { get; set; }
    /// <summary>The attempt whose ActionsStarted event is published but not yet applied.</summary>
    internal ulong? Starting { get; set; }
    /// <summary>Until when the worker's assignment, the protection or the finished report holds.</summary>
    internal TimeSpan LeaseUntil { get; set; }
    /// <summary>Started under an earlier coordinator session; it may still run until its worker reports or the lease
    /// passes.</summary>
    internal bool Protected { get; set; }
    /// <summary>Counted in the concurrency limits: held by a worker or protected.</summary>
    internal bool Counted { get; set; }
    /// <summary>Its worker reported the attempt finished; its ActionFinished is not applied here yet.</summary>
    internal bool Finished { get; set; }
    /// <summary>The coordinator published the attempt's ActionFinished itself.</summary>
    internal bool Abandoned { get; set; }
    /// <summary>The type's cost, or its override, plus the running attempt's reported adjustment; at least 10.</summary>
    internal int Cpu { get; set; }
    internal int BaseCpu { get; set; }
    /// <summary>The CPU counted on the holding worker.</summary>
    internal int HeldCpu { get; set; }
    internal string Group => Type.ConcurrencyGroup ?? Type.Name;
    /// <summary>Nothing runs or may run the recorded attempt, and nothing is about to finish it.</summary>
    internal bool Idle => Worker == null && Starting == null && !Protected && !Finished && !Abandoned;
    internal bool Runnable => Idle && !IsCancelled;
}

internal sealed class PlannedWorker
{
    internal required Guid Session { get; init; }
    internal required string NodeId { get; init; }
    internal int Capacity { get; set; }
    /// <summary>The worker's application generation; an older one than the coordinator's starts nothing new.</summary>
    internal ulong Generation { get; set; }
    internal bool Ready { get; set; }
    internal TimeSpan ExpiresAt { get; set; }
    internal TimeSpan LastExchange { get; set; }
    internal int Used { get; set; }
    internal readonly HashSet<QualifiedActionId> Actions = new();
}

internal readonly record struct Assignment(QualifiedActionId Id, ulong Attempt, Guid Worker, string Node);

internal readonly record struct Abandonment(QualifiedActionId Id, ulong Attempt, ActionOutcomeKind Kind, string Reason);

/// <summary>A worker's attempt; Cancelling when the worker already ends it, so it needs no Cancel command.</summary>
internal readonly record struct ReportedAttempt(QualifiedActionId Id, ulong Attempt, int CpuDelta = 0,
    bool Cancelling = false);

internal enum WorkerCommandKind : byte { Start = 1, Cancel = 2, Forget = 3 }

internal readonly record struct WorkerCommand(WorkerCommandKind Kind, QualifiedActionId Id, ulong Attempt,
    ulong StartEventId = 0);

/// <summary>
/// Deterministic planning state of one coordinator session; all times come from the caller's monotonic clock. Tenants
/// are served in ascending order of decayed CPU consumption, one action per tenant per round; within a tenant by
/// priority, then enqueue order. An action starts on the next ready worker, round robin, whose application generation
/// is not older than the coordinator's and that has room for its cost within its priority's capacity share; concurrency
/// groups and single-running tenants limit it, counting protected attempts too. An older worker keeps the attempts
/// it already runs.
/// </summary>
internal sealed class ActionPlanner(ActionPlannerOptions options, ulong generation = 0)
{
    static readonly Comparer<PlannedAction> TenantOrder = Comparer<PlannedAction>.Create((a, b) =>
    {
        var result = a.Priority.CompareTo(b.Priority);
        if (result == 0) result = a.Order.CompareTo(b.Order);
        return result != 0 ? result : a.Id.ActionId.CompareTo(b.Id.ActionId);
    });

    readonly Dictionary<QualifiedActionId, PlannedAction> _actions = new();
    readonly Dictionary<ulong, SortedSet<PlannedAction>> _tenants = new();
    readonly Dictionary<ulong, double> _consumption = new();
    readonly Dictionary<Guid, PlannedWorker> _workers = new();
    readonly List<Guid> _workerOrder = new();
    readonly Dictionary<string, int> _runningGroups = new(StringComparer.Ordinal);
    readonly Dictionary<(string Group, ulong Tenant), int> _runningTenantGroups = new();
    /// <summary>CPU of the attempts workers hold, per tenant.</summary>
    readonly Dictionary<ulong, int> _runningCpu = new();
    int _nextWorker;
    TimeSpan _decayedUntil;
    IReadOnlySet<string> _suspended = new HashSet<string>(StringComparer.Ordinal);
    IReadOnlySet<ulong> _suspendedTenants = new HashSet<ulong>();
    /// <summary>The last plan found nothing to start and nothing it depends on changed since.</summary>
    bool _settled;

    internal IReadOnlyDictionary<QualifiedActionId, PlannedAction> Actions => _actions;
    internal IReadOnlyDictionary<Guid, PlannedWorker> Workers => _workers;
    /// <summary>Action types that start no new attempts.</summary>
    internal IReadOnlySet<string> Suspended
    {
        get => _suspended;
        set
        {
            _suspended = value;
            _settled = false;
        }
    }

    internal IReadOnlySet<ulong> SuspendedTenants
    {
        get => _suspendedTenants;
        set
        {
            _suspendedTenants = value;
            _settled = false;
        }
    }

    /// <summary>Applies an action's committed row, or its removal when row is null. A started attempt this session did
    /// not start is protected for one lease, so a new coordinator does not start it twice while it may still run.</summary>
    internal void Update(QualifiedActionId id, QueuedAction? row, ActionType? type, TimeSpan now)
    {
        _settled = false;
        _actions.TryGetValue(id, out var action);
        if (row == null || type == null)
        {
            if (action != null) Remove(action);
            return;
        }
        var unknownAttempt = row.IsStarted;
        if (action == null)
        {
            action = new() { Id = id, Type = type };
            action.BaseCpu = Math.Max(10, options.CpuOverride?.Invoke(type.Name, type.Cpu) ?? type.Cpu);
            action.Cpu = action.BaseCpu;
            _actions.Add(id, action);
        }
        else
        {
            _tenants[id.TenantId].Remove(action);
            if (action.Starting is { } starting)
            {
                // The start event is applied: either it recorded this attempt or it rejected it.
                action.Starting = null;
                if (!row.IsStarted || row.RunAttempts != starting) Release(action);
                unknownAttempt = false;
            }
            else if (row.RunAttempts == action.RunAttempts) unknownAttempt = false;
            if (!row.IsStarted && action.Starting == null && action.Worker is { } holder)
            {
                // Its worker interrupted the attempt, which only a stopping worker does: give it nothing new.
                if (_workers.TryGetValue(holder, out var stopping)) stopping.Ready = false;
                Release(action);
            }
        }
        if (row.RunAttempts != action.RunAttempts)
        {
            if (unknownAttempt) Release(action); // another session recorded a newer attempt
            action.Protected = false;
            action.Finished = false;
            action.Abandoned = false;
        }
        if (unknownAttempt)
        {
            action.Protected = true;
            action.LeaseUntil = now + options.Lease;
        }
        action.Priority = row.Priority;
        action.Order = row.Order;
        action.IsCancelled = row.IsCancelled;
        action.IsStarted = row.IsStarted;
        action.StartEventId = row.StartEventId;
        action.RecordedSession = row.WorkerSession;
        action.RecordedNode = row.WorkerNode;
        action.RunAttempts = row.RunAttempts;
        action.InterruptedAttempts = row.InterruptedAttempts;
        if (action.Protected && _workers.Values.Any(w => w.NodeId == action.RecordedNode && w.Session != action.RecordedSession))
            action.Protected = false; // its node already runs a newer process
        Count(action);
        if (!_tenants.TryGetValue(id.TenantId, out var tenant))
            _tenants.Add(id.TenantId, tenant = new(TenantOrder));
        tenant.Add(action);
        _consumption.TryAdd(id.TenantId, 0);
    }

    /// <summary>Registers a worker's exchange and returns the commands it should execute now. Reported running
    /// attempts renew their lease or are adopted after a coordinator change; unknown or superseded ones are forgotten.
    /// Finished attempts stop being started again until their ActionFinished is applied here. Lost attempts free their
    /// worker, so the action gets a new attempt or, after its last one, finishes as Interrupted.</summary>
    internal List<WorkerCommand> Exchange(Guid session, string nodeId, int capacity, ulong workerGeneration, bool ready,
        IReadOnlyList<ReportedAttempt> running, IReadOnlyList<ReportedAttempt> finished, TimeSpan now,
        IReadOnlyList<ReportedAttempt>? lost = null)
    {
        foreach (var other in _workers.Values.Where(w => w.NodeId == nodeId && w.Session != session).ToList())
            Expire(other); // the node restarted: its previous process's attempts are gone
        var commands = new List<WorkerCommand>();
        var reported = new HashSet<QualifiedActionId>();
        foreach (var attempt in running) reported.Add(attempt.Id);
        if (!_workers.TryGetValue(session, out var worker))
        {
            // Later protections of this node end in Update, so only a new session needs this scan.
            foreach (var action in _actions.Values)
                if (action.Protected && action.RecordedNode == nodeId && action.RecordedSession != session &&
                    !reported.Contains(action.Id))
                {
                    action.Protected = false; // recorded by an earlier coordinator for that previous process
                    Count(action);
                }
            worker = new() { Session = session, NodeId = nodeId };
            _workers.Add(session, worker);
            _workerOrder.Add(session);
            _settled = false;
        }
        if (worker.Capacity != capacity || worker.Generation != workerGeneration || worker.Ready != ready)
            _settled = false;
        worker.Capacity = capacity;
        worker.Generation = workerGeneration;
        worker.Ready = ready;
        worker.ExpiresAt = now + options.WorkerTimeout;
        worker.LastExchange = now;
        foreach (var attempt in finished)
        {
            reported.Add(attempt.Id);
            if (!_actions.TryGetValue(attempt.Id, out var action) || action.RunAttempts != attempt.Attempt) continue;
            if (action.Worker == session) Release(action);
            action.Finished = true;
            action.LeaseUntil = now + options.Lease; // reruns if its ActionFinished never applies
        }
        foreach (var attempt in lost ?? [])
        {
            if (_actions.TryGetValue(attempt.Id, out var action) && action.RunAttempts == attempt.Attempt &&
                action.Worker == session && action.Starting == null)
                Release(action);
        }
        if (!ready)
        {
            Expire(worker); // it stops without results for its running attempts, so they run again elsewhere
            return commands;
        }
        foreach (var attempt in running)
        {
            reported.Add(attempt.Id);
            if (!_actions.TryGetValue(attempt.Id, out var action) || !action.IsStarted || action.Starting != null ||
                action.RunAttempts != attempt.Attempt || action.Finished || action.Abandoned ||
                action.Worker != null && action.Worker != session)
            {
                commands.Add(new(WorkerCommandKind.Forget, attempt.Id, attempt.Attempt));
                continue;
            }
            if (action.Worker == null)
            {
                action.Protected = false; // adopted: it was still running
                Acquire(action, worker, now);
            }
            action.LeaseUntil = now + options.Lease;
            var cpu = attempt.CpuDelta == 0 ? action.BaseCpu : Math.Max(10, action.BaseCpu + attempt.CpuDelta);
            if (cpu != action.HeldCpu)
            {
                worker.Used += cpu - action.HeldCpu;
                Add(_runningCpu, action.Id.TenantId, cpu - action.HeldCpu);
                action.HeldCpu = cpu;
                _settled = false;
            }
            action.Cpu = cpu;
            if (action.IsCancelled && !attempt.Cancelling)
                commands.Add(new(WorkerCommandKind.Cancel, attempt.Id, attempt.Attempt));
        }
        foreach (var id in worker.Actions.ToList())
        {
            var action = _actions[id];
            if (reported.Contains(id) || action.Starting != null || !action.IsStarted) continue;
            if (action.IsCancelled) Release(action); // never delivered, so nothing runs it
            else commands.Add(new(WorkerCommandKind.Start, id, action.RunAttempts, action.StartEventId));
        }
        return commands;
    }

    /// <summary>Ends lost workers, attempts whose lease passed and protections nobody adopted. A started action that
    /// nothing runs any more becomes runnable again, unless it is cancelled or used all attempts; those the coordinator
    /// finishes itself with the returned outcomes.</summary>
    internal List<Abandonment> Expire(TimeSpan now)
    {
        foreach (var worker in _workers.Values.Where(w => w.ExpiresAt <= now).ToList()) Expire(worker);
        var abandoned = new List<Abandonment>();
        foreach (var action in _actions.Values)
        {
            if (action.LeaseUntil <= now)
            {
                if (action.Worker != null && action.Starting == null) Release(action);
                if (action.Protected || action.Finished)
                {
                    action.Protected = false;
                    action.Finished = false; // its ActionFinished never applied
                    Count(action);
                }
            }
            if (!action.IsStarted || !action.Idle) continue;
            if (action.IsCancelled)
                abandoned.Add(new(action.Id, action.RunAttempts, ActionOutcomeKind.Cancelled, "WorkerLost"));
            else if (action.RunAttempts - action.InterruptedAttempts >= options.MaxAttempts)
                abandoned.Add(new(action.Id, action.RunAttempts, ActionOutcomeKind.Interrupted, "AttemptsExhausted"));
            else continue;
            action.Abandoned = true;
        }
        return abandoned;
    }

    /// <summary>Assigns runnable actions to workers; the caller publishes one ActionsStarted with the result.</summary>
    internal List<Assignment> Plan(TimeSpan now)
    {
        Decay(now);
        var assignments = new List<Assignment>();
        if (_settled || !_workers.Values.Any(w => w.Ready)) return assignments;
        bool progress;
        do
        {
            progress = false;
            foreach (var tenant in _consumption.OrderBy(c => c.Value).ThenBy(c => c.Key).Select(c => c.Key).ToList())
            {
                if (SuspendedTenants.Contains(tenant) || !_tenants.TryGetValue(tenant, out var actions)) continue;
                foreach (var action in actions)
                {
                    if (!action.Runnable || Suspended.Contains(action.Type.Name) || !GroupAdmits(action)) continue;
                    var worker = PickWorker(action);
                    if (worker == null) continue;
                    var attempt = action.RunAttempts + 1;
                    Acquire(action, worker, now);
                    action.Starting = attempt;
                    _consumption[tenant] += action.Cpu;
                    assignments.Add(new(action.Id, attempt, worker.Session, worker.NodeId));
                    progress = true;
                    break;
                }
            }
        } while (progress);
        _settled = true; // a mutation or a new worker report makes the next plan look again
        return assignments;
    }

    // Each whole second decays the consumption and adds the running CPU, which is constant since the last call, so the
    // seconds collapse into a geometric series.
    void Decay(TimeSpan now)
    {
        if (_decayedUntil == default) _decayedUntil = now;
        var seconds = (now - _decayedUntil).Ticks / TimeSpan.TicksPerSecond;
        if (seconds <= 0) return;
        _decayedUntil += TimeSpan.FromSeconds(seconds);
        var decay = options.ConsumptionDecay;
        var factor = Math.Pow(decay, seconds);
        var sum = decay == 1 ? seconds : (1 - factor) / (1 - decay);
        foreach (var tenant in _consumption.Keys.ToList())
        {
            var running = _runningCpu.GetValueOrDefault(tenant);
            var value = _consumption[tenant] * factor + running * sum;
            if (value <= 0.1 && running == 0 && !_tenants.ContainsKey(tenant)) _consumption.Remove(tenant);
            else _consumption[tenant] = value <= 0.1 ? 0 : value;
        }
    }

    bool GroupAdmits(PlannedAction action)
    {
        if (action.Type.MaxConcurrent > 0 && _runningGroups.GetValueOrDefault(action.Group) >= action.Type.MaxConcurrent)
            return false;
        return !action.Type.SingleRunningPerTenant ||
               _runningTenantGroups.GetValueOrDefault((action.Group, action.Id.TenantId)) == 0;
    }

    PlannedWorker? PickWorker(PlannedAction action)
    {
        var share = options.CapacityShares.GetValueOrDefault(action.Priority, 1.0);
        for (var i = 0; i < _workerOrder.Count; i++)
        {
            var worker = _workers[_workerOrder[(_nextWorker + i) % _workerOrder.Count]];
            if (!worker.Ready || worker.Generation < generation) continue;
            // An idle worker takes any single action; otherwise the cost must fit the priority's share.
            if (worker.Used != 0 && worker.Used + action.BaseCpu > Math.Max(100, worker.Capacity * share)) continue;
            _nextWorker = (_nextWorker + i + 1) % _workerOrder.Count;
            return worker;
        }
        return null;
    }

    void Acquire(PlannedAction action, PlannedWorker worker, TimeSpan now)
    {
        action.Worker = worker.Session;
        action.LeaseUntil = now + options.Lease;
        worker.Actions.Add(action.Id);
        action.Cpu = action.BaseCpu; // a new attempt starts without the previous one's adjustment
        action.HeldCpu = action.Cpu;
        worker.Used += action.HeldCpu;
        Add(_runningCpu, action.Id.TenantId, action.HeldCpu);
        Count(action);
    }

    void Release(PlannedAction action)
    {
        if (action.Worker is not { } session) return;
        action.Worker = null;
        if (_workers.TryGetValue(session, out var worker) && worker.Actions.Remove(action.Id)) worker.Used -= action.HeldCpu;
        Add(_runningCpu, action.Id.TenantId, -action.HeldCpu);
        Count(action);
        _settled = false;
    }

    // Concurrency limits count every attempt that runs or may still run: held by a worker or protected after a
    // coordinator change, so a new coordinator does not start a second one next to it.
    void Count(PlannedAction action)
    {
        var counted = action.Worker != null || action.Protected;
        if (counted == action.Counted) return;
        action.Counted = counted;
        _settled = false;
        var delta = counted ? 1 : -1;
        Add(_runningGroups, action.Group, delta);
        Add(_runningTenantGroups, (action.Group, action.Id.TenantId), delta);
    }

    static void Add<TKey>(Dictionary<TKey, int> counts, TKey key, int delta) where TKey : notnull
    {
        var value = counts.GetValueOrDefault(key) + delta;
        if (value == 0) counts.Remove(key);
        else counts[key] = value;
    }

    void Expire(PlannedWorker worker)
    {
        foreach (var id in worker.Actions.ToList()) Release(_actions[id]);
        _settled = false;
        _workers.Remove(worker.Session);
        _workerOrder.Remove(worker.Session);
        if (_nextWorker >= _workerOrder.Count) _nextWorker = 0;
    }

    void Remove(PlannedAction action)
    {
        Release(action);
        action.Protected = false;
        Count(action);
        _actions.Remove(action.Id);
        var tenant = _tenants[action.Id.TenantId];
        tenant.Remove(action);
        if (tenant.Count == 0) _tenants.Remove(action.Id.TenantId);
    }
}
