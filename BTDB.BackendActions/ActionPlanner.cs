namespace BTDB.BackendActions;

/// <summary>Planning limits; the defaults follow the reference implementation.</summary>
public sealed class ActionPlannerOptions
{
    /// <summary>How long a started attempt stays assigned without its worker reporting it.</summary>
    public TimeSpan Lease { get; set; } = TimeSpan.FromMinutes(2);
    /// <summary>A worker silent this long is gone, with all its attempts.</summary>
    public TimeSpan WorkerTimeout { get; set; } = TimeSpan.FromSeconds(30);
    /// <summary>After this many starts an action whose attempt is lost finishes as Interrupted.</summary>
    public ulong MaxAttempts { get; set; } = 5;
    /// <summary>Share of a worker's capacity that actions of each priority may fill.</summary>
    public Dictionary<ActionPriority, double> CapacityShares { get; set; } = new()
    {
        [ActionPriority.RealTime] = 1.0, [ActionPriority.Normal] = 0.8, [ActionPriority.Low] = 0.6,
        [ActionPriority.Idle] = 0.6
    };
    /// <summary>Per second, a tenant's past consumption decays by this factor before its running CPU is added.</summary>
    public double ConsumptionDecay { get; set; } = 0.95;
}

/// <summary>One action as the coordinator sees it: its replicated row plus the coordinator's assignment.</summary>
internal sealed class PlannedAction
{
    internal required QualifiedActionId Id { get; init; }
    internal required ActionType Type { get; init; }
    internal ActionPriority Priority { get; set; }
    internal ulong Order { get; set; }
    internal ulong RunAttempts { get; set; }
    internal bool IsCancelled { get; set; }
    internal bool IsStarted { get; set; }
    internal ulong StartEventId { get; set; }
    /// <summary>Worker session holding the action's resources, while starting or running.</summary>
    internal Guid? Worker { get; set; }
    /// <summary>The attempt whose ActionsStarted event is published but not yet applied.</summary>
    internal ulong? Starting { get; set; }
    /// <summary>Until when the worker's assignment, the protection or the finished report holds.</summary>
    internal TimeSpan LeaseUntil { get; set; }
    /// <summary>Started under an earlier coordinator session; it may still run until its worker reports or the lease
    /// passes.</summary>
    internal bool Protected { get; set; }
    /// <summary>Its worker reported the attempt finished; its ActionFinished is not applied here yet.</summary>
    internal bool Finished { get; set; }
    /// <summary>The coordinator published the attempt's ActionFinished itself.</summary>
    internal bool Abandoned { get; set; }
    internal int Cpu => Type.Cpu;
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
    internal HashSet<string> Names { get; set; } = new(StringComparer.Ordinal);
    internal bool Ready { get; set; }
    internal TimeSpan ExpiresAt { get; set; }
    internal int Used { get; set; }
    internal readonly HashSet<QualifiedActionId> Actions = new();
}

internal readonly record struct Assignment(QualifiedActionId Id, ulong Attempt, Guid Worker);

internal readonly record struct Abandonment(QualifiedActionId Id, ulong Attempt, ActionOutcomeKind Kind, string Reason);

internal readonly record struct ReportedAttempt(QualifiedActionId Id, ulong Attempt);

internal enum WorkerCommandKind : byte { Start = 1, Cancel = 2, Forget = 3 }

internal readonly record struct WorkerCommand(WorkerCommandKind Kind, QualifiedActionId Id, ulong Attempt,
    ulong StartEventId = 0);

/// <summary>
/// Deterministic planning state of one coordinator session; all times come from the caller's monotonic clock. Tenants
/// are served in ascending order of decayed CPU consumption, one action per tenant per round; within a tenant by
/// priority, then enqueue order. An action starts on the next ready worker, round robin, that supports its type and
/// has room for its cost within its priority's capacity share; concurrency groups and single-running tenants limit it.
/// </summary>
internal sealed class ActionPlanner(ActionPlannerOptions options)
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
    int _nextWorker;
    TimeSpan _decayedUntil;

    internal IReadOnlyDictionary<QualifiedActionId, PlannedAction> Actions => _actions;
    internal IReadOnlyDictionary<Guid, PlannedWorker> Workers => _workers;

    /// <summary>Applies an action's committed row, or its removal when row is null. A started attempt this session did
    /// not start is protected for one lease, so a new coordinator does not start it twice while it may still run.</summary>
    internal void Update(QualifiedActionId id, QueuedAction? row, ActionType? type, TimeSpan now)
    {
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
        action.RunAttempts = row.RunAttempts;
        if (!_tenants.TryGetValue(id.TenantId, out var tenant))
            _tenants.Add(id.TenantId, tenant = new(TenantOrder));
        tenant.Add(action);
        _consumption.TryAdd(id.TenantId, 0);
    }

    /// <summary>Registers a worker's exchange and returns the commands it should execute now. Reported running
    /// attempts renew their lease or are adopted after a coordinator change; unknown or superseded ones are forgotten.
    /// Finished attempts stop being started again until their ActionFinished is applied here.</summary>
    internal List<WorkerCommand> Exchange(Guid session, string nodeId, int capacity, IEnumerable<string> names, bool ready,
        IReadOnlyList<ReportedAttempt> running, IReadOnlyList<ReportedAttempt> finished, TimeSpan now)
    {
        foreach (var other in _workers.Values.Where(w => w.NodeId == nodeId && w.Session != session).ToList())
            Expire(other); // the node restarted: its previous process's attempts are gone
        if (!_workers.TryGetValue(session, out var worker))
        {
            worker = new() { Session = session, NodeId = nodeId };
            _workers.Add(session, worker);
            _workerOrder.Add(session);
        }
        worker.Capacity = capacity;
        worker.Names = new(names, StringComparer.Ordinal);
        worker.Ready = ready;
        worker.ExpiresAt = now + options.WorkerTimeout;
        var commands = new List<WorkerCommand>();
        var reported = new HashSet<QualifiedActionId>();
        foreach (var attempt in finished)
        {
            reported.Add(attempt.Id);
            if (!_actions.TryGetValue(attempt.Id, out var action) || action.RunAttempts != attempt.Attempt) continue;
            if (action.Worker == session) Release(action);
            action.Finished = true;
            action.LeaseUntil = now + options.Lease; // reruns if its ActionFinished never applies
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
            if (action.IsCancelled) commands.Add(new(WorkerCommandKind.Cancel, attempt.Id, attempt.Attempt));
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
                action.Protected = false;
                action.Finished = false; // its ActionFinished never applied
            }
            if (!action.IsStarted || !action.Idle) continue;
            if (action.IsCancelled)
                abandoned.Add(new(action.Id, action.RunAttempts, ActionOutcomeKind.Cancelled, "WorkerLost"));
            else if (action.RunAttempts >= options.MaxAttempts)
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
        if (!_workers.Values.Any(w => w.Ready)) return assignments;
        bool progress;
        do
        {
            progress = false;
            foreach (var tenant in _consumption.OrderBy(c => c.Value).ThenBy(c => c.Key).Select(c => c.Key).ToList())
            {
                if (!_tenants.TryGetValue(tenant, out var actions)) continue;
                foreach (var action in actions)
                {
                    if (!action.Runnable || !GroupAdmits(action)) continue;
                    var worker = PickWorker(action);
                    if (worker == null) continue;
                    var attempt = action.RunAttempts + 1;
                    Acquire(action, worker, now);
                    action.Starting = attempt;
                    _consumption[tenant] += action.Cpu;
                    assignments.Add(new(action.Id, attempt, worker.Session));
                    progress = true;
                    break;
                }
            }
        } while (progress);
        return assignments;
    }

    void Decay(TimeSpan now)
    {
        if (_decayedUntil == default) _decayedUntil = now;
        while (now - _decayedUntil >= TimeSpan.FromSeconds(1))
        {
            _decayedUntil += TimeSpan.FromSeconds(1);
            foreach (var tenant in _consumption.Keys.ToList())
            {
                var running = _tenants.TryGetValue(tenant, out var actions)
                    ? actions.Where(a => a.Worker != null).Sum(a => a.Cpu) : 0;
                var value = _consumption[tenant] * options.ConsumptionDecay + running;
                if (value <= 0.1 && running == 0 && actions is not { Count: > 0 }) _consumption.Remove(tenant);
                else _consumption[tenant] = value <= 0.1 ? 0 : value;
            }
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
            if (!worker.Ready || !worker.Names.Contains(action.Type.Name)) continue;
            // An idle worker takes any single action; otherwise the cost must fit the priority's share.
            if (worker.Used != 0 && worker.Used + action.Cpu > Math.Max(100, worker.Capacity * share)) continue;
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
        worker.Used += action.Cpu;
        _runningGroups[action.Group] = _runningGroups.GetValueOrDefault(action.Group) + 1;
        var key = (action.Group, action.Id.TenantId);
        _runningTenantGroups[key] = _runningTenantGroups.GetValueOrDefault(key) + 1;
    }

    void Release(PlannedAction action)
    {
        if (action.Worker is not { } session) return;
        action.Worker = null;
        if (_workers.TryGetValue(session, out var worker) && worker.Actions.Remove(action.Id)) worker.Used -= action.Cpu;
        _runningGroups[action.Group]--;
        _runningTenantGroups[(action.Group, action.Id.TenantId)]--;
    }

    void Expire(PlannedWorker worker)
    {
        foreach (var id in worker.Actions.ToList()) Release(_actions[id]);
        _workers.Remove(worker.Session);
        _workerOrder.Remove(worker.Session);
        if (_nextWorker >= _workerOrder.Count) _nextWorker = 0;
    }

    void Remove(PlannedAction action)
    {
        Release(action);
        _actions.Remove(action.Id);
        var tenant = _tenants[action.Id.TenantId];
        tenant.Remove(action);
        if (tenant.Count == 0) _tenants.Remove(action.Id.TenantId);
    }
}
