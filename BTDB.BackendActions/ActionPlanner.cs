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
    /// <summary>How long a runnable action may wait while actions of its own or a less urgent priority start in its
    /// place, because they fit a worker and it does not. Then it reserves the least used worker, which starts nothing
    /// else until the action fits it, at the latest once it is idle. <see cref="TimeSpan.MaxValue"/> never
    /// reserves.</summary>
    public TimeSpan ReserveAfter { get; set; } = TimeSpan.FromSeconds(30);
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
    /// <summary>Since when actions of its priority or a less urgent one started in its place, because it did not fit
    /// any worker; cleared when it starts.</summary>
    internal TimeSpan? BypassedSince { get; set; }
    /// <summary>The worker reserved for it.</summary>
    internal Guid? Reservation { get; set; }
    internal string Group => Type.ConcurrencyGroup ?? Type.Name;
    /// <summary>Nothing runs or may run the recorded attempt, and nothing is about to finish it.</summary>
    internal bool Idle => Worker == null && Starting == null && !Protected && !Finished && !Abandoned;
    internal bool Runnable => Idle && !IsCancelled;

    // The planner's indexes of this action.
    /// <summary>The lane it waits in, while it may start, and its node there unless it waits among the early ones.</summary>
    internal ActionLane? Lane;
    internal LinkedListNode<PlannedAction>? LaneNode;
    /// <summary>The time of its entry in the lease queue, while it has one.</summary>
    internal TimeSpan? LeaseEntry;
    /// <summary>What the overview counts it as, under <see cref="CountedPriority"/>.</summary>
    internal ActionCounts CountedAs;
    internal ActionPriority CountedPriority;
    internal bool Removed;
    /// <summary>It has an entry in the queue of reservations coming due.</summary>
    internal bool ReservationQueued;
}

[Flags]
internal enum ActionCounts : byte { None = 0, Present = 1, Waiting = 2, Held = 4, Protected = 8 }

/// <summary>Actions of one type, priority and cost: whether one may start depends only on these, so the planner
/// decides it once per class and plan. It lists the tenants that wait with an action of it, by consumption.</summary>
internal sealed class ActionClass(ActionType type, ActionPriority priority, int cpu, int serial)
{
    internal static readonly Comparer<ActionClass> ByCost = Comparer<ActionClass>.Create((a, b) =>
    {
        var result = a.Cpu.CompareTo(b.Cpu);
        return result != 0 ? result : a.Serial.CompareTo(b.Serial);
    });

    internal ActionType Type { get; } = type;
    internal ActionPriority Priority { get; } = priority;
    internal int Cpu { get; } = cpu;
    internal int Serial { get; } = serial;
    internal string Group => Type.ConcurrencyGroup ?? Type.Name;
    /// <summary>Listed lanes of this class, by their tenants' serving order as last seen: a tenant's order only rises
    /// between the plan's resets, so a lane may sit too early but never too late, and is repaired when it comes
    /// first.</summary>
    internal readonly SortedSet<ActionLane> Tenants = new(ActionLane.BySnapshot);
    /// <summary>Its type is not suspended and its concurrency group has room.</summary>
    internal bool Open;
    /// <summary>The share it belongs to and its position there, by cost.</summary>
    internal ShareClasses Share = null!;
    internal int Position;
    /// <summary>In the planner's set of classes whose first action is not marked bypassed yet.</summary>
    internal bool Unmarked;
}

/// <summary>A tenant's waiting actions of one class, in start order; only its first one can start next.</summary>
internal sealed class ActionLane(TenantQueue tenant, ActionClass actionClass)
{
    /// <summary>Settable only so the scaling benchmark can count comparisons; sets take it when created.</summary>
    internal static Comparer<ActionLane> BySnapshot = Comparer<ActionLane>.Create((a, b) =>
    {
        var result = a.SnapServed.CompareTo(b.SnapServed);
        if (result == 0) result = a.SnapKey.CompareTo(b.SnapKey);
        return result != 0 ? result : a.Tenant.Id.CompareTo(b.Tenant.Id);
    });

    /// <summary>Its tenant's serving order when it was placed in its class's tenants.</summary>
    internal int SnapServed;
    /// <summary>In its tenant's list of lanes to reset when the plan ends.</summary>
    internal bool InRefreshed;
    internal double SnapKey;
    internal TenantQueue Tenant { get; } = tenant;
    internal ActionClass Class { get; } = actionClass;
    /// <summary>Waiting actions that entered in start order, which is nearly every one: actions queue in enqueue order,
    /// so they append and leave in constant time.</summary>
    internal readonly LinkedList<PlannedAction> InOrder = new();
    /// <summary>The few that entered before the last one in start order, such as an attempt queued again.</summary>
    internal readonly SortedSet<PlannedAction> Early = new(ActionPlanner.StartOrder);
    /// <summary>The first waiting action, the earlier of both lists' first.</summary>
    internal PlannedAction? Head;
    internal int Count => InOrder.Count + Early.Count;
    /// <summary>In its class's tenants: it waits and is not parked.</summary>
    internal bool Listed;
}

/// <summary>A tenant's consumption key and waiting lanes.</summary>
internal sealed class TenantQueue(ulong id)
{
    internal ulong Id { get; } = id;
    /// <summary>Actions started for it in the current plan, zero between plans: a plan serves tenants by this, then by
    /// consumption, which is one action per tenant per round with each round ordered by consumption.</summary>
    internal int Served;
    /// <summary>The decayed consumption divided by the decay since the planner's base second, so decay alone never
    /// changes the order of tenants; see <see cref="ActionPlanner"/>.</summary>
    internal double Key;
    internal int RunningCpu;
    internal int Actions;
    internal bool Suspended;
    internal readonly Dictionary<ActionClass, ActionLane> Lanes = new();
    /// <summary>Its lanes placed again during the current plan with a served count, to reset when it ends.</summary>
    internal readonly List<ActionLane> Refreshed = new();
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
    /// <summary>The time of its entry in the planner's expiry queue, while it has one.</summary>
    internal TimeSpan? ExpiryEntry;
    internal TimeSpan LastExchange { get; set; }
    internal int Used { get; set; }
    /// <summary>The bypassed action this worker waits for; it starts nothing else meanwhile.</summary>
    internal QualifiedActionId? ReservedFor { get; set; }
    internal readonly HashSet<QualifiedActionId> Actions = new();
    /// <summary>Its position in the round robin.</summary>
    internal int Index;
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
/// it already runs. An action that smaller ones of its priority or a less urgent one keep bypassing reserves a worker
/// after <see cref="ActionPlannerOptions.ReserveAfter"/>, so it cannot starve.
/// <para>
/// The work per change does not grow with the backlog, the tenants, the workers or the types; see "Coordinator cost" in
/// Doc/BackendActionsPlan.md. Waiting actions sit in lanes, one per tenant and class (type, priority and cost), whose
/// first action alone can start next. Each class lists its lanes by their tenant's serving order: actions started for
/// the tenant in the plan, then consumption, kept divided by the decay since a base second so that decay alone keeps
/// the order. That order only rises during a plan, so an entry may sit too early but never too late, and is repaired
/// only when its class is consulted. Per capacity share, a segment tree over the classes sorted by cost holds each
/// class's first entry; the classes whose cost fits the largest room are a prefix, so the next lane to start is one
/// prefix minimum. Another tree per share finds the round robin's next worker with room, and a Fenwick tree keeps the
/// round robin's places. Leases and worker timeouts expire from deadline queues, and the overview's counts are kept
/// as actions change.
/// </para>
/// </summary>
internal sealed class ActionPlanner
{
    /// <summary>Settable only so the scaling benchmark can count comparisons; sets take it when created.</summary>
    internal static Comparer<PlannedAction> StartOrder = Comparer<PlannedAction>.Create((a, b) =>
    {
        var result = a.Priority.CompareTo(b.Priority);
        if (result == 0) result = a.Order.CompareTo(b.Order);
        return result != 0 ? result : a.Id.ActionId.CompareTo(b.Id.ActionId);
    });

    readonly ActionPlannerOptions options;
    readonly ulong generation;
    readonly double _decay;
    /// <summary>Seconds after which consumption keys are rebased: an hour, or sooner when the decay's inverse would
    /// pass 1e100 earlier.</summary>
    readonly int _rebaseSeconds;
    readonly Dictionary<QualifiedActionId, PlannedAction> _actions = new();
    readonly Dictionary<ulong, TenantQueue> _tenants = new();
    /// <summary>Normalized keys of tenants that have no action, until their consumption decays away.</summary>
    readonly Dictionary<ulong, double> _idleKeys = new();
    readonly HashSet<TenantQueue> _runningTenants = new();
    readonly Dictionary<(ActionType Type, ActionPriority Priority, int Cpu), ActionClass> _classes = new();
    readonly Dictionary<Guid, PlannedWorker> _workers = new();
    readonly Dictionary<string, PlannedWorker> _byNode = new(StringComparer.Ordinal);
    /// <summary>The round robin: workers in the order they joined, a departed one leaving an empty slot until the slots
    /// are compacted. The live slots before a slot give its place in the round robin.</summary>
    readonly List<PlannedWorker?> _slots = new();
    Fenwick _live = new(0);
    int _liveWorkers;
    readonly Dictionary<string, int> _runningGroups = new(StringComparer.Ordinal);
    readonly Dictionary<(string Group, ulong Tenant), int> _runningTenantGroups = new();
    readonly DeadlineQueue<PlannedAction> _leases = new();
    readonly DeadlineQueue<PlannedWorker> _expiries = new();
    readonly HashSet<PlannedWorker> _reserved = new();
    readonly HashSet<PlannedAction> _protected = new();
    readonly HashSet<PlannedAction> _abandonable = new();
    readonly SortedDictionary<ActionPriority, int[]> _counts = new();
    /// <summary>Classes by capacity share, each with its segment trees; a priority's share decides room and cost.</summary>
    readonly List<ShareClasses> _shares = new();
    readonly Dictionary<ActionPriority, ShareClasses> _shareOf = new();
    readonly ShareClasses _defaultShare;
    /// <summary>Per group and concurrency limit, the classes that close when the group's count reaches it.</summary>
    readonly Dictionary<string, Dictionary<int, List<ActionClass>>> _limited = new(StringComparer.Ordinal);
    /// <summary>Per priority, open classes with a waiting first action not marked bypassed, by cost.</summary>
    readonly Dictionary<ActionPriority, SortedSet<ActionClass>> _unmarked = new();
    /// <summary>Bypassed first actions by the time they may reserve a worker.</summary>
    readonly DeadlineQueue<PlannedAction> _reservationsDue = new();
    /// <summary>The least used worker that may be reserved, over the round robin's positions.</summary>
    MinTree _leastUsed = new(0);
    int _classSerial;
    int _nextWorker, _readyWorkers;
    /// <summary>The current plan's number; never 0, the value that excludes no class.</summary>
    int _plan = 1;
    /// <summary>Elements examined: lane heads, tenants, queue entries and reported attempts. Tests check that it does
    /// not grow with the backlog.</summary>
    internal long Work;
    /// <summary>Operations on ordered sets and queues, each logarithmic in the set's size. Tests check that their number
    /// per step does not grow with the backlog either, so the only growth left is that logarithm.</summary>
    internal long OrderedOperations;
    TimeSpan _decayedUntil;
    bool _decayStarted;
    /// <summary>Whole seconds of decay since the base second of the consumption keys.</summary>
    int _sinceBase;
    IReadOnlySet<string> _suspended = new HashSet<string>(StringComparer.Ordinal);
    IReadOnlySet<ulong> _suspendedTenants = new HashSet<ulong>();
    /// <summary>The last plan found nothing to start and nothing it depends on changed since.</summary>
    bool _settled;

    internal ActionPlanner(ActionPlannerOptions options, ulong generation = 0)
    {
        this.options = options;
        this.generation = generation;
        _decay = options.ConsumptionDecay;
        if (!(_decay > 0 && _decay <= 1))
            throw new ArgumentOutOfRangeException(nameof(options), "The consumption decay must be above 0 and at most 1.");
        _rebaseSeconds = _decay == 1 ? int.MaxValue : (int)Math.Clamp(100 * Math.Log(10) / -Math.Log(_decay), 1, 3600);
        // The shares are read once: a share's classes and worker rooms are indexed under it.
        ShareClasses ShareFor(double share)
        {
            var existing = _shares.Find(s => s.Share == share);
            if (existing != null) return existing;
            _shares.Add(existing = new(share));
            return existing;
        }
        foreach (var (priority, share) in options.CapacityShares) _shareOf[priority] = ShareFor(share);
        _defaultShare = ShareFor(1.0);
    }

    internal IReadOnlyDictionary<QualifiedActionId, PlannedAction> Actions => _actions;
    internal IReadOnlyDictionary<Guid, PlannedWorker> Workers => _workers;
    /// <summary>Tenants with actions, for the scaling benchmark.</summary>
    internal int Tenants => _tenants.Count;
    /// <summary>Nothing changed since a plan found nothing to start, so planning again would start nothing.</summary>
    internal bool Settled => _settled;
    /// <summary>The worker holding the action, whose commands depend on it.</summary>
    internal Guid? Holder(QualifiedActionId id) => _actions.TryGetValue(id, out var action) ? action.Worker : null;
    /// <summary>Action types that start no new attempts.</summary>
    internal IReadOnlySet<string> Suspended
    {
        get => _suspended;
        set
        {
            _suspended = value;
            _settled = false;
            foreach (var actionClass in _classes.Values) Reopen(actionClass); // rare: an administrator's change
        }
    }

    internal IReadOnlySet<ulong> SuspendedTenants
    {
        get => _suspendedTenants;
        set
        {
            var previous = _suspendedTenants;
            _suspendedTenants = value;
            _settled = false;
            // Only the tenants whose suspension changed park or unpark their lanes.
            foreach (var id in previous.Where(t => !value.Contains(t)).Concat(value.Where(t => !previous.Contains(t))).ToList())
            {
                if (!_tenants.TryGetValue(id, out var tenant)) continue;
                tenant.Suspended = value.Contains(id);
                foreach (var lane in tenant.Lanes.Values.ToList()) Relist(lane);
            }
        }
    }

    /// <summary>The tenant's decayed consumption as of the last decay, for diagnostics and tests.</summary>
    internal double ConsumptionOf(ulong tenantId)
    {
        var key = _tenants.TryGetValue(tenantId, out var tenant) ? tenant.Key : _idleKeys.GetValueOrDefault(tenantId);
        return _decay == 1 ? key : key * Math.Pow(_decay, _sinceBase);
    }

    /// <summary>Per priority, in ascending order: the actions waiting to start, held by a worker, and protected.</summary>
    internal List<PriorityOverview> PriorityCounts() => _counts.Where(c => c.Value[0] != 0)
        .Select(c => new PriorityOverview(c.Key, c.Value[1], c.Value[2], c.Value[3])).ToList();

    /// <summary>Applies an action's committed row, or its removal when row is null. A started attempt this session did
    /// not start is protected for one lease, so a new coordinator does not start it twice while it may still run.</summary>
    internal void Update(QualifiedActionId id, QueuedAction? row, ActionType? type, TimeSpan now)
    {
        Work++;
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
            Tenant(id.TenantId).Actions++;
        }
        else
        {
            Leave(action); // its start order may change
            if (action.Starting is { } starting)
            {
                // The start event is applied: either it recorded this attempt or it rejected it.
                action.Starting = null;
                if (!row.IsStarted || row.RunAttempts != starting) Release(action);
                else if (action.Worker != null) Lease(action, action.LeaseUntil); // its lease may have passed meanwhile
                unknownAttempt = false;
            }
            else if (row.RunAttempts == action.RunAttempts) unknownAttempt = false;
            if (!row.IsStarted && action.Starting == null && action.Worker is { } holder)
            {
                // Its worker interrupted the attempt, which only a stopping worker does: give it nothing new.
                if (_workers.TryGetValue(holder, out var stopping)) SetReady(stopping, false);
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
            Lease(action, now + options.Lease);
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
        if (action.Protected && _byNode.ContainsKey(action.RecordedNode))
            action.Protected = false; // its node's worker already exchanged without it, or runs a newer process
        Count(action);
        Refresh(action);
    }

    /// <summary>Registers a worker's exchange and returns the commands it should execute now. Reported running
    /// attempts renew their lease or are adopted after a coordinator change; unknown or superseded ones are forgotten.
    /// Finished attempts stop being started again until their ActionFinished is applied here. Lost attempts free their
    /// worker, so the action gets a new attempt or, after its last one, finishes as Interrupted.</summary>
    internal List<WorkerCommand> Exchange(Guid session, string nodeId, int capacity, ulong workerGeneration, bool ready,
        IReadOnlyList<ReportedAttempt> running, IReadOnlyList<ReportedAttempt> finished, TimeSpan now,
        IReadOnlyList<ReportedAttempt>? lost = null)
    {
        if (_byNode.TryGetValue(nodeId, out var other) && other.Session != session)
            Expire(other); // the node restarted: its previous process's attempts are gone
        var commands = new List<WorkerCommand>();
        var reported = new HashSet<QualifiedActionId>();
        foreach (var attempt in running) reported.Add(attempt.Id);
        if (!_workers.TryGetValue(session, out var worker))
        {
            // A worker exchanges one request at a time and registers every Start it receives before it sends the next
            // request, so a report reaching this coordinator lists every attempt of its node that may still run.
            // A protected attempt it leaves out was recorded for an earlier process of the node, or recorded for this
            // one but never delivered, because the previous coordinator stopped first. Later protections of this
            // node end in Update, so only a new session needs this scan.
            foreach (var action in _protected.Where(a => (a.RecordedSession == session || a.RecordedNode == nodeId) &&
                                                         !reported.Contains(a.Id)).ToList())
            {
                action.Protected = false;
                Count(action);
                Refresh(action);
            }
            worker = new() { Session = session, NodeId = nodeId };
            _workers.Add(session, worker);
            _byNode[nodeId] = worker;
            worker.Index = _slots.Count;
            _slots.Add(worker);
            _liveWorkers++;
            if (_slots.Count > _live.Capacity) ReindexWorkers(); // grows by doubling: amortized constant
            else _live.Add(worker.Index, 1);
            _settled = false;
        }
        if (worker.Capacity != capacity || worker.Generation != workerGeneration || worker.Ready != ready)
        {
            _settled = false;
            worker.Capacity = capacity;
            worker.Generation = workerGeneration;
            SetReady(worker, ready);
            Touch(worker);
        }
        worker.ExpiresAt = now + options.WorkerTimeout;
        if (worker.ExpiryEntry is not { } queued || queued > worker.ExpiresAt)
        {
            worker.ExpiryEntry = worker.ExpiresAt;
            Queue(_expiries, worker, worker.ExpiresAt);
        }
        Work += 1 + running.Count + finished.Count + (lost?.Count ?? 0) + worker.Actions.Count;
        worker.LastExchange = now;
        foreach (var attempt in finished)
        {
            reported.Add(attempt.Id);
            if (!_actions.TryGetValue(attempt.Id, out var action) || action.RunAttempts != attempt.Attempt) continue;
            if (action.Worker == session) Release(action);
            action.Finished = true;
            Lease(action, now + options.Lease); // reruns if its ActionFinished never applies
            Refresh(action);
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
            Lease(action, now + options.Lease);
            var cpu = attempt.CpuDelta == 0 ? action.BaseCpu : Math.Max(10, action.BaseCpu + attempt.CpuDelta);
            if (cpu != action.HeldCpu)
            {
                worker.Used += cpu - action.HeldCpu;
                Touch(worker);
                AddRunningCpu(action.Id.TenantId, cpu - action.HeldCpu);
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
        // Workers expire from a queue by time; an exchange only moves ExpiresAt, and the entry then queues again.
        while (_expiries.TryPeek(out var worker, out var due) && due <= now)
        {
            if (_expiries.Dequeue()) OrderedOperations++;
            Work++;
            if (worker.ExpiryEntry != due) continue;
            worker.ExpiryEntry = null;
            if (!_workers.TryGetValue(worker.Session, out var current) || current != worker) continue;
            if (worker.ExpiresAt > now)
            {
                worker.ExpiryEntry = worker.ExpiresAt;
                Queue(_expiries, worker, worker.ExpiresAt);
            }
            else Expire(worker);
        }
        while (_leases.TryPeek(out var action, out var due) && due <= now)
        {
            if (_leases.Dequeue()) OrderedOperations++;
            Work++;
            if (action.LeaseEntry != due) continue; // replaced by an earlier entry
            action.LeaseEntry = null;
            if (action.Removed || !InFlight(action)) continue;
            if (action.LeaseUntil > now)
            {
                Lease(action, action.LeaseUntil); // renewed since: wait for the renewed lease
                continue;
            }
            if (action.Worker != null && action.Starting == null) Release(action);
            if (action.Protected || action.Finished)
            {
                action.Protected = false;
                action.Finished = false; // its ActionFinished never applied
                Count(action);
            }
            Refresh(action);
            if (action.Starting != null) continue; // its start event decides; Update queues its lease again
        }
        var abandoned = new List<Abandonment>();
        if (_abandonable.Count == 0) return abandoned;
        foreach (var action in _abandonable.OrderBy(a => a.Id.TenantId).ThenBy(a => a.Id.ActionId).ToList())
        {
            Work++;
            abandoned.Add(action.IsCancelled
                ? new(action.Id, action.RunAttempts, ActionOutcomeKind.Cancelled, "WorkerLost")
                : new(action.Id, action.RunAttempts, ActionOutcomeKind.Interrupted, "AttemptsExhausted"));
            action.Abandoned = true;
            Refresh(action);
        }
        return abandoned;
    }

    /// <summary>Assigns runnable actions to workers; the caller publishes one ActionsStarted with the result.</summary>
    internal List<Assignment> Plan(TimeSpan now)
    {
        Decay(now);
        var assignments = new List<Assignment>();
        if (_settled && now < NextReservation() || _readyWorkers == 0) return assignments;
        EndStaleReservations();
        DueReservations(now);
        _plan++;
        ActionPriority? leastUrgentStarted = null;
        var served = new List<TenantQueue>();
        // A reserved action has waited long enough: it starts as soon as its worker has room, before the rounds.
        foreach (var worker in _reserved.ToList())
        {
            Work++;
            if (_actions[worker.ReservedFor!.Value] is { Lane: { } lane } reserved && lane.Listed &&
                lane.Head == reserved && PickWorker(reserved) is { } picked)
            {
                Start(reserved, picked, now, assignments, ref leastUrgentStarted);
                Served(lane.Tenant, served);
            }
        }
        // The rounds: tenants by the actions started for them in this plan, then by consumption. The least entry over
        // the classes that may start is the next tenant and, of its lanes that may start, the one whose first action
        // comes first.
        while (Candidate(out var actionClass) is { } tenant)
        {
            Work++;
            var head = tenant.Lanes[actionClass!].Head!;
            // A class may start only while some worker has room for its cost, so the round robin finds one.
            var worker = PickWorker(head) ?? throw new InvalidOperationException("A class that fits found no worker.");
            Start(head, worker, now, assignments, ref leastUrgentStarted);
            Served(tenant, served);
        }
        // Between plans no tenant has been served: lanes placed with a served count go back to their consumption.
        foreach (var tenant in served)
        {
            tenant.Served = 0;
            foreach (var lane in tenant.Refreshed)
            {
                lane.InRefreshed = false;
                if (lane.Listed && lane.SnapServed != 0)
                {
                    Remove(lane.Class.Tenants, lane);
                    Snap(lane);
                    Add(lane.Class.Tenants, lane);
                    Place(lane.Class);
                }
            }
            tenant.Refreshed.Clear();
        }
        if (leastUrgentStarted is { } started) MarkBypassed(started, now);
        _settled = true; // a mutation, a new worker report or a due reservation makes the next plan look again
        return assignments;
    }

    void Served(TenantQueue tenant, List<TenantQueue> served)
    {
        if (tenant.Served == 0) served.Add(tenant);
        Move(tenant, 1, 0);
    }

    // The first tenant over all classes that may start: of each share, the classes whose cost fits the largest room
    // a worker has under it, and of those the one whose first tenant comes first.
    // Entries may be stale, but never too late: the least one, once repaired and still least, is the true first.
    TenantQueue? Candidate(out ActionClass? best)
    {
        while (true)
        {
            best = null;
            var at = CandidateKey.None;
            foreach (var share in _shares)
            {
                if (share.Dirty)
                {
                    share.Build();
                    foreach (var actionClass in share.ByCost) Place(actionClass);
                }
                OrderedOperations++;
                var (key, position) = share.First(share.Fitting(share.Rooms.Max));
                if (position >= 0 && key.CompareTo(at) < 0) (at, best) = (key, share.ByCost[position]);
            }
            if (best == null) return null;
            var first = Top(best)!;
            if (Order(first).CompareTo(at) == 0) return first.Tenant;
            Place(best); // it was stale: its entry rises, and the search looks again
        }
    }

    static CandidateKey Order(ActionLane lane) => new(lane.Tenant.Served, lane.Tenant.Key, lane.Tenant.Id,
        lane.Head!.Priority, lane.Head.Order, lane.Head.Id.ActionId);

    // A lane placed with a served count goes back to its consumption when the plan ends; listed once.
    static void Refreshed(ActionLane lane)
    {
        if (lane.SnapServed == 0 || lane.InRefreshed) return;
        lane.InRefreshed = true;
        lane.Tenant.Refreshed.Add(lane);
    }

    void Snap(ActionLane lane)
    {
        lane.SnapServed = lane.Tenant.Served;
        lane.SnapKey = lane.Tenant.Key;
    }

    // The class's first lane by its tenant's current order, repairing stale entries that came first.
    ActionLane? Top(ActionClass actionClass)
    {
        while (actionClass.Tenants.Count != 0)
        {
            OrderedOperations++;
            var first = actionClass.Tenants.Min!;
            if (first.SnapServed == first.Tenant.Served && first.SnapKey == first.Tenant.Key) return first;
            Work++;
            Remove(actionClass.Tenants, first);
            Snap(first);
            Add(actionClass.Tenants, first);
            Refreshed(first);
        }
        return null;
    }

    void Start(PlannedAction action, PlannedWorker worker, TimeSpan now, List<Assignment> assignments,
        ref ActionPriority? leastUrgentStarted)
    {
        if (leastUrgentStarted == null || action.Priority > leastUrgentStarted) leastUrgentStarted = action.Priority;
        var attempt = action.RunAttempts + 1;
        Acquire(action, worker, now);
        action.Starting = attempt;
        AddConsumption(Tenant(action.Id.TenantId), action.Cpu);
        assignments.Add(new(action.Id, attempt, worker.Session, worker.NodeId));
    }

    // Actions up to this priority started in this plan. Classes of the same or a more urgent priority that no worker
    // has room for were bypassed: their first action is marked, and may reserve a worker after ReserveAfter. A more
    // urgent action taking room a less urgent one needs is what the capacity shares intend. Each first action is
    // marked once, so this costs nothing for classes marked before.
    void MarkBypassed(ActionPriority priority, TimeSpan now)
    {
        if (options.ReserveAfter == TimeSpan.MaxValue) return;
        foreach (var (classPriority, unmarked) in _unmarked)
        {
            if (classPriority > priority || unmarked.Count == 0) continue;
            var share = ShareOf(classPriority);
            var room = share.Rooms.Max;
            if (room == double.PositiveInfinity) continue;
            OrderedOperations++;
            var blocked = new List<ActionClass>();
            foreach (var actionClass in unmarked.Reverse())
            {
                if (actionClass.Cpu <= room) break;
                blocked.Add(actionClass);
            }
            foreach (var actionClass in blocked)
            {
                Work++;
                var head = Top(actionClass)!.Head!;
                head.BypassedSince ??= now;
                Place(actionClass);
            }
        }
    }

    TimeSpan NextReservation() => _reservationsDue.TryPeek(out _, out var due) ? due : TimeSpan.MaxValue;

    // Bypassed first actions whose wait reached ReserveAfter reserve a worker, if still no worker has room for them.
    void DueReservations(TimeSpan now)
    {
        while (_reservationsDue.TryPeek(out var head, out var due) && due <= now)
        {
            if (_reservationsDue.Dequeue()) OrderedOperations++;
            Work++;
            head.ReservationQueued = false;
            if (head.Removed || head.Lane is not { Listed: true } lane || lane.Head != head || head.Reservation != null ||
                head.BypassedSince is not { } since || !lane.Class.Open)
                continue;
            if (Top(lane.Class) != lane)
            {
                Place(lane.Class); // another tenant comes first now: its first action may be marked instead
                continue;
            }
            if (now - since < options.ReserveAfter)
            {
                Place(lane.Class); // marked again since: queue it for its new time
                continue;
            }
            if (lane.Class.Cpu > lane.Class.Share.Rooms.Max) Reserve(head);
            Place(lane.Class);
        }
    }

    // The least used eligible worker that is not reserved yet, the first of equals in the round robin; none when
    // every one is.
    void Reserve(PlannedAction action)
    {
        OrderedOperations++;
        var (used, index) = _leastUsed.First();
        if (index < 0 || used == long.MaxValue) return;
        var best = _slots[index]!;
        best.ReservedFor = action.Id;
        _reserved.Add(best);
        action.Reservation = best.Session;
        Touch(best);
    }

    // A reservation ends when its action starts or leaves, or can no longer start: so a reserved worker never waits
    // for an action that waits for something else.
    void EndStaleReservations()
    {
        foreach (var worker in _reserved.ToList())
        {
            Work++;
            var id = worker.ReservedFor!.Value;
            if (_actions.TryGetValue(id, out var action) && action.Runnable && worker.Ready &&
                worker.Generation >= generation && !Suspended.Contains(action.Type.Name) &&
                !SuspendedTenants.Contains(id.TenantId) && GroupAdmits(action))
                continue;
            worker.ReservedFor = null;
            _reserved.Remove(worker);
            Touch(worker);
            if (action == null) continue;
            action.Reservation = null;
            if (action.Lane != null) Place(action.Lane.Class);
        }
    }

    void EndReservation(PlannedAction action)
    {
        if (action.Reservation is not { } session) return;
        action.Reservation = null;
        if (_workers.TryGetValue(session, out var worker) && worker.ReservedFor == action.Id)
        {
            worker.ReservedFor = null;
            _reserved.Remove(worker);
            Touch(worker);
        }
        if (action.Lane != null) Place(action.Lane.Class);
        _settled = false;
    }

    // Each whole second decays the consumption and adds the running CPU, which is constant since the last call. Keys
    // are the consumption divided by the decay since the base second, so a second changes only running tenants' keys.
    void Decay(TimeSpan now)
    {
        if (!_decayStarted)
        {
            // Not "_decayedUntil == default": the coordinator's clock starts at zero, which would restart the decay.
            _decayStarted = true;
            _decayedUntil = now;
        }
        var elapsed = (now - _decayedUntil).Ticks / TimeSpan.TicksPerSecond;
        if (elapsed <= 0) return;
        _decayedUntil += TimeSpan.FromSeconds(elapsed);
        if (_decay == 1)
        {
            foreach (var tenant in _runningTenants) AddKey(tenant, tenant.RunningCpu * (double)elapsed);
            Work += _runningTenants.Count;
            return;
        }
        if (_sinceBase + elapsed > _rebaseSeconds)
        {
            Rebase(elapsed);
            return;
        }
        // Running CPU r adds r / decay^s for each second s since the base.
        var growth = Math.Pow(_decay, -(_sinceBase + 1)) * (Math.Pow(_decay, -elapsed) - 1) / (1 / _decay - 1);
        _sinceBase += (int)elapsed;
        foreach (var tenant in _runningTenants) AddKey(tenant, tenant.RunningCpu * growth);
        Work += _runningTenants.Count;
    }

    // Moves the base to now, decaying every key by the elapsed seconds; consumption below 0.1 is forgotten.
    void Rebase(long elapsed)
    {
        var toNow = Math.Pow(_decay, _sinceBase); // from a key to the consumption at the previous second
        var factor = Math.Pow(_decay, elapsed);
        var sum = (1 - factor) / (1 - _decay);
        foreach (var actionClass in _classes.Values) actionClass.Tenants.Clear();
        foreach (var tenant in _tenants.Values)
        {
            var value = tenant.Key * toNow * factor + tenant.RunningCpu * sum;
            tenant.Key = value <= 0.1 ? 0 : value;
            foreach (var lane in tenant.Lanes.Values)
                if (lane.Listed)
                {
                    Snap(lane);
                    Add(lane.Class.Tenants, lane);
                }
        }
        foreach (var actionClass in _classes.Values) Place(actionClass);
        foreach (var (id, key) in _idleKeys.ToList())
        {
            var value = key * toNow * factor;
            if (value <= 0.1) _idleKeys.Remove(id);
            else _idleKeys[id] = value;
        }
        _sinceBase = 0;
    }

    void AddConsumption(TenantQueue tenant, int cpu) =>
        AddKey(tenant, _decay == 1 ? cpu : cpu * Math.Pow(_decay, -_sinceBase));

    void AddKey(TenantQueue tenant, double delta) => Move(tenant, 0, delta);

    // The tenant's serving order rises. Its entries in the classes it waits in stay where they are, too early, until a
    // class that is consulted finds them first and repairs them; classes nobody consults cost nothing.
    void Move(TenantQueue tenant, int served, double delta)
    {
        tenant.Served += served;
        tenant.Key += delta;
    }

    void AddRunningCpu(ulong tenantId, int delta)
    {
        if (delta == 0) return;
        var tenant = Tenant(tenantId);
        tenant.RunningCpu += delta;
        if (tenant.RunningCpu == 0) _runningTenants.Remove(tenant);
        else _runningTenants.Add(tenant);
    }

    TenantQueue Tenant(ulong id)
    {
        if (_tenants.TryGetValue(id, out var tenant)) return tenant;
        tenant = new(id) { Suspended = SuspendedTenants.Contains(id) };
        if (_idleKeys.Remove(id, out var key)) tenant.Key = key;
        _tenants.Add(id, tenant);
        return tenant;
    }

    // A tenant without actions keeps only its consumption, until a rebase finds it decayed away.
    void ForgetIfEmpty(TenantQueue tenant)
    {
        if (tenant.Actions != 0 || tenant.RunningCpu != 0) return;
        _tenants.Remove(tenant.Id);
        if (tenant.Key != 0) _idleKeys[tenant.Id] = tenant.Key;
    }

    bool GroupAdmits(PlannedAction action)
    {
        if (action.Type.MaxConcurrent > 0 && _runningGroups.GetValueOrDefault(action.Group) >= action.Type.MaxConcurrent)
            return false;
        return !action.Type.SingleRunningPerTenant ||
               _runningTenantGroups.GetValueOrDefault((action.Group, action.Id.TenantId)) == 0;
    }

    ShareClasses ShareOf(ActionPriority priority) => _shareOf.GetValueOrDefault(priority, _defaultShare);

    // The next worker in the round robin with room for the action: the first from the round robin's place whose room
    // under the action's share fits its cost, found in the share's tree, or the worker reserved for it when that comes
    // first. An idle worker takes any single action.
    PlannedWorker? PickWorker(PlannedAction action)
    {
        var count = _liveWorkers;
        if (count == 0) return null;
        var share = ShareOf(action.Priority);
        OrderedOperations++;
        var slot = share.Rooms.FirstAtLeast(_live.Select(_nextWorker), action.BaseCpu);
        var place = slot < 0 ? -1 : _live.Prefix(slot);
        if (action.Reservation is { } reserved && _workers.TryGetValue(reserved, out var own) && own.Ready &&
            own.Generation >= generation &&
            (own.Used == 0 || own.Used + action.BaseCpu <= Math.Max(100, own.Capacity * share.Share)))
        {
            var ownPlace = _live.Prefix(own.Index);
            if (place < 0 || (ownPlace - _nextWorker + count) % count < (place - _nextWorker + count) % count)
                (slot, place) = (own.Index, ownPlace);
        }
        if (slot < 0) return null;
        _nextWorker = (place + 1) % count;
        return _slots[slot];
    }

    // A worker's room under each share, and whether it may be reserved, changed.
    void Touch(PlannedWorker worker)
    {
        if (!_workers.TryGetValue(worker.Session, out var current) || current != worker) return;
        foreach (var share in _shares)
        {
            OrderedOperations++;
            share.Rooms.Set(worker.Index, Room(worker, share.Share));
        }
        OrderedOperations++;
        _leastUsed.Set(worker.Index, worker.Ready && worker.Generation >= generation && worker.ReservedFor == null
            ? ((long)worker.Used << 32) | (uint)worker.Index : long.MaxValue);
    }

    double Room(PlannedWorker worker, double share) =>
        !worker.Ready || worker.Generation < generation || worker.ReservedFor != null ? double.NegativeInfinity
        : worker.Used == 0 ? double.PositiveInfinity : Math.Max(100, worker.Capacity * share) - worker.Used;

    // Compacts the slots and sizes the trees for twice the live workers: O(W), after Θ(W) joins or departures, so
    // amortized constant per worker.
    void ReindexWorkers()
    {
        var live = _slots.Where(w => w != null).ToList();
        _slots.Clear();
        _slots.AddRange(live);
        var capacity = Math.Max(16, 2 * live.Count);
        _live = new Fenwick(capacity);
        foreach (var share in _shares) share.Rooms = new MaxTree(capacity);
        _leastUsed = new MinTree(capacity);
        for (var i = 0; i < live.Count; i++)
        {
            live[i]!.Index = i;
            _live.Add(i, 1);
            Touch(live[i]!);
        }
    }

    void Acquire(PlannedAction action, PlannedWorker worker, TimeSpan now)
    {
        EndReservation(action);
        action.BypassedSince = null;
        action.Worker = worker.Session;
        Lease(action, now + options.Lease);
        worker.Actions.Add(action.Id);
        action.Cpu = action.BaseCpu; // a new attempt starts without the previous one's adjustment
        action.HeldCpu = action.Cpu;
        worker.Used += action.HeldCpu;
        Touch(worker);
        AddRunningCpu(action.Id.TenantId, action.HeldCpu);
        Count(action);
        Refresh(action);
    }

    void Release(PlannedAction action)
    {
        if (action.Worker is not { } session) return;
        action.Worker = null;
        if (_workers.TryGetValue(session, out var worker) && worker.Actions.Remove(action.Id))
        {
            worker.Used -= action.HeldCpu;
            Touch(worker);
        }
        AddRunningCpu(action.Id.TenantId, -action.HeldCpu);
        Count(action);
        _settled = false;
        Refresh(action);
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
        var running = _runningGroups.GetValueOrDefault(action.Group);
        Add(_runningGroups, action.Group, delta);
        // Only the classes whose limit the count reaches or leaves open or close.
        if (_limited.TryGetValue(action.Group, out var byLimit) &&
            byLimit.TryGetValue(counted ? running + 1 : running, out var flipped))
            foreach (var actionClass in flipped) Reopen(actionClass);
        var key = (action.Group, action.Id.TenantId);
        var before = _runningTenantGroups.GetValueOrDefault(key);
        Add(_runningTenantGroups, key, delta);
        if (before == 0 || before + delta == 0) Repark(action.Id.TenantId, action.Group);
    }

    // The tenant starts or stops running an action of the group: its single-running lanes of it park or unpark.
    void Repark(ulong tenantId, string group)
    {
        if (!_tenants.TryGetValue(tenantId, out var tenant)) return;
        foreach (var lane in tenant.Lanes.Values) // Relist never adds or removes lanes
            if (lane.Class.Type.SingleRunningPerTenant && lane.Class.Group == group)
                Relist(lane);
    }

    static void Add<TKey>(Dictionary<TKey, int> counts, TKey key, int delta) where TKey : notnull
    {
        var value = counts.GetValueOrDefault(key) + delta;
        if (value == 0) counts.Remove(key);
        else counts[key] = value;
    }

    void SetReady(PlannedWorker worker, bool ready)
    {
        if (worker.Ready == ready) return;
        worker.Ready = ready;
        _readyWorkers += ready ? 1 : -1;
        Touch(worker);
    }

    void Expire(PlannedWorker worker)
    {
        var reservedFor = worker.ReservedFor;
        _reserved.Remove(worker);
        worker.ReservedFor = null;
        foreach (var id in worker.Actions.ToList()) Release(_actions[id]);
        _settled = false;
        SetReady(worker, false);
        _workers.Remove(worker.Session);
        if (_byNode.TryGetValue(worker.NodeId, out var current) && current == worker) _byNode.Remove(worker.NodeId);
        // Its slot empties; the places after it in the round robin move up by one, as in a list.
        _slots[worker.Index] = null;
        _live.Add(worker.Index, -1);
        foreach (var share in _shares) share.Rooms.Set(worker.Index, double.NegativeInfinity);
        _leastUsed.Set(worker.Index, long.MaxValue);
        _liveWorkers--;
        if (_nextWorker >= _liveWorkers) _nextWorker = 0;
        if (_slots.Count > 16 && _slots.Count > 2 * _liveWorkers) ReindexWorkers();
        if (reservedFor is { } reserved && _actions.TryGetValue(reserved, out var waiting))
        {
            waiting.Reservation = null;
            if (waiting.Lane != null) Place(waiting.Lane.Class);
        }
    }

    void Remove(PlannedAction action)
    {
        EndReservation(action);
        Release(action);
        action.Protected = false;
        Count(action);
        Leave(action);
        _protected.Remove(action);
        _abandonable.Remove(action);
        Track(action, ActionCounts.None);
        action.Removed = true;
        _actions.Remove(action.Id);
        var tenant = Tenant(action.Id.TenantId);
        tenant.Actions--;
        ForgetIfEmpty(tenant);
    }

    // A started attempt whose lease, protection or finished report the coordinator watches.
    static bool InFlight(PlannedAction action) => action.Worker != null || action.Protected || action.Finished;

    // Leases expire from a queue by time. A renewal only moves LeaseUntil; the entry, due earlier, then queues again.
    void Lease(PlannedAction action, TimeSpan until)
    {
        action.LeaseUntil = until;
        if (action.LeaseEntry is { } queued && queued <= until) return;
        action.LeaseEntry = until;
        Queue(_leases, action, until);
    }

    bool Exhausted(PlannedAction action) => action.RunAttempts - action.InterruptedAttempts >= options.MaxAttempts;

    // Brings the indexes in line with the action's state: its lane while it may start, the protected and abandonable
    // sets, and the overview's counts.
    void Refresh(PlannedAction action)
    {
        if (action.Removed) return;
        var exhausted = action.IsStarted && Exhausted(action);
        var waiting = action.Runnable && !exhausted;
        if (waiting != (action.Lane != null))
        {
            if (waiting) Enter(action);
            else Leave(action);
        }
        if (action.Protected) _protected.Add(action);
        else _protected.Remove(action);
        if (action.IsStarted && action.Idle && (action.IsCancelled || exhausted)) _abandonable.Add(action);
        else _abandonable.Remove(action);
        Track(action, ActionCounts.Present | (action.Runnable ? ActionCounts.Waiting : 0) |
                      (action.Worker != null ? ActionCounts.Held : 0) | (action.Protected ? ActionCounts.Protected : 0));
    }

    void Track(PlannedAction action, ActionCounts counts)
    {
        if (action.CountedAs == counts && (counts == ActionCounts.None || action.CountedPriority == action.Priority))
            return;
        if (action.CountedAs != ActionCounts.None) Tally(action.CountedPriority, action.CountedAs, -1);
        action.CountedAs = counts;
        action.CountedPriority = action.Priority;
        if (counts != ActionCounts.None) Tally(action.Priority, counts, 1);
    }

    void Tally(ActionPriority priority, ActionCounts counts, int delta)
    {
        if (!_counts.TryGetValue(priority, out var tally)) _counts.Add(priority, tally = new int[4]);
        tally[0] += delta;
        if ((counts & ActionCounts.Waiting) != 0) tally[1] += delta;
        if ((counts & ActionCounts.Held) != 0) tally[2] += delta;
        if ((counts & ActionCounts.Protected) != 0) tally[3] += delta;
        if (tally[0] == 0) _counts.Remove(priority);
    }

    void Enter(PlannedAction action)
    {
        var tenant = Tenant(action.Id.TenantId);
        var key = (action.Type, action.Priority, action.BaseCpu);
        if (!_classes.TryGetValue(key, out var actionClass)) actionClass = NewClass(key);
        if (!tenant.Lanes.TryGetValue(actionClass, out var lane)) tenant.Lanes.Add(actionClass, lane = new(tenant, actionClass));
        if (lane.InOrder.Last is not { } last || StartOrder.Compare(action, last.Value) > 0)
            action.LaneNode = lane.InOrder.AddLast(action);
        else Add(lane.Early, action);
        if (lane.Head == null || StartOrder.Compare(action, lane.Head) < 0) lane.Head = action;
        action.Lane = lane;
        Relist(lane);
    }

    void Leave(PlannedAction action)
    {
        if (action.Lane is not { } lane) return;
        if (action.LaneNode is { } node)
        {
            lane.InOrder.Remove(node);
            action.LaneNode = null;
        }
        else Remove(lane.Early, action);
        if (lane.Head == action)
        {
            var first = lane.InOrder.First?.Value;
            var early = lane.Early.Count == 0 ? null : lane.Early.Min;
            if (early != null) OrderedOperations++;
            lane.Head = early == null || first != null && StartOrder.Compare(first, early) < 0 ? first : early;
        }
        action.Lane = null;
        Relist(lane);
        if (lane.Count == 0) lane.Tenant.Lanes.Remove(lane.Class);
    }

    // A lane is listed while it waits and is not parked: in its class's tenants. Its first action may have changed
    // either way, so its class is placed again.
    void Relist(ActionLane lane)
    {
        var listed = lane.Count != 0 && !lane.Tenant.Suspended &&
                     !(lane.Class.Type.SingleRunningPerTenant &&
                       _runningTenantGroups.GetValueOrDefault((lane.Class.Group, lane.Tenant.Id)) != 0);
        if (listed != lane.Listed)
        {
            if (listed)
            {
                Snap(lane);
                Add(lane.Class.Tenants, lane);
                Refreshed(lane);
            }
            else Remove(lane.Class.Tenants, lane);
            lane.Listed = listed;
            _settled = false;
        }
        Place(lane.Class);
    }

    ActionClass NewClass((ActionType Type, ActionPriority Priority, int Cpu) key)
    {
        var actionClass = new ActionClass(key.Type, key.Priority, key.Cpu, ++_classSerial);
        _classes.Add(key, actionClass);
        actionClass.Share = ShareOf(key.Priority);
        actionClass.Share.Insert(actionClass); // rare: a new type, priority or cost; the share sorts before its search
        if (key.Type.MaxConcurrent > 0)
        {
            if (!_limited.TryGetValue(actionClass.Group, out var byLimit))
                _limited.Add(actionClass.Group, byLimit = new());
            if (!byLimit.TryGetValue(key.Type.MaxConcurrent, out var classes)) byLimit.Add(key.Type.MaxConcurrent, classes = new());
            classes.Add(actionClass);
        }
        Reopen(actionClass);
        return actionClass;
    }

    // Whether the class's type is suspended or its group full.
    void Reopen(ActionClass actionClass)
    {
        var open = !Suspended.Contains(actionClass.Type.Name) && (actionClass.Type.MaxConcurrent == 0 ||
                                                                  _runningGroups.GetValueOrDefault(actionClass.Group) <
                                                                  actionClass.Type.MaxConcurrent);
        if (open == actionClass.Open) return;
        actionClass.Open = open;
        _settled = false;
        Place(actionClass);
    }

    // Brings the class's entry in its share's tree, the unmarked set and the reservation queue in line with its first
    // tenant and that tenant's first action.
    void Place(ActionClass actionClass)
    {
        // A closed class is not consulted, so its stale entries wait; an open one shows its true first tenant.
        var first = !actionClass.Open ? null : Top(actionClass);
        OrderedOperations++;
        actionClass.Share.Set(actionClass.Position, first == null ? CandidateKey.None : Order(first));
        var head = first?.Head;
        var unmarked = head is { BypassedSince: null, Reservation: null };
        if (unmarked != actionClass.Unmarked)
        {
            if (!_unmarked.TryGetValue(actionClass.Priority, out var set))
                _unmarked.Add(actionClass.Priority, set = new(ActionClass.ByCost));
            if (unmarked) Add(set, actionClass);
            else Remove(set, actionClass);
            actionClass.Unmarked = unmarked;
        }
        if (head is { BypassedSince: { } since, Reservation: null, ReservationQueued: false } &&
            options.ReserveAfter <= TimeSpan.MaxValue - since)
        {
            head.ReservationQueued = true;
            Queue(_reservationsDue, head, since + options.ReserveAfter);
        }
    }

    bool Add<T>(SortedSet<T> set, T item)
    {
        OrderedOperations++;
        return set.Add(item);
    }

    bool Remove<T>(SortedSet<T> set, T item)
    {
        OrderedOperations++;
        return set.Remove(item);
    }

    void Queue<T>(DeadlineQueue<T> queue, T item, TimeSpan due)
    {
        if (queue.Enqueue(item, due)) OrderedOperations++;
    }

    void Push<TElement, TPriority>(PriorityQueue<TElement, TPriority> queue, TElement element, TPriority priority)
    {
        OrderedOperations++;
        queue.Enqueue(element, priority);
    }
}

/// <summary>
/// Items by due time. Deadlines are nearly always now plus a fixed timeout, so they arrive in order: those append to a
/// FIFO in constant time, and only one due before the last appended waits in a heap. The earlier of both fronts comes
/// first.
/// </summary>
internal sealed class DeadlineQueue<T>
{
    readonly Queue<(T Item, TimeSpan Due)> _inOrder = new();
    readonly PriorityQueue<T, TimeSpan> _early = new();
    TimeSpan _last = TimeSpan.MinValue;

    /// <summary>True when it went to the heap, a logarithmic operation.</summary>
    internal bool Enqueue(T item, TimeSpan due)
    {
        if (_inOrder.Count == 0 || due >= _last)
        {
            _inOrder.Enqueue((item, due));
            _last = due;
            return false;
        }
        _early.Enqueue(item, due);
        return true;
    }

    internal bool TryPeek(out T item, out TimeSpan due)
    {
        var inOrder = _inOrder.TryPeek(out var first);
        if (_early.TryPeek(out var early, out var earlyDue) && (!inOrder || earlyDue < first.Due))
        {
            (item, due) = (early, earlyDue);
            return true;
        }
        (item, due) = inOrder ? first : (default!, default);
        return inOrder;
    }

    /// <summary>Removes the front that <see cref="TryPeek"/> returned; true when it came from the heap.</summary>
    internal bool Dequeue()
    {
        var inOrder = _inOrder.TryPeek(out var first);
        if (_early.TryPeek(out _, out var earlyDue) && (!inOrder || earlyDue < first.Due))
        {
            _early.Dequeue();
            return true;
        }
        _inOrder.Dequeue();
        return false;
    }
}

/// <summary>A lane's place in a plan's serving order: its tenant's actions started in the plan, consumption and ID,
/// then its first action's priority, enqueue order and ID. The least over the classes that may start is the next
/// tenant and its earliest action of them.</summary>
internal readonly record struct CandidateKey(int Served, double Key, ulong Id, ActionPriority Priority, ulong Order,
    ulong ActionId) : IComparable<CandidateKey>
{
    internal static readonly CandidateKey None = new(int.MaxValue, double.PositiveInfinity, ulong.MaxValue,
        (ActionPriority)byte.MaxValue, ulong.MaxValue, ulong.MaxValue);

    public int CompareTo(CandidateKey other)
    {
        var result = Served.CompareTo(other.Served);
        if (result == 0) result = Key.CompareTo(other.Key);
        if (result == 0) result = Id.CompareTo(other.Id);
        if (result == 0) result = Priority.CompareTo(other.Priority);
        if (result == 0) result = Order.CompareTo(other.Order);
        return result != 0 ? result : ActionId.CompareTo(other.ActionId);
    }
}

/// <summary>
/// The classes of one capacity share sorted by cost, with a segment tree of their first tenants' places, and the
/// workers' rooms under the share. Those classes whose cost fits the largest room are a prefix, so the first tenant of
/// every class that may start is one prefix minimum: O(log C), however many classes are closed or too large.
/// </summary>
internal sealed class ShareClasses(double share)
{
    internal double Share { get; } = share;
    internal readonly List<ActionClass> ByCost = new();
    internal MaxTree Rooms = new(0);
    CandidateKey[] _keys = [CandidateKey.None];
    int[] _best = [-1, 0];
    int _size = 1;

    /// <summary>Classes were added since the tree was built; it is built again, once, before the next search.</summary>
    internal bool Dirty { get; private set; }

    /// <summary>Adds a class; the tree is rebuilt lazily, so adding many classes, as a coordinator's load does, costs
    /// one sort.</summary>
    internal void Insert(ActionClass actionClass)
    {
        ByCost.Add(actionClass);
        Dirty = true;
    }

    /// <summary>Sorts the classes by cost and rebuilds the tree empty; the caller places every class again.</summary>
    internal void Build()
    {
        Dirty = false;
        ByCost.Sort(ActionClass.ByCost);
        for (var i = 0; i < ByCost.Count; i++) ByCost[i].Position = i;
        _size = 1;
        while (_size < ByCost.Count) _size *= 2;
        _keys = new CandidateKey[_size];
        Array.Fill(_keys, CandidateKey.None);
        _best = new int[2 * _size];
        for (var i = 0; i < _size; i++) _best[_size + i] = i;
        for (var node = _size - 1; node >= 1; node--) _best[node] = Better(_best[2 * node], _best[2 * node + 1]);
    }

    int Better(int a, int b) => a < 0 ? b : b < 0 ? a : _keys[b].CompareTo(_keys[a]) < 0 ? b : a;

    internal void Set(int position, CandidateKey key)
    {
        if (Dirty || _keys[position] == key) return;
        _keys[position] = key;
        for (var node = (_size + position) / 2; node >= 1; node /= 2)
            _best[node] = Better(_best[2 * node], _best[2 * node + 1]);
    }

    /// <summary>The first place among the first <paramref name="count"/> classes and its position.</summary>
    internal (CandidateKey Key, int Position) First(int count)
    {
        var best = -1;
        for (int left = _size, right = _size + count; left < right; left /= 2, right /= 2)
        {
            if ((left & 1) == 1) best = Better(best, _best[left++]);
            if ((right & 1) == 1) best = Better(best, _best[--right]);
        }
        return best < 0 ? (CandidateKey.None, -1) : (_keys[best], best);
    }

    /// <summary>How many classes cost at most the room.</summary>
    internal int Fitting(double room)
    {
        int low = 0, high = ByCost.Count;
        while (low < high)
        {
            var middle = (low + high) / 2;
            if (ByCost[middle].Cpu <= room) low = middle + 1;
            else high = middle;
        }
        return low;
    }
}

/// <summary>A segment tree of the workers' rooms in round robin order: the largest room, and the first worker from a
/// position on, wrapping around, whose room is at least a cost, both in O(log W).</summary>
internal sealed class MaxTree
{
    readonly double[] _tree;
    readonly int _size;

    internal MaxTree(int count)
    {
        _size = 1;
        while (_size < count) _size *= 2;
        _tree = new double[2 * _size];
        Array.Fill(_tree, double.NegativeInfinity);
    }

    internal double Max => _tree[1];

    internal void Set(int index, double value)
    {
        var node = _size + index;
        if (_tree[node] == value) return;
        _tree[node] = value;
        for (node /= 2; node >= 1; node /= 2) _tree[node] = Math.Max(_tree[2 * node], _tree[2 * node + 1]);
    }

    internal int FirstAtLeast(int from, double least)
    {
        var index = First(1, 0, _size, from, least);
        if (index < 0 && from > 0) index = First(1, 0, _size, 0, least);
        return index;
    }

    int First(int node, int low, int high, int from, double least)
    {
        if (high <= from || _tree[node] < least) return -1;
        if (high - low == 1) return low;
        var middle = (low + high) / 2;
        var left = First(2 * node, low, middle, from, least);
        return left >= 0 ? left : First(2 * node + 1, middle, high, from, least);
    }
}

/// <summary>A segment tree of keys whose low 32 bits are the position: the least key and its position in O(1), set in
/// O(log W).</summary>
internal sealed class MinTree
{
    readonly long[] _tree;
    readonly int _size;

    internal MinTree(int count)
    {
        _size = 1;
        while (_size < count) _size *= 2;
        _tree = new long[2 * _size];
        Array.Fill(_tree, long.MaxValue);
    }

    internal void Set(int index, long value)
    {
        var node = _size + index;
        if (_tree[node] == value) return;
        _tree[node] = value;
        for (node /= 2; node >= 1; node /= 2) _tree[node] = Math.Min(_tree[2 * node], _tree[2 * node + 1]);
    }

    internal (long Key, int Index) First() =>
        _tree[1] == long.MaxValue ? (long.MaxValue, -1) : (_tree[1], (int)(uint)_tree[1]);
}

/// <summary>Counts of live slots: the live slots before a slot, and the slot of a given place, both in O(log W).</summary>
internal sealed class Fenwick(int capacity)
{
    readonly int[] _tree = new int[capacity + 1];

    internal int Capacity => capacity;

    internal void Add(int index, int delta)
    {
        for (var i = index + 1; i <= capacity; i += i & -i) _tree[i] += delta;
    }

    /// <summary>Live slots before the slot.</summary>
    internal int Prefix(int index)
    {
        var sum = 0;
        for (var i = index; i > 0; i -= i & -i) sum += _tree[i];
        return sum;
    }

    /// <summary>The slot at the place, counting live slots from zero.</summary>
    internal int Select(int place)
    {
        var index = 0;
        var step = 1;
        while (step * 2 <= capacity) step *= 2;
        for (; step > 0; step /= 2)
            if (index + step <= capacity && _tree[index + step] <= place)
            {
                index += step;
                place -= _tree[index];
            }
        return index;
    }
}
