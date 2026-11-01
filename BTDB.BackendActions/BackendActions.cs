using System.Diagnostics.CodeAnalysis;
using BTDB.ODBLayer;
using BTDB.Replication.EventSourcing;

namespace BTDB.BackendActions;

/// <summary>A registered action type: its limits and typed handler invokers.</summary>
internal sealed class ActionType
{
    internal required string Name { get; init; }
    internal required Type InputType { get; init; }
    internal required int Cpu { get; init; }
    internal required int MaxConcurrent { get; init; }
    internal required string? ConcurrencyGroup { get; init; }
    internal required bool SingleRunningPerTenant { get; init; }
    /// <summary>The priority of an enqueue that names none.</summary>
    internal required ActionPriority Priority { get; init; }
    internal required Func<IActionInput, IActionContext, Task<(ActionResultKind Kind, object? Result)>> Execute { get; init; }
    internal required Action<IActionInput, ActionOutcomeKind, ulong, object?, ActionError?> Finished { get; init; }
    internal required Action<IActionInput>? BeforeStart { get; init; }
    internal ActionScheduleAttribute? Schedule { get; init; }
    /// <summary>A new input for a scheduled start.</summary>
    internal Func<IActionInput>? Create { get; init; }
    internal IScheduledActionGate? Gate { get; init; }
}

/// <summary>
/// Action types of one application and the consume-side operations on their replicated state. Register every type,
/// then <see cref="RegisterEvents"/> on the application's dispatcher before the publisher is constructed, include
/// <see cref="RelationTypes"/> in the database's relation initialization and call <see cref="RegisterTypes"/> on it.
/// </summary>
public sealed partial class BackendActions
{
    /// <summary>Relations to add to the application's <c>InitializeRelations</c> list.</summary>
    public static IReadOnlyList<Type> RelationTypes { get; } =
        [typeof(IQueuedActions), typeof(IDelayedActions), typeof(IActionInputIndexes), typeof(IBackendActionsStates),
            typeof(IScheduledActionRuns)];

    internal readonly ActionChanges Changes = new();
    readonly Dictionary<Type, ActionType> _byInput = new();
    readonly Dictionary<string, ActionType> _byName = new(StringComparer.Ordinal);
    bool _frozen;

    /// <summary>Called inside the consume when a handler's <c>OnFinished</c> throws an exception that is not fatal;
    /// the outcome still counts and the handler's writes before the exception stay. Without it the exception is
    /// ignored, so one failing handler does not stop the action from finishing or block the coordinator. Like any
    /// consume it runs on every replica.</summary>
    public Action<QualifiedActionId, ActionOutcomeKind, Exception>? OnFinishedFailed { get; set; }

    /// <summary>Registers an action type and its handler. The type's metadata comes from the source generator when
    /// it generated the handler's input, otherwise from the input's attributes through reflection, which trimming
    /// supports: the annotation keeps the input's parameterless constructor for scheduled starts.</summary>
    public BackendActions Register<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
        TInput, TResult>(IActionHandler<TInput, TResult> handler)
        where TInput : ActionInput<TResult> where TResult : class =>
        Register(handler, GeneratedActions.Metadata<TInput, TResult>() ?? ActionMetadata.Reflect<TInput>());

    /// <summary>Registers every handler the source generator found in the loaded assemblies, or those whose type
    /// <paramref name="include"/> accepts. Handlers come from <paramref name="handlers"/>, for example an IOC container's
    /// resolve; without it each handler needs a parameterless constructor.</summary>
    public BackendActions RegisterGenerated(Func<Type, object>? handlers = null, Func<Type, bool>? include = null)
    {
        foreach (var action in GeneratedActions.All)
            if (include?.Invoke(action.HandlerType) ?? true) action.RegisterTo(this, handlers);
        return this;
    }

    internal BackendActions Register<TInput, TResult>(IActionHandler<TInput, TResult> handler, ActionMetadata metadata)
        where TInput : ActionInput<TResult> where TResult : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (_frozen) throw new InvalidOperationException("Register action types before registering events.");
        if (metadata.Cpu < 0 || metadata.MaxConcurrent < 0 || !Enum.IsDefined(metadata.Priority))
            throw new ArgumentException($"Invalid limits of action {typeof(TInput).FullName}.");
        if (metadata.Schedule != null && metadata.Create == null)
            throw new ArgumentException($"Scheduled action {typeof(TInput).FullName} needs a parameterless constructor.");
        var before = handler as IActionWithBeforeStart<TInput>;
        var type = new ActionType
        {
            Name = metadata.Name ?? typeof(TInput).FullName!,
            InputType = typeof(TInput),
            Cpu = metadata.Cpu,
            MaxConcurrent = metadata.MaxConcurrent,
            ConcurrencyGroup = metadata.ConcurrencyGroup,
            SingleRunningPerTenant = metadata.SingleRunningPerTenant,
            Priority = metadata.Priority,
            Execute = async (input, context) =>
            {
                var result = await handler.ExecuteAsync((TInput)input, context).ConfigureAwait(false);
                return (result.Kind, result.Result);
            },
            Finished = (input, kind, attempt, result, error) =>
                handler.OnFinished((TInput)input, new(kind, attempt, (TResult?)result, error)),
            BeforeStart = before == null ? null : input => before.OnBeforeStart((TInput)input),
            Schedule = metadata.Schedule,
            Create = metadata.Schedule == null ? null : metadata.Create,
            Gate = handler as IScheduledActionGate
        };
        if (_byName.ContainsKey(type.Name)) throw new ArgumentException($"Action name {type.Name} is already registered.");
        _byInput.Add(typeof(TInput), type);
        _byName.Add(type.Name, type);
        return this;
    }

    /// <summary>Registers the library's events on the application's dispatcher; no action type can be added after.</summary>
    public void RegisterEvents(EventDispatcher dispatcher)
    {
        ArgumentNullException.ThrowIfNull(dispatcher);
        _frozen = true;
        var handler = new BackendActionsEventHandler(this);
        dispatcher.Register<ActionEnqueued, ActionEnqueuedResult>(handler)
            .Register<ActionCancelRequested, ActionCancelResult>(handler)
            .Register<CoordinatorActivated>(handler)
            .Register<ActionsStarted>(handler)
            .Register<ActionFinished>(handler)
            .Register<ActionInterrupted>(handler)
            .Register<DelayedActionsDue>(handler)
            .Register<ScheduledActionStarted>(handler)
            .Register<ActionsSuspended>(handler)
            .Register<ActionsImported>(handler);
    }

    /// <summary>Registers every input type for polymorphic storage in the database's action rows.</summary>
    public void RegisterTypes(IObjectDB database)
    {
        ArgumentNullException.ThrowIfNull(database);
        foreach (var type in _byInput.Keys) database.RegisterType(type);
    }

    internal IReadOnlyCollection<ActionType> Types => _byName.Values;

    internal ActionType TypeOf(IActionInput input) => _byInput.TryGetValue(input.GetType(), out var type) ? type
        : throw new InvalidOperationException($"Action input {input.GetType().FullName} is not registered.");

    internal ActionType? TypeOf(string name) => _byName.GetValueOrDefault(name);

    /// <summary>Enqueues as <see cref="Enqueue"/> does and returns the action it queued into. Inside a query scope it
    /// publishes <see cref="ActionEnqueued"/> with the scope's publisher and waits for its consume; inside a consume it
    /// queues in the consume's transaction.</summary>
    public async Task<QualifiedActionId> EnqueueAsync(IActionInput input, ActionPriority? priority = null,
        DateTime? startAt = null, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(input);
        if (!Ambient.IsQuery) return Enqueue(input, priority, startAt);
        // The event carries the resolved priority, so every replica queues with the same one.
        var result = await Ambient.PublishAndWaitEventAsync(new ActionEnqueued
        {
            Input = input, Priority = priority ?? TypeOf(input).Priority, StartAt = startAt
        }, cancellation).ConfigureAwait(false);
        return new(result.TenantId, result.ActionId);
    }

    /// <summary>Cancels as <see cref="Cancel"/> does: inside a query scope by publishing
    /// <see cref="ActionCancelRequested"/> with the scope's publisher, inside a consume in its transaction. False when
    /// no queued or delayed action has the ID.</summary>
    public async Task<bool> CancelAsync(QualifiedActionId id, CancellationToken cancellation = default)
    {
        if (!Ambient.IsQuery) return Cancel(id);
        return (await Ambient.PublishAndWaitEventAsync(new ActionCancelRequested
        {
            TenantId = id.TenantId, ActionId = id.ActionId
        }, cancellation).ConfigureAwait(false)).Found;
    }

    /// <summary>Queues an action in the current consume, in its transaction, and returns its ID: the ID of the queued
    /// action the input merged into, or a new one allocated deterministically when the input's ActionId is zero.
    /// Without <paramref name="priority"/> it uses the type's <see cref="BackendActionAttribute.Priority"/>; a
    /// <paramref name="startAt"/> after the enqueuing event's time keeps the action delayed until then. A merged input
    /// changes neither of the queued action's.</summary>
    public QualifiedActionId Enqueue(IActionInput input, ActionPriority? priority = null, DateTime? startAt = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        var ev = CurrentConsume();
        var type = TypeOf(input);
        var resolved = priority ?? type.Priority;
        if (startAt > ev.When)
        {
            if (input.ActionId == 0) input.ActionId = Ambient.AllocateId();
            Ambient.Relation<IDelayedActions>().Insert(new()
            {
                TenantId = input.TenantId, ActionId = input.ActionId, StartAt = startAt.Value, Name = type.Name,
                Priority = resolved, Input = input, QueuedAt = ev.When
            });
            return new(input.TenantId, input.ActionId);
        }
        return Queue(type.Name, input.TenantId, input.ActionId, input, resolved, ev);
    }

    /// <summary>Cancels an action in the current consume. A delayed action is removed, and so is a queued one that
    /// has not started, after its handler's <see cref="ActionOutcomeKind.Cancelled"/> outcome. A started action is
    /// marked cancelled and finishes through its worker or the coordinator. False when no such action exists.</summary>
    public bool Cancel(QualifiedActionId id)
    {
        CurrentConsume();
        if (Ambient.Relation<IDelayedActions>().RemoveById(id.TenantId, id.ActionId)) return true;
        var queued = Ambient.Relation<IQueuedActions>();
        if (queued.FindByIdOrDefault(id.TenantId, id.ActionId) is not { } row) return false;
        if (row.IsStarted)
        {
            if (!row.IsCancelled)
            {
                row.IsCancelled = true;
                queued.Update(row);
                Changed(id);
            }
            return true;
        }
        Complete(row, ActionOutcomeKind.Cancelled, row.RunAttempts, null, null);
        return true;
    }

    /// <summary>Suspends action types as <see cref="Suspend(IEnumerable{string}, bool)"/> does: inside a query scope
    /// by publishing <see cref="ActionsSuspended"/> with the scope's publisher, inside a consume in its
    /// transaction.</summary>
    public Task SuspendAsync(IEnumerable<string> names, bool suspended = true, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(names);
        return SuspendAsync(names.ToList(), [], suspended, cancellation);
    }

    /// <summary>Suspends tenants as <see cref="SuspendTenants"/> does: inside a query scope by publishing
    /// <see cref="ActionsSuspended"/> with the scope's publisher, inside a consume in its transaction.</summary>
    public Task SuspendTenantsAsync(IEnumerable<ulong> tenantIds, bool suspended = true,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(tenantIds);
        return SuspendAsync([], tenantIds.ToList(), suspended, cancellation);
    }

    async Task SuspendAsync(List<string> names, List<ulong> tenantIds, bool suspended, CancellationToken cancellation)
    {
        if (!Ambient.IsQuery)
        {
            CurrentConsume();
            Suspend(names, tenantIds, suspended);
            return;
        }
        await Ambient.PublishEventAsync(new ActionsSuspended
        {
            Names = names, TenantIds = tenantIds, Suspended = suspended
        }, cancellation).ConfigureAwait(false);
    }

    /// <summary>Suspends or resumes action types in the current consume: suspended types start no new attempts,
    /// running ones go on, and their inputs keep queueing.</summary>
    public void Suspend(IEnumerable<string> names, bool suspended = true)
    {
        ArgumentNullException.ThrowIfNull(names);
        CurrentConsume();
        Suspend(names.ToList(), [], suspended);
    }

    /// <summary>Suspends or resumes tenants in the current consume, for example when an application event deactivates
    /// a tenant: their actions start no new attempts, running ones go on, and their inputs keep queueing. Tenant 0
    /// holds the actions without a tenant, scheduled ones included.</summary>
    public void SuspendTenants(IEnumerable<ulong> tenantIds, bool suspended = true)
    {
        ArgumentNullException.ThrowIfNull(tenantIds);
        CurrentConsume();
        Suspend([], tenantIds.ToList(), suspended);
    }

    /// <summary>The suspended action types, from the ambient query or consume snapshot.</summary>
    public IReadOnlyList<string> SuspendedActions =>
        Ambient.Relation<IBackendActionsStates>().FindByIdOrDefault(0)?.Suspended ?? [];

    /// <summary>The suspended tenants in ascending order, from the ambient query or consume snapshot.</summary>
    public IReadOnlyList<ulong> SuspendedTenants =>
        Ambient.Relation<IBackendActionsStates>().FindByIdOrDefault(0)?.SuspendedTenants ?? [];

    public bool IsTenantSuspended(ulong tenantId) =>
        Ambient.Relation<IBackendActionsStates>().FindByIdOrDefault(0)?.SuspendedTenants.BinarySearch(tenantId) >= 0;

    internal void Suspend(List<string> names, List<ulong> tenantIds, bool suspended)
    {
        if (names.Count == 0 && tenantIds.Count == 0) return;
        var state = State();
        state.Suspended = Toggle(state.Suspended, names, suspended, StringComparer.Ordinal);
        state.SuspendedTenants = Toggle(state.SuspendedTenants, tenantIds, suspended, Comparer<ulong>.Default);
        Ambient.Relation<IBackendActionsStates>().Update(state);
        Changed(ActionChanges.Settings);
    }

    static List<T> Toggle<T>(List<T> current, List<T> changes, bool add, IComparer<T> comparer)
    {
        if (changes.Count == 0) return current;
        var set = new SortedSet<T>(current, comparer);
        foreach (var item in changes)
            if (add) set.Add(item);
            else set.Remove(item);
        return set.ToList();
    }

    internal void ActivateCoordinator(Guid session)
    {
        var state = State();
        state.CoordinatorSession = session;
        Ambient.Relation<IBackendActionsStates>().Update(state);
        Changed(ActionChanges.Settings); // a running coordinator of another session learns it was replaced
    }

    internal void Start(ActionsStarted ev)
    {
        if (ev.Session == Guid.Empty || ev.Session != State().CoordinatorSession) return;
        var queued = Ambient.Relation<IQueuedActions>();
        foreach (var started in ev.Actions)
        {
            // Also a rejected start: the coordinator then releases the attempt's worker.
            Changed(new(started.TenantId, started.ActionId));
            if (queued.FindByIdOrDefault(started.TenantId, started.ActionId) is not { } row || row.IsCancelled ||
                started.Attempt != row.RunAttempts + 1)
                continue;
            if (!row.IsStarted) RemoveIndex(row);
            row.IsStarted = true;
            row.IsAggregable = false;
            row.RunAttempts = started.Attempt;
            row.WorkerSession = started.WorkerSession;
            row.WorkerNode = started.WorkerNode;
            row.StartEventId = ev.EventId;
            if (TypeOf(row.Name)?.BeforeStart is { } before)
            {
                try
                {
                    before((IActionInput)row.Input!);
                }
                catch (Exception error) when (!EventPublisherOptions.DefaultIsFatalConsumeException(error))
                {
                    Complete(row, ActionOutcomeKind.Failed, row.RunAttempts, null,
                        new() { Code = "BeforeStartFailed", Message = error.Message, Details = error.ToString() });
                    continue;
                }
            }
            queued.Update(row);
        }
    }

    // Rejects an unknown or non-final outcome at publication; the consume checks again for events in the log already.
    internal static void CheckKind(ActionFinished ev)
    {
        if (!Enum.IsDefined(ev.Kind) || ev.Kind == ActionOutcomeKind.Progress)
            throw new ArgumentException("Unknown action outcome.");
    }

    internal void Finish(ActionFinished ev)
    {
        CheckKind(ev);
        var queued = Ambient.Relation<IQueuedActions>();
        if (queued.FindByIdOrDefault(ev.TenantId, ev.ActionId) is not { IsStarted: true } row ||
            row.RunAttempts != ev.Attempt)
            return; // a superseded attempt or an action that already finished
        Complete(row, ev.Kind, ev.Attempt, ev.Result, ev.Error);
    }

    internal void Interrupt(ActionInterrupted ev)
    {
        var queued = Ambient.Relation<IQueuedActions>();
        if (queued.FindByIdOrDefault(ev.TenantId, ev.ActionId) is not { IsStarted: true } row ||
            row.RunAttempts != ev.Attempt)
            return; // a superseded attempt, a repeated report or an action that already finished
        if (ev.Progress != null)
        {
            OnFinished(row, ActionOutcomeKind.Progress, ev.Attempt, ev.Progress, null);
            if (queued.FindByIdOrDefault(ev.TenantId, ev.ActionId) is not { IsStarted: true } current ||
                current.RunAttempts != ev.Attempt)
                return; // OnFinished finished the action itself
            row = current;
        }
        if (ev.CountsAsAttempt) return; // the worker reports the attempt lost
        if (row.IsCancelled)
        {
            Complete(row, ActionOutcomeKind.Cancelled, row.RunAttempts, null, null);
            return;
        }
        row.IsStarted = false;
        row.InterruptedAttempts++;
        queued.Update(row);
        Changed(new(row.TenantId, row.ActionId));
    }

    internal void QueueDue(DelayedActionsDue ev)
    {
        var delayed = Ambient.Relation<IDelayedActions>();
        foreach (var key in ev.Actions)
        {
            if (delayed.FindByIdOrDefault(key.TenantId, key.ActionId) is not { } row || row.StartAt > ev.When) continue;
            delayed.RemoveById(row.TenantId, row.ActionId);
            // By its stored name, so an unregistered type queues like any row and waits for its type.
            Queue(row.Name, row.TenantId, row.ActionId, row.Input, row.Priority, ev);
        }
    }

    internal void StartScheduled(ScheduledActionStarted ev)
    {
        var runs = Ambient.Relation<IScheduledActionRuns>();
        var run = runs.FindByIdOrDefault(ev.Name);
        if ((run?.LastRun ?? default) != ev.ExpectedLastRun) return; // another start already moved it
        runs.Upsert(new() { Name = ev.Name, LastRun = ev.NewLastRun });
        if (ev.Start && TypeOf(ev.Name) is { Create: { } create } type)
        {
            var input = create();
            Queue(type.Name, input.TenantId, input.ActionId, input, type.Priority, ev);
        }
    }

    /// <summary>Queues an input, or merges it into the queued action with its key. An actionId of zero allocates one
    /// only when a new action is queued, and stores it in the input.</summary>
    QualifiedActionId Queue(string name, ulong tenantId, ulong actionId, object? input, ActionPriority priority, Event ev)
    {
        var queued = Ambient.Relation<IQueuedActions>();
        if (input is IActionInputUnificator unificator && unificator.GetUniqueKey() is { } key)
        {
            var indexes = Ambient.Relation<IActionInputIndexes>();
            if (indexes.FindByIdOrDefault(tenantId, name, key) is { } index)
            {
                var existing = queued.FindByIdOrDefault(tenantId, index.ActionId) ?? throw new InvalidOperationException(
                    $"Action index {name}/{Convert.ToHexString(key)} has no queued action.");
                if (input is not IActionInputAggregator aggregator ||
                    existing.IsAggregable && ((IActionInputAggregator)existing.Input!).TryAggregate(aggregator))
                {
                    existing.LatestInputEventId = ev.EventId;
                    queued.Update(existing);
                    if (actionId == 0) unificator.ActionId = existing.ActionId;
                    return new(existing.TenantId, existing.ActionId); // its scheduling does not change
                }
                existing.IsAggregable = false;
                queued.Update(existing);
                index.ActionId = actionId = Allocate(actionId, input);
                indexes.Update(index);
            }
            else
                indexes.Insert(new()
                {
                    TenantId = tenantId, Name = name, UniqueKey = key, ActionId = actionId = Allocate(actionId, input)
                });
        }
        actionId = Allocate(actionId, input);
        queued.Insert(new()
        {
            TenantId = tenantId, ActionId = actionId, Name = name, Priority = priority,
            Order = queued.AllocateId(), Input = input, IsAggregable = input is IActionInputAggregator,
            LatestInputEventId = ev.EventId, QueuedAt = ev.When
        });
        Changed(new(tenantId, actionId));
        return new(tenantId, actionId);
    }

    // Removes the row after its handler's final outcome; an unregistered type finishes without a callback.
    // The index goes before the callback, so an input it enqueues does not merge into the finishing action; the row
    // goes after it, so the callback still reads the input's lazy collections.
    void Complete(QueuedAction row, ActionOutcomeKind kind, ulong attempt, object? result, ActionError? error)
    {
        RemoveIndex(row);
        OnFinished(row, kind, attempt, result, error);
        Ambient.Relation<IQueuedActions>().RemoveById(row.TenantId, row.ActionId);
        Changed(new(row.TenantId, row.ActionId));
    }

    static ulong Allocate(ulong actionId, object? input)
    {
        if (actionId != 0) return actionId;
        actionId = Ambient.AllocateId();
        ((IActionInput)input!).ActionId = actionId;
        return actionId;
    }

    void OnFinished(QueuedAction row, ActionOutcomeKind kind, ulong attempt, object? result, ActionError? error)
    {
        if (TypeOf(row.Name) is not { } type) return;
        try
        {
            type.Finished((IActionInput)row.Input!, kind, attempt, result, error);
        }
        catch (Exception exception) when (!EventPublisherOptions.DefaultIsFatalConsumeException(exception))
        {
            // Rethrowing would roll the outcome back, and the coordinator would publish it again forever.
            OnFinishedFailed?.Invoke(new(row.TenantId, row.ActionId), kind, exception);
        }
    }

    // Tells a coordinator running on this node, once the batch is visible; replay registers nothing.
    void Changed(QualifiedActionId id)
    {
        if (Changes.Active) Ambient.RegisterPostCommit(() => Changes.Add(id));
    }

    static void RemoveIndex(QueuedAction row)
    {
        if (row.Input is not IActionInputUnificator unificator || unificator.GetUniqueKey() is not { } key) return;
        var indexes = Ambient.Relation<IActionInputIndexes>();
        if (indexes.FindByIdOrDefault(row.TenantId, row.Name, key) is { } index && index.ActionId == row.ActionId)
            indexes.RemoveById(row.TenantId, row.Name, key);
    }

    static BackendActionsState State()
    {
        var states = Ambient.Relation<IBackendActionsStates>();
        if (states.FindByIdOrDefault(0) is { } state) return state;
        state = new();
        states.Insert(state);
        return state;
    }

    static Event CurrentConsume() => !Ambient.IsQuery && Ambient.CurrentEvent is { } ev ? ev
        : throw new InvalidOperationException("This runs only inside a consume; use its Async variant in a query scope.");
}

/// <summary>Actions whose rows changed in committed batches, for the coordinator running on this node.</summary>
internal sealed class ActionChanges
{
    /// <summary>Stands for a change of the cluster-wide state rather than of an action.</summary>
    internal static readonly QualifiedActionId Settings = new(ulong.MaxValue, ulong.MaxValue);

    readonly object _lock = new();
    HashSet<QualifiedActionId> _changed = new();
    TaskCompletionSource _signal = new(TaskCreationOptions.RunContinuationsAsynchronously);
    int _subscribers;

    internal bool Active => Volatile.Read(ref _subscribers) != 0;

    internal IDisposable Subscribe()
    {
        Interlocked.Increment(ref _subscribers);
        return new Subscription(this);
    }

    internal void Add(QualifiedActionId id)
    {
        TaskCompletionSource signal;
        lock (_lock)
        {
            _changed.Add(id);
            signal = _signal;
        }
        signal.TrySetResult();
    }

    /// <summary>A task completing at the next change; take it before draining so no change is missed.</summary>
    internal Task Next
    {
        get { lock (_lock) return _signal.Task; }
    }

    internal HashSet<QualifiedActionId> Drain()
    {
        lock (_lock)
        {
            var result = _changed;
            _changed = new();
            if (_signal.Task.IsCompleted) _signal = new(TaskCreationOptions.RunContinuationsAsynchronously);
            return result;
        }
    }

    sealed class Subscription(ActionChanges owner) : IDisposable
    {
        int _disposed;
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0) Interlocked.Decrement(ref owner._subscribers);
        }
    }
}
