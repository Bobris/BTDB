using BTDB.ODBLayer;
using BTDB.Replication.EventSourcing;

namespace BTDB.BackendActions;

/// <summary>A registered action type: its limits and typed handler invokers.</summary>
internal sealed class ActionType
{
    internal required string Name { get; init; }
    internal required Type InputType { get; init; }
    internal required Type ResultType { get; init; }
    internal required int Cpu { get; init; }
    internal required int MaxConcurrent { get; init; }
    internal required string? ConcurrencyGroup { get; init; }
    internal required bool SingleRunningPerTenant { get; init; }
    internal required Func<IActionInput, IActionContext, Task<object>> Execute { get; init; }
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

    /// <summary>Registers an action type and its handler. The type's metadata comes from the source generator when
    /// it generated the handler's input, otherwise from the input's attributes through reflection.</summary>
    public BackendActions Register<TInput, TResult>(IActionHandler<TInput, TResult> handler)
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
        if (metadata.Cpu < 0 || metadata.MaxConcurrent < 0)
            throw new ArgumentException($"Invalid limits of action {typeof(TInput).FullName}.");
        if (metadata.Schedule != null && metadata.Create == null)
            throw new ArgumentException($"Scheduled action {typeof(TInput).FullName} needs a parameterless constructor.");
        var before = handler as IActionWithBeforeStart<TInput>;
        var type = new ActionType
        {
            Name = metadata.Name ?? typeof(TInput).FullName!,
            InputType = typeof(TInput),
            ResultType = typeof(TResult),
            Cpu = metadata.Cpu,
            MaxConcurrent = metadata.MaxConcurrent,
            ConcurrencyGroup = metadata.ConcurrencyGroup,
            SingleRunningPerTenant = metadata.SingleRunningPerTenant,
            Execute = async (input, context) => await handler.ExecuteAsync((TInput)input, context).ConfigureAwait(false),
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

    /// <summary>Publishes <see cref="ActionEnqueued"/> from outside a consume and returns the action it queued into.</summary>
    public async Task<QualifiedActionId> EnqueueAsync(IEventPublisher publisher, IActionInput input,
        ActionEnqueueOptions? options = null, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(input);
        options ??= new();
        var result = await publisher.PublishAndWaitAsync(new ActionEnqueued
        {
            Input = input, Priority = options.Priority, StartAt = options.StartAt
        }, cancellation).ConfigureAwait(false);
        return new(result.TenantId, result.ActionId);
    }

    /// <summary>Publishes <see cref="ActionCancelRequested"/>; false when no queued or delayed action has the ID.</summary>
    public async Task<bool> CancelAsync(IEventPublisher publisher, QualifiedActionId id,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        return (await publisher.PublishAndWaitAsync(new ActionCancelRequested
        {
            TenantId = id.TenantId, ActionId = id.ActionId
        }, cancellation).ConfigureAwait(false)).Found;
    }

    /// <summary>Queues an action in the current consume, in its transaction, and returns its ID: the ID of the queued
    /// action the input merged into, or a new one allocated deterministically when the input's ActionId is zero.</summary>
    public QualifiedActionId Enqueue(IActionInput input, ActionEnqueueOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        var ev = CurrentConsume();
        var type = TypeOf(input);
        options ??= new();
        if (input.ActionId == 0) input.ActionId = Ambient.AllocateId();
        if (options.StartAt is { } startAt && startAt > ev.When)
        {
            Ambient.Relation<IDelayedActions>().Insert(new()
            {
                TenantId = input.TenantId, ActionId = input.ActionId, StartAt = startAt, Name = type.Name,
                Priority = options.Priority, Input = input, QueuedAt = ev.When
            });
            return new(input.TenantId, input.ActionId);
        }
        return Queue(type, input, options.Priority, ev);
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

    /// <summary>Publishes <see cref="ActionsSuspended"/>: suspended types start no new attempts, running ones go on,
    /// and their inputs keep queueing. Resume them with <paramref name="suspended"/> false.</summary>
    public async Task SuspendAsync(IEventPublisher publisher, IEnumerable<string> names, bool suspended = true,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(names);
        await publisher.PublishAsync(new ActionsSuspended { Names = names.ToList(), Suspended = suspended }, cancellation)
            .ConfigureAwait(false);
    }

    /// <summary>The suspended action types, from the ambient query or consume snapshot.</summary>
    public IReadOnlyList<string> SuspendedActions =>
        Ambient.Relation<IBackendActionsStates>().FindByIdOrDefault(0)?.Suspended ?? [];

    internal void Suspend(List<string> names, bool suspended)
    {
        var state = State();
        var set = new SortedSet<string>(state.Suspended, StringComparer.Ordinal);
        foreach (var name in names)
            if (suspended) set.Add(name);
            else set.Remove(name);
        state.Suspended = set.ToList();
        Ambient.Relation<IBackendActionsStates>().Update(state);
        Changed(ActionChanges.Settings);
    }

    internal void ActivateCoordinator(Guid session)
    {
        var state = State();
        state.CoordinatorSession = session;
        Ambient.Relation<IBackendActionsStates>().Update(state);
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

    internal void Finish(ActionFinished ev)
    {
        if (!Enum.IsDefined(ev.Kind)) throw new ArgumentException("Unknown action outcome.");
        var queued = Ambient.Relation<IQueuedActions>();
        if (queued.FindByIdOrDefault(ev.TenantId, ev.ActionId) is not { IsStarted: true } row ||
            row.RunAttempts != ev.Attempt)
            return; // a superseded attempt or an action that already finished
        Changed(new(row.TenantId, row.ActionId));
        if (ev.Kind == ActionOutcomeKind.Partial)
            TypeOf(row.Name)?.Finished((IActionInput)row.Input!, ev.Kind, ev.Attempt, ev.Result, ev.Error);
        else
            Complete(row, ev.Kind, ev.Attempt, ev.Result, ev.Error);
    }

    internal void QueueDue(DelayedActionsDue ev)
    {
        var delayed = Ambient.Relation<IDelayedActions>();
        foreach (var key in ev.Actions)
        {
            if (delayed.FindByIdOrDefault(key.TenantId, key.ActionId) is not { } row || row.StartAt > ev.When) continue;
            delayed.RemoveById(row.TenantId, row.ActionId);
            var input = (IActionInput)row.Input!;
            Queue(TypeOf(input), input, row.Priority, ev);
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
            input.ActionId = Ambient.AllocateId();
            Queue(type, input, ActionPriority.Normal, ev);
        }
    }

    QualifiedActionId Queue(ActionType type, IActionInput input, ActionPriority priority, Event ev)
    {
        var queued = Ambient.Relation<IQueuedActions>();
        if (input is IActionInputUnificator unificator && unificator.GetUniqueKey() is { Length: > 0 } key)
        {
            var indexes = Ambient.Relation<IActionInputIndexes>();
            if (indexes.FindByIdOrDefault(input.TenantId, type.Name, key) is { } index)
            {
                var existing = queued.FindByIdOrDefault(input.TenantId, index.ActionId) ??
                               throw new InvalidOperationException($"Action index {type.Name}/{key} has no queued action.");
                if (input is not IActionInputAggregator aggregator ||
                    existing.IsAggregable && ((IActionInputAggregator)existing.Input!).TryAggregate(aggregator))
                {
                    existing.LatestInputEventId = ev.EventId;
                    queued.Update(existing);
                    return new(existing.TenantId, existing.ActionId); // its scheduling does not change
                }
                existing.IsAggregable = false;
                queued.Update(existing);
                index.ActionId = input.ActionId;
                indexes.Update(index);
            }
            else
                indexes.Insert(new() { TenantId = input.TenantId, Name = type.Name, UniqueKey = key, ActionId = input.ActionId });
        }
        var state = State();
        queued.Insert(new()
        {
            TenantId = input.TenantId, ActionId = input.ActionId, Name = type.Name, Priority = priority,
            Order = state.NextOrder++, Input = input, IsAggregable = input is IActionInputAggregator,
            LatestInputEventId = ev.EventId, QueuedAt = ev.When
        });
        Ambient.Relation<IBackendActionsStates>().Update(state);
        Changed(new(input.TenantId, input.ActionId));
        return new(input.TenantId, input.ActionId);
    }

    // Removes the row after its handler's final outcome; an unregistered type finishes without a callback.
    void Complete(QueuedAction row, ActionOutcomeKind kind, ulong attempt, object? result, ActionError? error)
    {
        TypeOf(row.Name)?.Finished((IActionInput)row.Input!, kind, attempt, result, error);
        RemoveIndex(row);
        Ambient.Relation<IQueuedActions>().RemoveById(row.TenantId, row.ActionId);
        Changed(new(row.TenantId, row.ActionId));
    }

    // Tells a coordinator running on this node, once the batch is visible; replay registers nothing.
    void Changed(QualifiedActionId id)
    {
        if (Changes.Active) Ambient.RegisterPostCommit(() => Changes.Add(id));
    }

    static void RemoveIndex(QueuedAction row)
    {
        if (row.Input is not IActionInputUnificator unificator || unificator.GetUniqueKey() is not { Length: > 0 } key) return;
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
        : throw new InvalidOperationException("Enqueue and Cancel run only inside a consume; use EnqueueAsync or CancelAsync elsewhere.");
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
