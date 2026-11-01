using BTDB.ODBLayer;

namespace BTDB.BackendActions;

/// <summary>An action waiting to start or running. Started is set by the consume that records an attempt; a later
/// attempt of the same action supersedes it.</summary>
public class QueuedAction
{
    [PrimaryKey(1)] public ulong TenantId { get; set; }
    [PrimaryKey(2)] public ulong ActionId { get; set; }
    public string Name { get; set; } = "";
    public ActionPriority Priority { get; set; }
    /// <summary>Enqueue order, unique across the cluster's actions, from <see cref="IQueuedActions.AllocateId"/>.</summary>
    public ulong Order { get; set; }
    public object? Input { get; set; }
    public QueuedActionFlags Flags { get; set; }

    /// <summary>True until the action starts or an input fails to aggregate into it.</summary>
    [NotStored]
    public bool IsAggregable
    {
        get => (Flags & QueuedActionFlags.Aggregable) != 0;
        set => Set(QueuedActionFlags.Aggregable, value);
    }

    /// <summary>Cancel requested while started; the coordinator finishes it.</summary>
    [NotStored]
    public bool IsCancelled
    {
        get => (Flags & QueuedActionFlags.Cancelled) != 0;
        set => Set(QueuedActionFlags.Cancelled, value);
    }

    [NotStored]
    public bool IsStarted
    {
        get => (Flags & QueuedActionFlags.Started) != 0;
        set => Set(QueuedActionFlags.Started, value);
    }

    /// <summary>Number of recorded starts; the current attempt when started.</summary>
    public ulong RunAttempts { get; set; }
    public Guid WorkerSession { get; set; }
    /// <summary>Node of the current attempt's worker; a new worker session on that node ends the attempt.</summary>
    public string WorkerNode { get; set; } = "";
    /// <summary>Event that recorded the current attempt; a worker reads the input after consuming it.</summary>
    public ulong StartEventId { get; set; }
    /// <summary>Event that queued the action or last merged an input into it.</summary>
    public ulong LatestInputEventId { get; set; }
    public DateTime QueuedAt { get; set; }

    void Set(QueuedActionFlags flag, bool value) => Flags = value ? Flags | flag : Flags & ~flag;
}

/// <summary>States of a <see cref="QueuedAction"/>, stored together in one byte.</summary>
[Flags]
public enum QueuedActionFlags : byte
{
    None = 0,
    Aggregable = 1,
    Cancelled = 2,
    Started = 4
}

public interface IQueuedActions : IRelation<QueuedAction>
{
    /// <summary>The next enqueue order: a counter of the relation, cheaper than a row every enqueue rewrites.</summary>
    ulong AllocateId();
    void Insert(QueuedAction item);
    void Update(QueuedAction item);
    QueuedAction? FindByIdOrDefault(ulong tenantId, ulong actionId);
    IEnumerable<QueuedAction> FindById(ulong tenantId);
    bool RemoveById(ulong tenantId, ulong actionId);
}

/// <summary>An action that becomes queued once <see cref="StartAt"/> passes.</summary>
public class DelayedAction
{
    [PrimaryKey(1)] public ulong TenantId { get; set; }
    [PrimaryKey(2)] public ulong ActionId { get; set; }
    [SecondaryKey("StartAt")] public DateTime StartAt { get; set; }
    public string Name { get; set; } = "";
    public ActionPriority Priority { get; set; }
    public object? Input { get; set; }
    public DateTime QueuedAt { get; set; }
}

public interface IDelayedActions : IRelation<DelayedAction>
{
    void Insert(DelayedAction item);
    void Update(DelayedAction item);
    DelayedAction? FindByIdOrDefault(ulong tenantId, ulong actionId);
    IEnumerable<DelayedAction> FindById(ulong tenantId);
    IEnumerable<DelayedAction> ListByStartAt(AdvancedEnumeratorParam<DateTime> param);
    bool RemoveById(ulong tenantId, ulong actionId);
}

/// <summary>Unification key of a queued action that has not started yet.</summary>
public class ActionInputIndex
{
    [PrimaryKey(1)] public ulong TenantId { get; set; }
    [PrimaryKey(2)] public string Name { get; set; } = "";
    [PrimaryKey(3)] public byte[] UniqueKey { get; set; } = [];
    public ulong ActionId { get; set; }
}

public interface IActionInputIndexes : IRelation<ActionInputIndex>
{
    void Insert(ActionInputIndex item);
    void Update(ActionInputIndex item);
    ActionInputIndex? FindByIdOrDefault(ulong tenantId, string name, byte[] uniqueKey);
    bool RemoveById(ulong tenantId, string name, byte[] uniqueKey);
}

/// <summary>The latest occurrence a scheduled action type was started for (or, the first time, recorded at).</summary>
public class ScheduledActionRun
{
    [PrimaryKey(1)] public string Name { get; set; } = "";
    public DateTime LastRun { get; set; }
}

public interface IScheduledActionRuns : IRelation<ScheduledActionRun>
{
    ScheduledActionRun? FindByIdOrDefault(string name);
}

/// <summary>Cluster-wide state in one row with Id 0.</summary>
public class BackendActionsState
{
    [PrimaryKey(1)] public ulong Id { get; set; }
    /// <summary>The coordinator session whose start events are accepted.</summary>
    public Guid CoordinatorSession { get; set; }
    /// <summary>Action types that start no new attempts; their inputs keep queueing.</summary>
    public List<string> Suspended { get; set; } = new();
}

public interface IBackendActionsStates : IRelation<BackendActionsState>
{
    void Insert(BackendActionsState item);
    void Update(BackendActionsState item);
    BackendActionsState? FindByIdOrDefault(ulong id);
}
