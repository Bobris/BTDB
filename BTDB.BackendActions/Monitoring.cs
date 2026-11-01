using BTDB.Replication.EventSourcing;

namespace BTDB.BackendActions;

public enum ActionState : byte
{
    Delayed = 1,
    Queued = 2,
    Started = 3
}

/// <summary>One action as the replicated database records it. A started action may be running, waiting for its
/// worker's next exchange or about to start again; the leader overview shows what runs.</summary>
public sealed record ActionInfo(QualifiedActionId Id, string Name, ActionState State, ActionPriority Priority,
    ulong RunAttempts, bool IsCancelled, DateTime QueuedAt, DateTime? StartAt, string WorkerNode, object? Input);

public sealed partial class BackendActions
{
    /// <summary>Delayed, queued and started actions, of one tenant or of all, from the ambient query or consume
    /// snapshot; any node answers without the leader. Delayed actions come first, each kind in key order.</summary>
    public IEnumerable<ActionInfo> ListActions(ulong? tenantId = null)
    {
        var delayed = Ambient.Relation<IDelayedActions>();
        foreach (var row in tenantId is { } d ? delayed.FindById(d) : delayed) yield return Info(row);
        var queued = Ambient.Relation<IQueuedActions>();
        foreach (var row in tenantId is { } q ? queued.FindById(q) : queued) yield return Info(row);
    }

    /// <summary>The action, or null when it is not delayed, queued or started.</summary>
    public ActionInfo? FindAction(QualifiedActionId id) =>
        Ambient.Relation<IQueuedActions>().FindByIdOrDefault(id.TenantId, id.ActionId) is { } queued ? Info(queued)
        : Ambient.Relation<IDelayedActions>().FindByIdOrDefault(id.TenantId, id.ActionId) is { } delayed ? Info(delayed)
        : null;

    /// <summary>True when the tenant has a started action, for example before deleting the tenant's data.</summary>
    public bool HasStartedActions(ulong tenantId) =>
        Ambient.Relation<IQueuedActions>().FindById(tenantId).Any(row => row.IsStarted);

    /// <summary>The queued, not yet started action an input with this unique key would merge into.</summary>
    public QualifiedActionId? FindByUniqueKey(ulong tenantId, string name, byte[] uniqueKey) =>
        Ambient.Relation<IActionInputIndexes>().FindByIdOrDefault(tenantId, name, uniqueKey) is { } index
            ? new(tenantId, index.ActionId) : null;

    static ActionInfo Info(QueuedAction row) => new(new(row.TenantId, row.ActionId), row.Name,
        row.IsStarted ? ActionState.Started : ActionState.Queued, row.Priority, row.RunAttempts, row.IsCancelled,
        row.QueuedAt, null, row.IsStarted ? row.WorkerNode : "", row.Input);

    static ActionInfo Info(DelayedAction row) => new(new(row.TenantId, row.ActionId), row.Name, ActionState.Delayed,
        row.Priority, 0, false, row.QueuedAt, row.StartAt, "", row.Input);
}

/// <summary>The active coordinator's view; see <see cref="BackendActionsHost.GetOverviewAsync"/>.</summary>
public sealed record ActionsOverview(Guid CoordinatorSession, IReadOnlyList<WorkerOverview> Workers,
    IReadOnlyList<PriorityOverview> Priorities, IReadOnlyList<string> Suspended);

/// <summary>A worker the coordinator heard from: CPU in use and capacity, and its attempts.</summary>
public sealed record WorkerOverview(Guid Session, string NodeId, ulong Generation, int Capacity, int Used, bool Ready,
    TimeSpan SinceExchange, int Attempts);

/// <summary>Actions of one priority: waiting to start, held by a worker, and started under an earlier coordinator
/// session and not yet reported by their worker.</summary>
public sealed record PriorityOverview(ActionPriority Priority, int Waiting, int Running, int Protected);
