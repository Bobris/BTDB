using BTDB.Replication.EventSourcing;

namespace BTDB.BackendActions;

/// <summary>An action carried over from another system at cutover, with its identity, order and attempts.</summary>
public class ImportedAction
{
    public ulong TenantId { get; set; }
    public ulong ActionId { get; set; }
    /// <summary>A registered input; its TenantId and ActionId are set to the values above.</summary>
    public object? Input { get; set; }
    public ActionPriority Priority { get; set; } = ActionPriority.Normal;
    /// <summary>The old system's order; only the relative order counts.</summary>
    public ulong Order { get; set; }
    public DateTime QueuedAt { get; set; }
    /// <summary>Starts so far; the next attempt is this plus one.</summary>
    public ulong RunAttempts { get; set; }
    /// <summary>Set for a delayed action.</summary>
    public DateTime? StartAt { get; set; }
    /// <summary>Finished as Cancelled by the import, with its handler's outcome.</summary>
    public bool IsCancelled { get; set; }
    /// <summary>False when the old action already stopped aggregating further inputs.</summary>
    public bool IsAggregable { get; set; } = true;
}

/// <summary>Imports actions, the last runs of scheduled types, and suspended types and tenants; see
/// <see cref="BackendActions.ImportAsync"/>.</summary>
public class ActionsImported : PureEvent
{
    public List<ImportedAction> Actions { get; set; } = new();
    public Dictionary<string, DateTime> ScheduledRuns { get; set; } = new();
    public List<string> Suspended { get; set; } = new();
    public List<ulong> SuspendedTenants { get; set; } = new();
}

public sealed partial class BackendActions
{
    /// <summary>Imports one batch: inside a query scope by publishing <see cref="ActionsImported"/> with the scope's
    /// publisher, inside a consume in its transaction. Import batches in ascending old order, below the publisher's
    /// MaximumEventSize, before any coordinator starts.</summary>
    public async Task ImportAsync(IReadOnlyList<ImportedAction> actions,
        IReadOnlyDictionary<string, DateTime>? scheduledRuns = null, IReadOnlyList<string>? suspended = null,
        IReadOnlyList<ulong>? suspendedTenants = null, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(actions);
        var ev = new ActionsImported
        {
            Actions = actions.ToList(),
            ScheduledRuns = scheduledRuns?.ToDictionary(r => r.Key, r => r.Value, StringComparer.Ordinal) ?? new(),
            Suspended = suspended?.ToList() ?? new(),
            SuspendedTenants = suspendedTenants?.ToList() ?? new()
        };
        if (!Ambient.IsQuery) Import(ev);
        else await Ambient.PublishEventAsync(ev, cancellation).ConfigureAwait(false);
    }

    /// <summary>Imports actions in the current consume, keeping their IDs. They queue in their old relative order,
    /// after the actions already queued; delayed ones stay delayed; cancelled ones finish as Cancelled at once. The
    /// unification index points to the latest aggregable action of each key, and the ID allocator rises above every
    /// imported ID. An ID that already exists fails the whole consume.</summary>
    public void Import(IEnumerable<ImportedAction> actions)
    {
        ArgumentNullException.ThrowIfNull(actions);
        var ev = CurrentConsume();
        var queued = Ambient.Relation<IQueuedActions>();
        var delayed = Ambient.Relation<IDelayedActions>();
        var indexes = Ambient.Relation<IActionInputIndexes>();
        var ordered = actions.OrderBy(a => a.Order).ToList();
        // Before any cancellation callback, so IDs it allocates never collide with a later imported row.
        Ambient.ReserveIdsUpTo(ordered.Count == 0 ? 0 : ordered.Max(a => a.ActionId));
        foreach (var action in ordered)
        {
            if (action.Input is not IActionInput input) throw new ArgumentException("An imported action has no input.");
            var type = TypeOf(input);
            if (action.ActionId == 0) throw new ArgumentException("An imported action needs its ActionId.");
            if (queued.FindByIdOrDefault(action.TenantId, action.ActionId) != null ||
                delayed.FindByIdOrDefault(action.TenantId, action.ActionId) != null)
                throw new InvalidOperationException($"Action {action.TenantId}/{action.ActionId} already exists.");
            input.TenantId = action.TenantId;
            input.ActionId = action.ActionId;
            if (action.IsCancelled)
            {
                type.Finished(input, ActionOutcomeKind.Cancelled, action.RunAttempts, null, null);
                continue;
            }
            if (action.StartAt is { } startAt)
            {
                delayed.Insert(new()
                {
                    TenantId = action.TenantId, ActionId = action.ActionId, StartAt = startAt, Name = type.Name,
                    Priority = action.Priority, Input = input, QueuedAt = action.QueuedAt
                });
                continue;
            }
            var aggregable = input is IActionInputAggregator && action.IsAggregable;
            queued.Insert(new()
            {
                TenantId = action.TenantId, ActionId = action.ActionId, Name = type.Name, Priority = action.Priority,
                Order = queued.AllocateId(), Input = input, IsAggregable = aggregable, RunAttempts = action.RunAttempts,
                LatestInputEventId = ev.EventId, QueuedAt = action.QueuedAt
            });
            if (input is IActionInputUnificator unificator && unificator.GetUniqueKey() is { } key &&
                (input is not IActionInputAggregator || aggregable))
                indexes.Upsert(new() { TenantId = action.TenantId, Name = type.Name, UniqueKey = key, ActionId = action.ActionId });
            Changed(new(action.TenantId, action.ActionId));
        }
    }

    /// <summary>Sets the last run of scheduled types in the current consume, so the first check after the cutover
    /// starts only occurrences the old system had not started.</summary>
    public void ImportScheduledRuns(IReadOnlyDictionary<string, DateTime> runs)
    {
        ArgumentNullException.ThrowIfNull(runs);
        CurrentConsume();
        var table = Ambient.Relation<IScheduledActionRuns>();
        foreach (var (name, lastRun) in runs.OrderBy(r => r.Key, StringComparer.Ordinal))
            table.Upsert(new() { Name = name, LastRun = lastRun });
    }

    internal void Import(ActionsImported ev)
    {
        Import(ev.Actions);
        ImportScheduledRuns(ev.ScheduledRuns);
        Suspend(ev.Suspended, ev.SuspendedTenants, true);
    }
}
