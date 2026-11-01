using BTDB.Replication.EventSourcing;

namespace BTDB.BackendActions;

/// <summary>Enqueues an input from outside a consume; see <see cref="BackendActions.EnqueueAsync"/>.</summary>
public class ActionEnqueued : EventWithResult<ActionEnqueuedResult>
{
    public object? Input { get; set; }
    public ActionPriority Priority { get; set; } = ActionPriority.Normal;
    public DateTime? StartAt { get; set; }
}

public class ActionEnqueuedResult
{
    public ulong TenantId { get; set; }
    public ulong ActionId { get; set; }
}

public class ActionCancelRequested : EventWithResult<ActionCancelResult>
{
    public ulong TenantId { get; set; }
    public ulong ActionId { get; set; }
}

public class ActionCancelResult
{
    /// <summary>False when no queued or delayed action has this ID.</summary>
    public bool Found { get; set; }
}

/// <summary>Published by a new coordinator before it loads actions: a barrier, and the session whose
/// <see cref="ActionsStarted"/> events are accepted from then on.</summary>
public class CoordinatorActivated : PureEvent
{
    public Guid Session { get; set; }
}

public class StartedAction
{
    public ulong TenantId { get; set; }
    public ulong ActionId { get; set; }
    /// <summary>The attempt being started: the action's recorded attempts plus one.</summary>
    public ulong Attempt { get; set; }
    public Guid WorkerSession { get; set; }
    public string WorkerNode { get; set; } = "";
}

/// <summary>Records the attempts one planning round starts.</summary>
public class ActionsStarted : PureEvent
{
    public Guid Session { get; set; }
    public List<StartedAction> Actions { get; set; } = new();
}

/// <summary>Outcome of an attempt, from its worker or, for an abandoned attempt, from the coordinator.</summary>
public class ActionFinished : PureEvent
{
    public ulong TenantId { get; set; }
    public ulong ActionId { get; set; }
    public ulong Attempt { get; set; }
    public ActionOutcomeKind Kind { get; set; }
    public object? Result { get; set; }
    public ActionError? Error { get; set; }
}

/// <summary>A stopping worker's attempt ended without an outcome: the action queues again, and the attempt does not
/// count toward MaxAttempts.</summary>
public class ActionInterrupted : PureEvent
{
    public ulong TenantId { get; set; }
    public ulong ActionId { get; set; }
    public ulong Attempt { get; set; }
}

/// <summary>Starts a scheduled type for an occurrence, unless another start already moved its last run.</summary>
public class ScheduledActionStarted : PureEvent
{
    public string Name { get; set; } = "";
    /// <summary>The last run the coordinator read; default when the type never ran.</summary>
    public DateTime ExpectedLastRun { get; set; }
    public DateTime NewLastRun { get; set; }
    /// <summary>False only records NewLastRun, for the first check of a newly scheduled type or a vetoed one.</summary>
    public bool Start { get; set; }
}

/// <summary>Stops or resumes starting the named action types and the actions of the tenants; see
/// <see cref="BackendActions.SuspendAsync"/> and <see cref="BackendActions.SuspendTenantsAsync"/>.</summary>
public class ActionsSuspended : PureEvent
{
    public List<string> Names { get; set; } = new();
    public List<ulong> TenantIds { get; set; } = new();
    public bool Suspended { get; set; }
}

public class ActionKey
{
    public ulong TenantId { get; set; }
    public ulong ActionId { get; set; }
}

/// <summary>Moves delayed actions whose start time passed into the queue.</summary>
public class DelayedActionsDue : PureEvent
{
    public List<ActionKey> Actions { get; set; } = new();
}

sealed class BackendActionsEventHandler(BackendActions actions) :
    IEventHandler<ActionEnqueued, ActionEnqueuedResult>, IEventHandler<ActionCancelRequested, ActionCancelResult>,
    IEventHandler<CoordinatorActivated>, IEventHandler<ActionsStarted>, IEventHandler<ActionFinished>,
    IEventHandler<ActionInterrupted>,
    IEventHandler<DelayedActionsDue>, IEventHandler<ScheduledActionStarted>, IEventHandler<ActionsSuspended>,
    IEventHandler<ActionsImported>
{
    public ActionEnqueuedResult? Validate(ActionEnqueued ev)
    {
        if (ev.Input is not IActionInput input) throw new ArgumentException("The event carries no action input.");
        actions.TypeOf(input);
        return null;
    }

    public ActionEnqueuedResult Consume(ActionEnqueued ev)
    {
        var id = actions.Enqueue((IActionInput)ev.Input!, new() { Priority = ev.Priority, StartAt = ev.StartAt });
        return new() { TenantId = id.TenantId, ActionId = id.ActionId };
    }

    public ActionCancelResult? Validate(ActionCancelRequested ev) => null;

    public ActionCancelResult Consume(ActionCancelRequested ev) =>
        new() { Found = actions.Cancel(new(ev.TenantId, ev.ActionId)) };

    public void Validate(CoordinatorActivated ev) { }
    public void Consume(CoordinatorActivated ev) => actions.ActivateCoordinator(ev.Session);
    public void Validate(ActionsStarted ev) { }
    public void Consume(ActionsStarted ev) => actions.Start(ev);
    public void Validate(ActionFinished ev) => BackendActions.CheckKind(ev);
    public void Consume(ActionFinished ev) => actions.Finish(ev);
    public void Validate(ActionInterrupted ev) { }
    public void Consume(ActionInterrupted ev) => actions.Interrupt(ev);
    public void Validate(DelayedActionsDue ev) { }
    public void Consume(DelayedActionsDue ev) => actions.QueueDue(ev);
    public void Validate(ScheduledActionStarted ev) { }
    public void Consume(ScheduledActionStarted ev) => actions.StartScheduled(ev);
    public void Validate(ActionsSuspended ev) { }
    public void Consume(ActionsSuspended ev) => actions.Suspend(ev.Names, ev.TenantIds, ev.Suspended);
    public void Validate(ActionsImported ev) { }
    public void Consume(ActionsImported ev) => actions.Import(ev);
}
