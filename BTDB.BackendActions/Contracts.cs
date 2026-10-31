namespace BTDB.BackendActions;

/// <summary>Lower values start first.</summary>
public enum ActionPriority : byte
{
    RealTime = 2,
    Normal = 4,
    Low = 8,
    Idle = 16
}

/// <summary>An action's identity. New IDs are unique on their own; imported ones only together with their tenant.</summary>
public readonly record struct QualifiedActionId(ulong TenantId, ulong ActionId);

/// <summary>Input of a backend action, stored in ObjectDB and serialized in events. TenantId scopes fairness and
/// single-running limits (0 means no tenant); ActionId is assigned on enqueue when it is zero.</summary>
[Generate]
public interface IActionInput
{
    ulong TenantId { get; set; }
    ulong ActionId { get; set; }
}

/// <summary>Base of inputs whose handler returns <typeparamref name="TResult"/>. Results travel in events, so mark
/// the result class with <c>[BTDB.Generate]</c> unless the source generator already discovers it.</summary>
public abstract class ActionInput<TResult> : IActionInput where TResult : class
{
    public ulong TenantId { get; set; }
    public ulong ActionId { get; set; }
}

/// <summary>An enqueued input with the same type, tenant and nonempty key as a queued action that has not started
/// yet merges into it instead of creating another action.</summary>
public interface IActionInputUnificator : IActionInput
{
    string GetUniqueKey();
}

/// <summary>A unificator whose queued input absorbs the new one. When <see cref="TryAggregate"/> returns false, the
/// queued action stops aggregating and the new input starts a new action.</summary>
public interface IActionInputAggregator : IActionInputUnificator
{
    bool TryAggregate(IActionInputAggregator other);
}

/// <summary>Execution limits of an action type, placed on its input class. The name defaults to the input type's full
/// name; set it to keep stored actions when the type is renamed.</summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class BackendActionAttribute : Attribute
{
    public string? Name { get; set; }
    /// <summary>Cost in CPU units; 100 is one core.</summary>
    public int Cpu { get; set; } = 100;
    /// <summary>Maximum actions of this type (or of its concurrency group) running in the cluster; 0 is unlimited.</summary>
    public int MaxConcurrent { get; set; }
    public string? ConcurrencyGroup { get; set; }
    public bool SingleRunningPerTenant { get; set; }
}

public enum ActionOutcomeKind : byte
{
    Completed = 1,
    Partial = 2,
    Failed = 3,
    Cancelled = 4,
    Interrupted = 5
}

/// <summary>A failure description that travels in the finishing event.</summary>
public sealed class ActionError
{
    public string Code { get; set; } = "";
    public string Message { get; set; } = "";
    public string? Details { get; set; }
}

/// <summary>Attempt counts the action's starts, from 1; it is 0 for an action cancelled before it started.</summary>
public readonly record struct ActionOutcome<TResult>(ActionOutcomeKind Kind, ulong Attempt, TResult? Result,
    ActionError? Error) where TResult : class;

/// <summary>Execution context of one attempt on a worker.</summary>
public interface IActionContext
{
    QualifiedActionId Id { get; }
    ulong Attempt { get; }
    CancellationToken Cancellation { get; }
}

public interface IActionHandler<in TInput, TResult> where TInput : ActionInput<TResult> where TResult : class
{
    /// <summary>Runs off-transaction on the assigned worker; may run more than once for one action.</summary>
    Task<TResult> ExecuteAsync(TInput input, IActionContext context);

    /// <summary>Runs inside the consume of the event that finishes the attempt, on every replica, exactly once per
    /// outcome. Like any consume it must be deterministic.</summary>
    void OnFinished(TInput input, ActionOutcome<TResult> outcome);
}

/// <summary>Runs inside the consume that records the start, before execution. An exception finishes the attempt as
/// <see cref="ActionOutcomeKind.Failed"/>; writes made before it stay, so validate before writing.</summary>
public interface IActionWithBeforeStart<in TInput> where TInput : IActionInput
{
    void OnBeforeStart(TInput input);
}

public sealed class ActionEnqueueOptions
{
    public ActionPriority Priority { get; set; } = ActionPriority.Normal;
    /// <summary>A start time after the enqueuing event's time keeps the action delayed until then.</summary>
    public DateTime? StartAt { get; set; }
}
