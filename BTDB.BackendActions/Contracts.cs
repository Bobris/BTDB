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

/// <summary>An enqueued input with the same type, tenant and key as a queued action that has not started yet merges
/// into it instead of creating another action.</summary>
public interface IActionInputUnificator : IActionInput
{
    /// <summary>The key, compared byte by byte; null means the input never merges, and an empty key merges with other
    /// empty keys. Compose a key of several values by writing them into one array, for example with
    /// <c>BinaryPrimitives</c> or <c>MemWriter</c>.</summary>
    byte[]? GetUniqueKey();
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
    /// <summary>Not final: an interrupted attempt's progress, from <see cref="ActionResult.InterruptedWith{TResult}"/>;
    /// the action runs again.</summary>
    Progress = 2,
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

/// <summary>Why <see cref="IActionContext.Cancellation"/> fired. The handler acknowledges it by returning
/// <see cref="ActionResult.Cancelled"/> or <see cref="ActionResult.Interrupted"/>, or by throwing
/// OperationCanceledException; a handler that returns a result instead completes the attempt with it.</summary>
public enum ActionCancellationReason : byte
{
    None = 0,
    /// <summary>The action was cancelled; an acknowledged attempt finishes as
    /// <see cref="ActionOutcomeKind.Cancelled"/>.</summary>
    Cancelled = 1,
    /// <summary><see cref="IActionContext.CancelAfter"/> elapsed; an acknowledged attempt finishes as Cancelled with the
    /// error code <c>Timeout</c>.</summary>
    Timeout = 2,
    /// <summary>A newer attempt replaced this one, or the worker stops; an acknowledged or failed attempt publishes
    /// nothing, so the action runs again.</summary>
    Interrupted = 3
}

/// <summary>Execution context of one attempt on a worker.</summary>
public interface IActionContext
{
    QualifiedActionId Id { get; }
    ulong Attempt { get; }
    /// <summary>Cancelled for every <see cref="ActionCancellationReason"/>; an OperationCanceledException thrown after
    /// it acknowledges the cancellation and finishes the attempt according to <see cref="CancellationReason"/>, while a
    /// returned result still completes it.</summary>
    CancellationToken Cancellation { get; }
    ActionCancellationReason CancellationReason { get; }
    /// <summary>A read-only snapshot of the worker's database; it already contains the event that started this attempt.</summary>
    Replication.EventSourcing.Ambient.Scope StartQuery();
    /// <summary>Publishes the application's own event and returns after this node consumed it, so parts keep their
    /// order and a slow consumer slows the action down. The application's handler consumes it; nothing else happens,
    /// so put <see cref="Id"/> and <see cref="Attempt"/> in the event to tell parts of a superseded attempt apart.
    /// Cancellation of the attempt cancels the wait.</summary>
    ValueTask SubmitPartialAsync(Replication.EventSourcing.PureEvent ev);
    /// <summary>Changes the CPU the coordinator accounts to this attempt by <paramref name="cpuDelta"/>, from its type's
    /// cost and never below 10; it applies with the worker's next exchange.</summary>
    void AdjustResources(int cpuDelta);
    /// <summary>Cancels the attempt with <see cref="ActionCancellationReason.Timeout"/> after the delay.</summary>
    void CancelAfter(TimeSpan delay);
}

/// <summary>Starts an action type on a schedule. The coordinator enqueues a new input of the type, created by its
/// parameterless constructor, whenever an occurrence passes; the first check after the type appears only records the
/// latest occurrence, so deploying a type never starts it at once.</summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public abstract class ActionScheduleAttribute : Attribute
{
    /// <summary>The latest occurrence at or before <paramref name="now"/> (UTC).</summary>
    public abstract DateTime LatestOccurrence(DateTime now);
}

/// <summary>Every <see cref="Minutes"/> minutes, counted from midnight UTC.</summary>
public sealed class RepeatedlyScheduledAttribute(int minutes) : ActionScheduleAttribute
{
    public int Minutes { get; } = minutes > 0 ? minutes : throw new ArgumentOutOfRangeException(nameof(minutes));

    public override DateTime LatestOccurrence(DateTime now)
    {
        var day = now.Date;
        return day.AddMinutes((int)(now - day).TotalMinutes / Minutes * Minutes);
    }
}

/// <summary>Daily at <see cref="MinuteOfDay"/> UTC.</summary>
public sealed class DailyScheduledAttribute(int minuteOfDay) : ActionScheduleAttribute
{
    public int MinuteOfDay { get; } = minuteOfDay is >= 0 and < 1440 ? minuteOfDay
        : throw new ArgumentOutOfRangeException(nameof(minuteOfDay));

    public override DateTime LatestOccurrence(DateTime now)
    {
        var today = now.Date.AddMinutes(MinuteOfDay);
        return today <= now ? today : today.AddDays(-1);
    }
}

/// <summary>Monthly on <see cref="Day"/> (the month's last day when it is shorter) at <see cref="MinuteOfDay"/> UTC.</summary>
public sealed class MonthlyScheduledAttribute(int day, int minuteOfDay) : ActionScheduleAttribute
{
    public int Day { get; } = day is >= 1 and <= 31 ? day : throw new ArgumentOutOfRangeException(nameof(day));
    public int MinuteOfDay { get; } = minuteOfDay is >= 0 and < 1440 ? minuteOfDay
        : throw new ArgumentOutOfRangeException(nameof(minuteOfDay));

    public override DateTime LatestOccurrence(DateTime now)
    {
        var occurrence = In(now.Year, now.Month);
        if (occurrence <= now) return occurrence;
        var previous = new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(-1);
        return In(previous.Year, previous.Month);
    }

    DateTime In(int year, int month) => new DateTime(year, month, Math.Min(Day, DateTime.DaysInMonth(year, month)), 0, 0,
        0, DateTimeKind.Utc).AddMinutes(MinuteOfDay);
}

/// <summary>Implemented by a handler of a scheduled type to skip occurrences; evaluated on the leader.</summary>
public interface IScheduledActionGate
{
    bool CanStartScheduled(DateTime now);
}

/// <summary>A failure with an application error code; any other exception from an action finishes it with the code
/// <c>Unexpected</c>.</summary>
public class BackendActionException(string code, string message, string? details = null) : Exception(message)
{
    public string Code { get; } = code;
    public string? Details { get; } = details;
}

/// <summary>How <c>ExecuteAsync</c> ends without an exception: <see cref="ActionResultKind.Completed"/> with a
/// result, which converts implicitly so <c>return result;</c> works, or <see cref="ActionResult.Cancelled"/> or
/// <see cref="ActionResult.Interrupted"/>, which acknowledge the attempt's cancellation reason, or
/// <see cref="ActionResult.InterruptedWith{TResult}"/>, which also commits progress.</summary>
public readonly struct ActionResult<TResult> where TResult : class
{
    internal ActionResult(ActionResultKind kind, TResult? result)
    {
        Kind = kind;
        Result = result;
    }

    public ActionResultKind Kind { get; }
    /// <summary>The result when completed; the progress, if any, when interrupted.</summary>
    public TResult? Result { get; }

    public static implicit operator ActionResult<TResult>(TResult? result) => new(ActionResultKind.Completed, result);
    public static implicit operator ActionResult<TResult>(ActionEnd end) => new(end.Kind, null);
}

public enum ActionResultKind : byte
{
    Completed = 0,
    /// <summary>The attempt finishes as <see cref="ActionOutcomeKind.Cancelled"/>, with the error code <c>Timeout</c>
    /// when <see cref="IActionContext.CancelAfter"/> elapsed.</summary>
    Cancelled = 1,
    /// <summary>The attempt publishes no outcome and the action runs again. It does not count toward MaxAttempts when
    /// it acknowledges <see cref="ActionCancellationReason.Interrupted"/>, and counts otherwise.</summary>
    Interrupted = 2
}

/// <summary>An end without a result, converting to any <see cref="ActionResult{TResult}"/>.</summary>
public readonly struct ActionEnd
{
    internal ActionEnd(ActionResultKind kind) => Kind = kind;
    internal ActionResultKind Kind { get; }
}

public static class ActionResult
{
    /// <summary><c>return ActionResult.Cancelled;</c> instead of throwing OperationCanceledException.</summary>
    public static ActionEnd Cancelled => new(ActionResultKind.Cancelled);
    /// <summary><c>return ActionResult.Interrupted;</c> instead of throwing OperationCanceledException.</summary>
    public static ActionEnd Interrupted => new(ActionResultKind.Interrupted);

    /// <summary>Interrupted, and commits <paramref name="progress"/> in the same event as the interruption:
    /// <c>OnFinished</c> receives it as a <see cref="ActionOutcomeKind.Progress"/> outcome, only while the attempt is
    /// current, so the application stores it and the next attempt resumes from it.</summary>
    public static ActionResult<TResult> InterruptedWith<TResult>(TResult progress) where TResult : class
    {
        ArgumentNullException.ThrowIfNull(progress);
        return new(ActionResultKind.Interrupted, progress);
    }
}

public interface IActionHandler<in TInput, TResult> where TInput : ActionInput<TResult> where TResult : class
{
    /// <summary>Runs off-transaction on the assigned worker; may run more than once for one action. It acknowledges a
    /// cancellation or interruption by returning <see cref="ActionResult.Cancelled"/> or
    /// <see cref="ActionResult.Interrupted"/>, or by throwing OperationCanceledException.</summary>
    Task<ActionResult<TResult>> ExecuteAsync(TInput input, IActionContext context);

    /// <summary>Runs inside the consume of the event that finishes the attempt, or that interrupts it with progress, on
    /// every replica, exactly once per outcome. Like any consume it must be deterministic.</summary>
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
