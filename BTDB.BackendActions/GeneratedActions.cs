using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace BTDB.BackendActions;

/// <summary>What registration needs to know about an action type: its <see cref="BackendActionAttribute"/> values,
/// its schedule and how to create its input for a scheduled start.</summary>
internal sealed record ActionMetadata(string? Name, int Cpu, int MaxConcurrent, string? ConcurrencyGroup,
    bool SingleRunningPerTenant, ActionPriority Priority, ActionScheduleAttribute? Schedule, Func<IActionInput>? Create)
{
    // Hand registration of an input the source generator did not see.
    internal static ActionMetadata Reflect<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] TInput>()
        where TInput : IActionInput
    {
        var attribute = (BackendActionAttribute?)Attribute.GetCustomAttribute(typeof(TInput), typeof(BackendActionAttribute))
                        ?? new BackendActionAttribute();
        var schedule = (ActionScheduleAttribute?)Attribute.GetCustomAttribute(typeof(TInput), typeof(ActionScheduleAttribute));
        var create = typeof(TInput).GetConstructor(Type.EmptyTypes) == null ? null
            : (Func<IActionInput>)(() => (IActionInput)Activator.CreateInstance(typeof(TInput))!);
        return new(attribute.Name, attribute.Cpu, attribute.MaxConcurrent, attribute.ConcurrencyGroup,
            attribute.SingleRunningPerTenant, attribute.Priority, schedule, create);
    }
}

/// <summary>An action handler the source generator found; generated module initializers add one per handler and
/// input to <see cref="GeneratedActions"/>.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class GeneratedAction
{
    private protected GeneratedAction(Type handlerType, Type inputType, Func<object>? createHandler)
    {
        HandlerType = handlerType;
        InputType = inputType;
        CreateHandler = createHandler;
    }

    public Type HandlerType { get; }
    public Type InputType { get; }
    /// <summary>The handler's parameterless constructor, when it has an accessible one.</summary>
    public Func<object>? CreateHandler { get; }
    internal abstract ActionMetadata Metadata { get; }
    internal abstract void RegisterTo(BackendActions actions, Func<Type, object>? handlers);
}

[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class GeneratedAction<TInput, TResult>(Type handlerType, string? name, int cpu, int maxConcurrent,
    string? concurrencyGroup, bool singleRunningPerTenant, ActionPriority priority, ActionScheduleAttribute? schedule,
    Func<TInput>? createInput, Func<object>? createHandler) : GeneratedAction(handlerType, typeof(TInput), createHandler)
    where TInput : ActionInput<TResult> where TResult : class
{
    internal override ActionMetadata Metadata { get; } = new(name, cpu, maxConcurrent, concurrencyGroup,
        singleRunningPerTenant, priority, schedule, createInput == null ? null : () => createInput());

    internal override void RegisterTo(BackendActions actions, Func<Type, object>? handlers)
    {
        var handler = handlers?.Invoke(HandlerType) ?? CreateHandler?.Invoke() ?? throw new InvalidOperationException(
            $"Handler {HandlerType.FullName} has no parameterless constructor; pass a handler factory.");
        actions.Register((IActionHandler<TInput, TResult>)handler, Metadata);
    }
}

/// <summary>The handlers the source generator found in every loaded assembly.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public static class GeneratedActions
{
    static readonly object Lock = new();
    static readonly List<GeneratedAction> Actions = new();

    /// <summary>Called by generated module initializers.</summary>
    public static void Add(GeneratedAction action)
    {
        ArgumentNullException.ThrowIfNull(action);
        lock (Lock) Actions.Add(action);
    }

    internal static IReadOnlyList<GeneratedAction> All
    {
        get { lock (Lock) return Actions.ToArray(); }
    }

    internal static ActionMetadata? Metadata<TInput, TResult>() where TInput : ActionInput<TResult> where TResult : class
    {
        lock (Lock) return Actions.FirstOrDefault(a => a.InputType == typeof(TInput))?.Metadata;
    }
}
