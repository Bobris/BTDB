using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace BTDB.IOC;

public interface IContainer : IAsyncDisposable
{
    IContainer CreateScope();
    object Resolve(Type type);
    object ResolveNamed(string name, Type type);
    object ResolveKeyed(object key, Type type);
    object? ResolveOptional(Type type);
    object? ResolveOptionalNamed(string name, Type type);
    object? ResolveOptionalKeyed(object key, Type type);
    Func<IContainer, IResolvingCtx?, object?>? CreateFactory(ICreateFactoryCtx ctx, Type type, object? key);
    Func<IContainer, IResolvingCtx?, object?> CreateFactory(Type type);

    internal static readonly
        ConcurrentDictionary<nint, Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx?, object>?>>
        FactoryRegistry = new();

    public static void RegisterFactory(Type type,
        Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx?, object>?> factory)
    {
        FactoryRegistry[type.TypeHandle.Value] = factory;
    }
}

public interface IRootContainer : IContainer
{
}

public interface IResolvingCtx
{
    object Exchange(int idx, object value);
    object Get(int idx);
    void Set(int idx, object value);
}

public interface ICreateFactoryCtx
{
    int GetParamSize();
    bool HasResolvingCtx();
    int AddInstanceToCtx(Type paramType, string? name = null);
    IDisposable ResolvingCtxRestorer();
}

public struct DispatcherItem
{
    public Func<IContainer, Func<IContainer, object, object?>> ExecuteFactory;
    public Func<IContainer, object, object?>? Execute;
    DispatcherExecutes? _executes;

    // Handlers are registered once per process, but their factories belong to a container, so they are kept for each
    // root container a dispatcher is created for or called with.
    public void Prepare(IContainer container)
    {
        var executes = _executes ??= new(ExecuteFactory);
        Execute = executes.Get(container);
    }

    public readonly object? Invoke(IContainer container, object message)
    {
        var executes = _executes ?? new DispatcherExecutes(ExecuteFactory);
        return executes.Get(container)(container, message);
    }
}

sealed class DispatcherExecutes
{
    readonly ConditionalWeakTable<IContainer, Func<IContainer, object, object?>> _byRoot = new();
    readonly ConditionalWeakTable<IContainer, Func<IContainer, object, object?>>.CreateValueCallback _create;

    // One object, so a concurrent reader never pairs a root with the handler of another one.
    sealed record Last(IContainer Root, Func<IContainer, object, object?> Execute);

    Last? _last;

    public DispatcherExecutes(Func<IContainer, Func<IContainer, object, object?>> executeFactory) =>
        _create = root => executeFactory(root);

    public Func<IContainer, object, object?> Get(IContainer container)
    {
        var root = container is ContainerImpl impl ? impl.Root : container;
        var last = _last;
        if (last != null && ReferenceEquals(last.Root, root)) return last.Execute;
        var execute = _byRoot.GetValue(root, _create);
        _last = new(root, execute);
        return execute;
    }
}
