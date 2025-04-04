using System;
using System.Collections.Concurrent;

namespace BTDB.IOC;

public interface IContainer
{
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
}
