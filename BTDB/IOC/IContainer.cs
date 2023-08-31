using System;
using System.Collections.Generic;

namespace BTDB.IOC;

public interface IContainer
{
    object Resolve(Type type);
    object ResolveNamed(string name, Type type);
    object ResolveKeyed(object key, Type type);
    object? ResolveOptional(Type type);
    object? ResolveOptionalNamed(string name, Type type);
    object? ResolveOptionalKeyed(object key, Type type);
    Func<IContainer,IResolvingCtx?,object?>? CreateFactory(ICreateFactoryCtx ctx, Type type, object? key);

    internal static readonly Dictionary<nint, Func<IContainer, ICreateFactoryCtx, Func<IContainer,IResolvingCtx?,object>>> FactoryRegistry = new();
    public static void RegisterFactory(nint typeToken, Func<IContainer, ICreateFactoryCtx, Func<IContainer,IResolvingCtx?,object>> factory)
    {
        FactoryRegistry[typeToken] = factory;
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
}
