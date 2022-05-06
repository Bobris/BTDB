using System;

namespace BTDB.IOC;

public interface IContainer
{
    object Resolve(Type type);
    object ResolveNamed(string name, Type type);
    object ResolveKeyed(object key, Type type);
    object? ResolveOptional(Type type);
    object? ResolveOptionalNamed(string name, Type type);
    object? ResolveOptionalKeyed(object key, Type type);
}
