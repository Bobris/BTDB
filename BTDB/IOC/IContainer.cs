using System;

namespace BTDB.IOC
{
    public interface IContainer
    {
        object Resolve(Type type);
        object ResolveNamed(string name, Type type);
        object ResolveKeyed(object key, Type type);
    }
}