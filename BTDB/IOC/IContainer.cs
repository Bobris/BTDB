using System;

namespace BTDB.IOC
{
    public interface IContainer
    {
        object Resolve(Type type);
    }
}