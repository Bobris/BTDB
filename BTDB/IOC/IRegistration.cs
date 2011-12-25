using System;

namespace BTDB.IOC
{
    public interface IRegistration
    {
        object InternalTraits(Type trait);
    }

    public interface IRegistration<TTraits> : IRegistration
    {
        IRegistration<TTraits> As<T>();
    }
}