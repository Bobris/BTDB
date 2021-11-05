using System;

namespace BTDB.IOC;

abstract class RegistrationBaseImpl<TTraits> : IRegistration<TTraits> where TTraits : IAsTrait
{
    public IRegistration<TTraits> As<T>()
    {
        ((IAsTrait)InternalTraits(typeof(IAsTrait))).As(typeof(T));
        return this;
    }

    public IRegistration<TTraits> Keyed<T>(object key)
    {
        ((IAsTrait)InternalTraits(typeof(IAsTrait))).Keyed(key, typeof(T));
        return this;
    }

    public IRegistration<TTraits> Named<T>(string name)
    {
        ((IAsTrait)InternalTraits(typeof(IAsTrait))).Keyed(name, typeof(T));
        return this;
    }

    public abstract object InternalTraits(Type trait);
}
