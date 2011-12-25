using System;

namespace BTDB.IOC
{
    internal abstract class RegistrationBaseImpl<TTraits> : IRegistration<TTraits> where TTraits : IAsTrait
    {
        public IRegistration<TTraits> As<T>()
        {
            ((IAsTrait)InternalTraits(typeof(IAsTrait))).As(typeof(T));
            return this;
        }

        public abstract object InternalTraits(Type trait);
    }
}