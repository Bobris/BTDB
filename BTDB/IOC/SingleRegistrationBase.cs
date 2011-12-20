using System;
using System.Collections.Generic;
using System.Linq;

namespace BTDB.IOC
{
    internal class SingleRegistrationBase : IRegistration
    {
        protected readonly Type ImplementationType;
        protected readonly List<Type> AsTypes = new List<Type>();
        protected Lifetime Lifetime;

        protected SingleRegistrationBase(Type type)
        {
            ImplementationType = type;
            Lifetime = Lifetime.AlwaysNew;
        }

        protected void FinalizeAsTypes()
        {
            if (AsTypes.Count==0)
            {
                AsSelf();
            }
        }

        public IRegistration As(Type type)
        {
            AsTypes.Add(type);
            return this;
        }

        public IRegistration SingleInstance()
        {
            Lifetime = Lifetime.Singleton;
            return this;
        }

        public IRegistration AsSelf()
        {
            AsTypes.Add(ImplementationType);
            return this;
        }

        public IRegistration AsImplementedInterfaces()
        {
            foreach (var type in ImplementationType.GetInterfaces().Where(t => t != typeof(IDisposable)))
            {
                AsTypes.Add(type);
            }
            return this;
        }
    }
}