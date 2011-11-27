using System;
using System.Collections.Generic;

namespace BTDB.IOC
{
    internal class SingleRegistration : IRegistration
    {
        readonly Type _implementationType;
        readonly List<Type> _asTypes = new List<Type>();
        Lifetime _lifetime;

        public SingleRegistration(Type implementationType)
        {
            _implementationType = implementationType;
        }

        internal Lifetime HasLifetime
        {
            get { return _lifetime; }
        }

        internal Type ImplementationType
        {
            get { return _implementationType; }
        }

        internal IEnumerable<Type> AsTypes
        {
            get { return _asTypes; }
        }

        public IRegistration As(Type type)
        {
            _asTypes.Add(type);
            return this;
        }

        public IRegistration SingleInstance()
        {
            _lifetime = Lifetime.Singleton;
            return this;
        }
    }
}