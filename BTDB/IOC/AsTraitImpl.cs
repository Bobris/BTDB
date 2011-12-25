using System;
using System.Collections.Generic;

namespace BTDB.IOC
{
    internal class AsTraitImpl : IAsTrait, IAsTraitImpl
    {
        readonly List<Type> _asTypes = new List<Type>();
        bool _asSelf;
        bool _asImplementedInterfaces;

        public void As(Type type)
        {
            _asTypes.Add(type);
        }

        public void AsSelf()
        {
            _asSelf = true;
        }

        public void AsImplementedInterfaces()
        {
            _asImplementedInterfaces = true;
        }

        public IEnumerable<Type> GetAsTypesFor(Type implementationType)
        {
            var defaultNeeded = true;
            foreach (var asType in _asTypes)
            {
                if (!asType.IsAssignableFrom(implementationType)) continue;
                yield return asType;
                defaultNeeded = false;
            }
            if (_asImplementedInterfaces)
            {
                foreach (var type in implementationType.GetInterfaces())
                {
                    if (type == typeof (IDisposable)) continue;
                    yield return type;
                    defaultNeeded = false;
                }
            }
            if (_asSelf || defaultNeeded)
            {
                yield return implementationType;
            }
        }
    }
}