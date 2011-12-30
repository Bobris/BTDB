using System;
using System.Collections.Generic;

namespace BTDB.IOC
{
    internal class AsTraitImpl : IAsTrait, IAsTraitImpl
    {
        readonly List<KeyValuePair<object, Type>> _asTypes = new List<KeyValuePair<object, Type>>();
        bool _asSelf;
        bool _asImplementedInterfaces;

        public void As(Type type)
        {
            _asTypes.Add(new KeyValuePair<object, Type>(null, type));
        }

        public void Keyed(object serviceKey, Type type)
        {
            _asTypes.Add(new KeyValuePair<object, Type>(serviceKey, type));
        }

        public void AsSelf()
        {
            _asSelf = true;
        }

        public void AsImplementedInterfaces()
        {
            _asImplementedInterfaces = true;
        }

        public IEnumerable<KeyValuePair<object, Type>> GetAsTypesFor(Type implementationType)
        {
            var defaultNeeded = true;
            foreach (var asType in _asTypes)
            {
                if (!asType.Value.IsAssignableFrom(implementationType)) continue;
                yield return asType;
                defaultNeeded = false;
            }
            if (_asImplementedInterfaces)
            {
                foreach (var type in implementationType.GetInterfaces())
                {
                    if (type == typeof (IDisposable)) continue;
                    yield return new KeyValuePair<object, Type>(null, type);
                    defaultNeeded = false;
                }
            }
            if (_asSelf || defaultNeeded)
            {
                yield return new KeyValuePair<object, Type>(null, implementationType);
            }
        }
    }
}