using System;
using System.Collections.Generic;
using BTDB.Collections;

namespace BTDB.IOC;

class AsTraitImpl : IAsTrait, IAsTraitImpl
{
    StructList<KeyAndType> _asTypes;
    bool _asSelf;
    bool _asImplementedInterfaces;
    bool _preserveExistingDefaults;

    public void As(Type type)
    {
        _asTypes.Add(new KeyAndType(null, type));
    }

    public void Keyed(object serviceKey, Type type)
    {
        _asTypes.Add(new KeyAndType(serviceKey, type));
    }

    public void AsSelf()
    {
        _asSelf = true;
    }

    public void AsImplementedInterfaces()
    {
        _asImplementedInterfaces = true;
    }

    public void SetPreserveExistingDefaults()
    {
        _preserveExistingDefaults = true;
    }

    public IEnumerable<KeyAndType> GetAsTypesFor(Type implementationType)
    {
        var defaultNeeded = true;
        foreach (var asType in _asTypes)
        {
            if (!asType.Type.IsAssignableFrom(implementationType)) continue;
            yield return asType;
            defaultNeeded = false;
        }
        if (_asImplementedInterfaces)
        {
            foreach (var type in implementationType.GetInterfaces())
            {
                if (type == typeof(IDisposable)) continue;
                if (type == typeof(IAsyncDisposable)) continue;
                yield return new KeyAndType(null, type);
                defaultNeeded = false;
            }
        }
        if (_asSelf || defaultNeeded)
        {
            yield return new KeyAndType(null, implementationType);
        }
    }

    public bool PreserveExistingDefaults => _preserveExistingDefaults;
    public bool UniqueRegistration { get; set; }
}
