using System;
using System.Collections.Generic;
using BTDB.Collections;
using BTDB.IL;

namespace BTDB.IOC;

abstract class RegistrationBaseImpl<TTraits> : IRegistration<TTraits>, IAsTrait, IAsTraitImpl where TTraits : IAsTrait
{
    protected StructList<KeyAndType> _asTypes;
    bool _asSelf;
    bool _asImplementedInterfaces;
    protected bool _preserveExistingDefaults;

    public void As(Type type)
    {
        _asTypes.Add(new(null, type));
    }

    public void Keyed(object serviceKey, Type type)
    {
        _asTypes.Add(new(serviceKey, type));
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
            // if asType is open generic type, we need to close it with implementationType's generic arguments
            if (asType.Type.IsGenericTypeDefinition)
            {
                var closedType = implementationType.SpecializationOf(asType.Type);
                if (closedType == null) continue;
                yield return new(asType.Key, closedType);
                defaultNeeded = false;
                continue;
            }

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
                yield return new(null, type);
                defaultNeeded = false;
            }
        }

        if (_asSelf || defaultNeeded)
        {
            yield return new(null, implementationType);
        }
    }

    public bool PreserveExistingDefaults => _preserveExistingDefaults;
    public bool UniqueRegistration { get; set; }

    public IRegistration<TTraits> As<T>()
    {
        (this as IAsTrait)!.As(typeof(T));
        return this;
    }

    public IRegistration<TTraits> Keyed<T>(object key)
    {
        (this as IAsTrait)!.Keyed(key, typeof(T));
        return this;
    }

    public IRegistration<TTraits> Named<T>(string name)
    {
        (this as IAsTrait)!.Keyed(name, typeof(T));
        return this;
    }
}
