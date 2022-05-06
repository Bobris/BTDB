using System.Collections.Generic;
using System.Reflection;
using BTDB.Collections;
using BTDB.KVDBLayer;

namespace BTDB.IOC;

class ContainerRegistrationContext
{
    readonly ContainerImpl _container;
    readonly Dictionary<KeyAndType, ICReg> _registrations;
    StructList<object> _instances;

    internal ContainerRegistrationContext(ContainerImpl container, Dictionary<KeyAndType, ICReg> registrations)
    {
        _container = container;
        _registrations = registrations;
    }

    internal int SingletonCount { get; set; }

    internal object[] Instances => _instances.ToArray();

    internal int AddInstance(object instance)
    {
        _instances.Add(instance);
        return (int)_instances.Count - 1;
    }

    public void AddCReg(IEnumerable<KeyAndType> asTypes, bool preserveExistingDefaults, bool uniqueRegistration, ICReg registration)
    {
        foreach (var asType in asTypes)
        {
            AddCReg(asType, preserveExistingDefaults, uniqueRegistration, registration);
        }
    }

    public void AddCReg(KeyAndType asType, bool preserveExistingDefaults, bool uniqueRegistration, ICReg registration)
    {
        if (!_registrations.TryGetValue(asType, out var currentReg))
        {
            _registrations.Add(asType, registration);
            return;
        }

        if (uniqueRegistration)
        {
            throw new BTDBException($"IOC Registration of {asType} is not unique");
        }

        if (currentReg is ICRegMulti multi)
        {
            multi.Add(registration, preserveExistingDefaults);
            return;
        }
        multi = new CRegMulti();
        multi.Add(currentReg, false);
        multi.Add(registration, preserveExistingDefaults);
        _registrations[asType] = multi;
    }
}
