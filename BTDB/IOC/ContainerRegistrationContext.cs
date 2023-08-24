using System.Collections.Generic;
using System.Reflection;
using BTDB.Collections;
using BTDB.KVDBLayer;

namespace BTDB.IOC;

class ContainerRegistrationContext
{
    readonly ContainerImpl _container;
    readonly Dictionary<KeyAndType, CReg> _registrations;

    internal ContainerRegistrationContext(ContainerImpl container, Dictionary<KeyAndType, CReg> registrations)
    {
        _container = container;
        _registrations = registrations;
    }

    internal uint SingletonCount { get; set; }

    public void AddCReg(IEnumerable<KeyAndType> asTypes, bool preserveExistingDefaults, bool uniqueRegistration, CReg registration)
    {
        foreach (var asType in asTypes)
        {
            AddCReg(asType, preserveExistingDefaults, uniqueRegistration, registration);
        }
    }

    public void AddCReg(KeyAndType asType, bool preserveExistingDefaults, bool uniqueRegistration, CReg registration)
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

        if (currentReg.Multi.Count > 0)
        {
            currentReg.Add(registration, preserveExistingDefaults);
        }

        var multi = new CReg
            { Factory = currentReg.Factory, Lifetime = currentReg.Lifetime, SingletonId = currentReg.SingletonId };
        multi.Multi.Add(currentReg);
        multi.Add(registration, preserveExistingDefaults);
        _registrations[asType] = multi;
    }
}
