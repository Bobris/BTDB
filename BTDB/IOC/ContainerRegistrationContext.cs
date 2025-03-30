using System.Collections.Generic;
using BTDB.KVDBLayer;

namespace BTDB.IOC;

class ContainerRegistrationContext
{
    readonly Dictionary<KeyAndType, CReg> _registrations;

    internal ContainerRegistrationContext(Dictionary<KeyAndType, CReg> registrations, bool allowReflectionFallback,
        bool reportNotGeneratedTypes)
    {
        _registrations = registrations;
        AllowReflectionFallback = allowReflectionFallback;
        ReportNotGeneratedTypes = reportNotGeneratedTypes;
    }

    internal bool ReportNotGeneratedTypes { get; private set; }

    internal uint SingletonCount { get; private set; }
    internal bool AllowReflectionFallback { get; private set; }

    public void AddCReg(IEnumerable<KeyAndType> asTypes, bool preserveExistingDefaults, bool uniqueRegistration,
        CReg registration)
    {
        foreach (var asType in asTypes)
        {
            AddCReg(asType, preserveExistingDefaults, uniqueRegistration, registration);
        }
    }

    public void AddCReg(KeyAndType asType, bool preserveExistingDefaults, bool uniqueRegistration, CReg registration)
    {
        if (registration.Lifetime == Lifetime.Singleton && registration.SingletonId == uint.MaxValue)
        {
            registration.SingletonId = SingletonCount++;
        }

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
            return;
        }

        var multi = new CReg
            { Factory = currentReg.Factory, Lifetime = currentReg.Lifetime, SingletonId = currentReg.SingletonId };
        multi.Multi.Add(currentReg);
        multi.Add(registration, preserveExistingDefaults);
        _registrations[asType] = multi;
    }
}
