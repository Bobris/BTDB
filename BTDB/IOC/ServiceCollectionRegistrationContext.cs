using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace BTDB.IOC;

readonly record struct ServiceCollectionExport(KeyAndType Service, ServiceLifetime Lifetime, int RegistrationIndex);

class ServiceCollectionRegistrationContext
{
    readonly Dictionary<KeyAndType, int> _registrationCounts = new();
    readonly List<ServiceCollectionExport> _registrations = new();

    public IReadOnlyList<ServiceCollectionExport> Registrations => _registrations;

    public void Add(IEnumerable<KeyAndType> asTypes, ServiceLifetime lifetime)
    {
        foreach (var asType in asTypes)
        {
            Add(asType, lifetime);
        }
    }

    public void Add(KeyAndType asType, ServiceLifetime lifetime)
    {
        _registrationCounts.TryGetValue(asType, out var registrationIndex);
        _registrationCounts[asType] = registrationIndex + 1;
        _registrations.Add(new(asType, lifetime, registrationIndex));
    }
}

static class LifetimeExtensions
{
    public static ServiceLifetime ToServiceLifetime(this Lifetime lifetime)
    {
        return lifetime switch
        {
            Lifetime.Singleton => ServiceLifetime.Singleton,
            Lifetime.Scoped => ServiceLifetime.Scoped,
            _ => ServiceLifetime.Transient
        };
    }
}
