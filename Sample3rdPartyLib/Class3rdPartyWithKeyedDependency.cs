using Microsoft.Extensions.DependencyInjection;

namespace Sample3rdPartyLib;

public class Class3rdPartyWithKeyedDependency : I3rdPartyInterface
{
    readonly I3rdPartyInterface _interface;

    public Class3rdPartyWithKeyedDependency([FromKeyedServices(ServiceKeys.Key1)] I3rdPartyInterface dependency)
    {
        _interface = dependency;
    }

    public string Name => _interface.Name;
} 