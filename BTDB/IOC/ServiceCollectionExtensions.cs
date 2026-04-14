using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace BTDB.IOC;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection UseBtdbIoc(this IServiceCollection services, ContainerBuilder containerBuilder,
        ContainerVerification options = ContainerVerification.AllTypesAreGenerated)
    {
        var builderServices = containerBuilder.GetServiceCollection();
        if (builderServices != null)
        {
            foreach (var descriptor in builderServices)
            {
                services.Add(descriptor);
            }
        }

        var integration = new ServiceProviderIntegration();
        var exports = containerBuilder.CollectServiceCollectionRegistrations().Registrations;
        integration.RegisterExternalServices(services, exports, (serviceProvider, serviceProviderIntegration) =>
        {
            containerBuilder.SetServiceProvider(serviceProvider, serviceProviderIntegration);
            return (ContainerImpl)containerBuilder.BuildAndVerify(options);
        });
        return services;
    }
}
