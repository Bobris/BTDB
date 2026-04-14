using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace BTDB.IOC;

sealed class ServiceProviderIntegration
{
    ContainerImpl? _rootContainer;
    IServiceProvider? _rootServiceProvider;
    IReadOnlyList<ServiceCollectionExport>? _exports;
    Func<IContainer, IResolvingCtx?, object?>?[]? _exportFactories;

    sealed class RootScopeIdentity(IServiceProvider serviceProvider)
    {
        public IServiceProvider ServiceProvider { get; } = serviceProvider;
    }

    sealed class ContainerScope(
        IServiceProvider serviceProvider,
        RootScopeIdentity rootScopeIdentity,
        ContainerImpl rootContainer) : IAsyncDisposable
    {
        readonly IContainer? _ownedContainer =
            ReferenceEquals(serviceProvider, rootScopeIdentity.ServiceProvider)
                ? null
                : rootContainer.CreateScopeForServiceProvider(serviceProvider);

        public ContainerImpl Container => (ContainerImpl)(_ownedContainer ?? rootContainer);

        public ValueTask DisposeAsync()
        {
            return _ownedContainer?.DisposeAsync() ?? default;
        }
    }

    ContainerImpl RootContainer =>
        _rootContainer ?? throw new InvalidOperationException("BTDB container is not initialized.");

    public void Initialize(ContainerImpl rootContainer, IServiceProvider rootServiceProvider)
    {
        _rootContainer = rootContainer;
        _rootServiceProvider = rootServiceProvider;
        var exports = _exports;
        _exportFactories = exports == null || exports.Count == 0
            ? []
            : new Func<IContainer, IResolvingCtx?, object?>?[exports.Count];
    }

    public void RegisterServices(ServiceCollection serviceCollection, IReadOnlyList<ServiceCollectionExport> exports)
    {
        RegisterCommonServices(serviceCollection, exports,
            sp => sp.GetRequiredService<ServiceProviderIntegration>().RootContainer);
    }

    public void RegisterExternalServices(IServiceCollection services, IReadOnlyList<ServiceCollectionExport> exports,
        Func<IServiceProvider, ServiceProviderIntegration, ContainerImpl> rootContainerFactory)
    {
        RegisterCommonServices(services, exports, sp =>
        {
            var integration = sp.GetRequiredService<ServiceProviderIntegration>();
            var rootServiceProvider = sp.GetRequiredService<RootScopeIdentity>().ServiceProvider;
            var rootContainer = rootContainerFactory(rootServiceProvider, integration);
            integration.Initialize(rootContainer, rootServiceProvider);
            return rootContainer;
        });
    }

    void RegisterCommonServices(IServiceCollection services, IReadOnlyList<ServiceCollectionExport> exports,
        Func<IServiceProvider, ContainerImpl> rootContainerFactory)
    {
        _exports = exports;
        services.AddSingleton(this);
        services.AddSingleton<RootScopeIdentity>(static sp => new(sp));
        services.AddSingleton(rootContainerFactory);
        services.AddSingleton<IRootContainer>(static sp => sp.GetRequiredService<ContainerImpl>());
        services.AddScoped<ContainerScope>(static sp =>
            new(sp, sp.GetRequiredService<RootScopeIdentity>(), sp.GetRequiredService<ContainerImpl>()));
        services.AddScoped<IContainer>(static sp => sp.GetRequiredService<ContainerScope>().Container);
        AddExports(services, exports);
    }

    void AddExports(IServiceCollection services, IReadOnlyList<ServiceCollectionExport> exports)
    {
        for (var exportIndex = 0; exportIndex < exports.Count; exportIndex++)
        {
            var export = exports[exportIndex];
            var currentExportIndex = exportIndex;
            if (export.Service.Key == null)
            {
                ((ICollection<ServiceDescriptor>)services).Add(ServiceDescriptor.Describe(export.Service.Type,
                    sp => sp.GetRequiredService<ServiceProviderIntegration>()
                        .ResolveFromContainer(sp, currentExportIndex), export.Lifetime));
            }
            else
            {
                ((ICollection<ServiceDescriptor>)services).Add(ServiceDescriptor.DescribeKeyed(export.Service.Type,
                    export.Service.Key,
                    (sp, serviceKey) => sp.GetRequiredService<ServiceProviderIntegration>()
                        .ResolveFromContainer(sp, currentExportIndex), export.Lifetime));
            }
        }
    }

    public object ResolveFromContainer(IServiceProvider serviceProvider, int exportIndex)
    {
        var rootServiceProvider = _rootServiceProvider;
        if (rootServiceProvider == null)
        {
            rootServiceProvider = serviceProvider.GetRequiredService<RootScopeIdentity>().ServiceProvider;
            _rootServiceProvider = rootServiceProvider;
        }

        var container = ReferenceEquals(serviceProvider, rootServiceProvider)
            ? _rootContainer ??= serviceProvider.GetRequiredService<ContainerImpl>()
            : serviceProvider.GetRequiredService<ContainerScope>().Container;
        return GetOrCreateExportFactory(serviceProvider, exportIndex)(container, null)!;
    }

    Func<IContainer, IResolvingCtx?, object?> GetOrCreateExportFactory(IServiceProvider serviceProvider,
        int exportIndex)
    {
        var exportFactories = _exportFactories;
        if (exportFactories == null)
        {
            throw new InvalidOperationException("BTDB export factories are not initialized.");
        }

        var factory = Volatile.Read(ref exportFactories[exportIndex]);
        if (factory != null) return factory;

        var rootContainer = _rootContainer ?? serviceProvider.GetRequiredService<ContainerImpl>();
        var exports = _exports ?? throw new InvalidOperationException("BTDB exports are not initialized.");
        var export = exports[exportIndex];
        factory = rootContainer.GetRegistrationFactory(export.Service.Type, export.Service.Key,
            export.RegistrationIndex);
        return Interlocked.CompareExchange(ref exportFactories[exportIndex], factory, null) ?? factory;
    }
}
