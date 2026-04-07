using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace BTDB.IOC;

sealed class ServiceProviderIntegration
{
    readonly AsyncLocal<List<ResolutionRequest>?> _resolutionStack = new();
    ContainerImpl? _rootContainer;

    readonly record struct ResolutionRequest(Type Type, object? Key);

    sealed class RootScopeIdentity(IServiceProvider serviceProvider)
    {
        public IServiceProvider ServiceProvider { get; } = serviceProvider;
    }

    sealed class ResolutionGuard(ServiceProviderIntegration owner) : IDisposable
    {
        public void Dispose()
        {
            var stack = owner._resolutionStack.Value;
            if (stack == null || stack.Count == 0) return;
            stack.RemoveAt(stack.Count - 1);
            if (stack.Count == 0)
            {
                owner._resolutionStack.Value = null;
            }
        }
    }

    sealed class ContainerScope(IServiceProvider serviceProvider, RootScopeIdentity rootScopeIdentity,
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

    ContainerImpl RootContainer => _rootContainer ?? throw new InvalidOperationException("BTDB container is not initialized.");

    public void Initialize(ContainerImpl rootContainer)
    {
        _rootContainer = rootContainer;
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
            var rootContainer = rootContainerFactory(sp.GetRequiredService<RootScopeIdentity>().ServiceProvider,
                integration);
            integration.Initialize(rootContainer);
            return rootContainer;
        });
    }

    void RegisterCommonServices(IServiceCollection services, IReadOnlyList<ServiceCollectionExport> exports,
        Func<IServiceProvider, ContainerImpl> rootContainerFactory)
    {
        services.AddSingleton(this);
        services.AddSingleton<RootScopeIdentity>(static sp => new(sp));
        services.AddSingleton(rootContainerFactory);
        services.AddScoped<ContainerScope>(static sp =>
            new(sp, sp.GetRequiredService<RootScopeIdentity>(), sp.GetRequiredService<ContainerImpl>()));
        services.AddScoped<IContainer>(static sp => sp.GetRequiredService<ContainerScope>().Container);
        AddExports(services, exports);
    }

    void AddExports(IServiceCollection services, IReadOnlyList<ServiceCollectionExport> exports)
    {
        foreach (var export in exports)
        {
            if (export.Service.Key == null)
            {
                ((ICollection<ServiceDescriptor>)services).Add(ServiceDescriptor.Describe(export.Service.Type,
                    sp => sp.GetRequiredService<ServiceProviderIntegration>()
                        .ResolveFromContainer(sp, export.Service.Type, null, export.RegistrationIndex), export.Lifetime));
            }
            else
            {
                ((ICollection<ServiceDescriptor>)services).Add(ServiceDescriptor.DescribeKeyed(export.Service.Type, export.Service.Key,
                    (sp, serviceKey) => sp.GetRequiredService<ServiceProviderIntegration>()
                        .ResolveFromContainer(sp, export.Service.Type, serviceKey, export.RegistrationIndex), export.Lifetime));
            }
        }
    }

    public object ResolveRequiredFromServiceProvider(IServiceProvider serviceProvider, Type type, object? key)
    {
        using var _ = EnterResolution(type, key);
        return key == null
            ? serviceProvider.GetRequiredService(type)
            : serviceProvider.GetRequiredKeyedService(type, key);
    }

    public object ResolveFromContainer(IServiceProvider serviceProvider, Type type, object? key, int registrationIndex)
    {
        using var _ = EnterResolution(type, key);
        var rootScopeIdentity = serviceProvider.GetRequiredService<RootScopeIdentity>();
        var container = ReferenceEquals(serviceProvider, rootScopeIdentity.ServiceProvider)
            ? serviceProvider.GetRequiredService<ContainerImpl>()
            : serviceProvider.GetRequiredService<ContainerScope>().Container;
        return container.ResolveRegistration(type, key, registrationIndex);
    }

    IDisposable EnterResolution(Type type, object? key)
    {
        var stack = _resolutionStack.Value ??= new();
        var current = new ResolutionRequest(type, key);
        foreach (var item in stack)
        {
            if (item.Type == current.Type && Equals(item.Key, current.Key))
            {
                throw new InvalidOperationException(
                    $"Detected circular dependency between BTDB IOC and IServiceProvider while resolving {Format(current)}. Resolution chain: {Format(stack)}");
            }
        }

        stack.Add(current);
        return new ResolutionGuard(this);
    }

    static string Format(IReadOnlyList<ResolutionRequest> stack)
    {
        var builder = new StringBuilder();
        for (var i = 0; i < stack.Count; i++)
        {
            if (i != 0) builder.Append(" -> ");
            builder.Append(Format(stack[i]));
        }

        return builder.ToString();
    }

    static string Format(ResolutionRequest request)
    {
        return request.Key == null ? request.Type.ToString() : $"{request.Type} with key {request.Key}";
    }
}
