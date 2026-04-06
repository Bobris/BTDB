using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB;
using BTDB.IOC;
using BTDBTest.IOCDomain;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BTDBTest;

public partial class IocTests
{
    [Generate]
    public class ExportedScopedBtDbService;

    [Generate]
    public class BtDbServiceWithMsDiScopedDependency(ScopedMsDiService dependency)
    {
        public ScopedMsDiService Dependency { get; } = dependency;
    }

    [Generate]
    public class CyclicBtDbService(CyclicMsDiService dependency)
    {
        public CyclicMsDiService Dependency { get; } = dependency;
    }

    public sealed class CyclicMsDiService(CyclicBtDbService dependency)
    {
        public CyclicBtDbService Dependency { get; } = dependency;
    }

    [Fact]
    public void ExportedSingletonCanBeResolvedFromServiceProvider()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<TestLogger>().As<ILogger>().SingleInstance();

        var container = builder.Build();
        var serviceProvider = container.Resolve<IServiceProvider>();

        var logger1 = serviceProvider.GetRequiredService<ILogger>();
        var logger2 = serviceProvider.GetRequiredService<ILogger>();

        Assert.IsType<TestLogger>(logger1);
        Assert.Same(logger1, logger2);
        Assert.Same(container.Resolve<ILogger>(), logger1);
    }

    [Fact]
    public void ExportedRegistrationsPreserveEnumerableOrderAndLastWins()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<TestLogger>().As<ILogger>();
        builder.RegisterType<TestLogger2>().As<ILogger>();

        var container = builder.Build();
        var serviceProvider = container.Resolve<IServiceProvider>();

        var all = serviceProvider.GetServices<ILogger>().Select(logger => logger.GetType().Name).ToArray();

        Assert.Equal(new[] { nameof(TestLogger), nameof(TestLogger2) }, all);
        Assert.IsType<TestLogger2>(serviceProvider.GetRequiredService<ILogger>());
    }

    [Fact]
    public void ExportedKeyedRegistrationsCanBeResolvedFromServiceProvider()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<TestLogger>().Keyed<ILogger>("A");
        builder.RegisterType<TestLogger2>().Keyed<ILogger>("A");

        var container = builder.Build();
        var serviceProvider = container.Resolve<IServiceProvider>();

        var all = serviceProvider.GetKeyedServices<ILogger>("A").Select(logger => logger.GetType().Name).ToArray();

        Assert.Equal(new[] { nameof(TestLogger), nameof(TestLogger2) }, all);
        Assert.IsType<TestLogger2>(serviceProvider.GetRequiredKeyedService<ILogger>("A"));
    }

    [Fact]
    public async Task ServiceProviderResolvesBtDbContainerPerScope()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<TestLogger>().As<ILogger>();
        var container = builder.Build();
        var serviceProvider = container.Resolve<IServiceProvider>();

        Assert.Same(container, serviceProvider.GetRequiredService<IContainer>());

        await using var scope = serviceProvider.CreateAsyncScope();
        var scopedContainer1 = scope.ServiceProvider.GetRequiredService<IContainer>();
        var scopedContainer2 = scope.ServiceProvider.GetRequiredService<IContainer>();

        Assert.NotSame(container, scopedContainer1);
        Assert.Same(scopedContainer1, scopedContainer2);
    }

    [Fact]
    public async Task ExportedScopedServicesFollowServiceProviderScopes()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<ExportedScopedBtDbService>().AsSelf().Scoped();

        var container = builder.Build();
        var serviceProvider = container.Resolve<IServiceProvider>();

        var root1 = serviceProvider.GetRequiredService<ExportedScopedBtDbService>();
        var root2 = serviceProvider.GetRequiredService<ExportedScopedBtDbService>();
        Assert.Same(root1, root2);

        await using var scope1 = serviceProvider.CreateAsyncScope();
        var scope1Obj1 = scope1.ServiceProvider.GetRequiredService<ExportedScopedBtDbService>();
        var scope1Obj2 = scope1.ServiceProvider.GetRequiredService<ExportedScopedBtDbService>();
        Assert.Same(scope1Obj1, scope1Obj2);
        Assert.NotSame(root1, scope1Obj1);

        await using var scope2 = serviceProvider.CreateAsyncScope();
        var scope2Obj = scope2.ServiceProvider.GetRequiredService<ExportedScopedBtDbService>();
        Assert.NotSame(scope1Obj1, scope2Obj);
        Assert.NotSame(root1, scope2Obj);
    }

    [Fact]
    public async Task ExportedBtDbServicesCanResolveScopedDependenciesFromServiceProvider()
    {
        var builder = new ContainerBuilder();
        builder.ServiceCollection.AddScoped<ScopedMsDiService>();
        builder.RegisterType<BtDbServiceWithMsDiScopedDependency>().AsSelf().Scoped();

        var container = builder.Build();
        var serviceProvider = container.Resolve<IServiceProvider>();

        await using var scope1 = serviceProvider.CreateAsyncScope();
        var scope1Obj1 = scope1.ServiceProvider.GetRequiredService<BtDbServiceWithMsDiScopedDependency>();
        var scope1Obj2 = scope1.ServiceProvider.GetRequiredService<BtDbServiceWithMsDiScopedDependency>();
        Assert.Same(scope1Obj1, scope1Obj2);
        Assert.Same(scope1Obj1.Dependency, scope1Obj2.Dependency);

        await using var scope2 = serviceProvider.CreateAsyncScope();
        var scope2Obj = scope2.ServiceProvider.GetRequiredService<BtDbServiceWithMsDiScopedDependency>();
        Assert.NotSame(scope1Obj1, scope2Obj);
        Assert.NotSame(scope1Obj1.Dependency, scope2Obj.Dependency);
    }

    [Fact]
    public void BtDbPrefersLocalRegistrationsOverServiceProviderFallback()
    {
        var builder = new ContainerBuilder();
        builder.ServiceCollection.AddSingleton<ILogger, TestLogger2>();
        builder.RegisterType<TestLogger>().As<ILogger>();

        var container = builder.Build();

        Assert.IsType<TestLogger>(container.Resolve<ILogger>());
        Assert.Equal(new[] { nameof(TestLogger) },
            container.Resolve<IEnumerable<ILogger>>().Select(logger => logger.GetType().Name).ToArray());
    }

    [Fact]
    public void CrossContainerCycleIsDetectedWhenResolvingFromServiceProvider()
    {
        var builder = new ContainerBuilder();
        builder.ServiceCollection.AddTransient<CyclicMsDiService>();
        builder.RegisterType<CyclicBtDbService>().AsSelf();

        var container = builder.Build();
        var serviceProvider = container.Resolve<IServiceProvider>();

        var exception = Assert.Throws<InvalidOperationException>(() => serviceProvider.GetRequiredService<CyclicBtDbService>());

        Assert.Contains("Detected circular dependency between BTDB IOC and IServiceProvider", exception.Message);
    }

    [Fact]
    public void CrossContainerCycleIsDetectedWhenResolvingFromBtDb()
    {
        var builder = new ContainerBuilder();
        builder.ServiceCollection.AddTransient<CyclicMsDiService>();
        builder.RegisterType<CyclicBtDbService>().AsSelf();

        var container = builder.Build();

        var exception = Assert.Throws<InvalidOperationException>(() => container.Resolve<CyclicBtDbService>());

        Assert.Contains("Detected circular dependency between BTDB IOC and IServiceProvider", exception.Message);
    }

    [Fact]
    public void UseBtdbIocMakesBtDbRegistrationsAvailableFromAspNetServiceProvider()
    {
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterType<TestLogger>().As<ILogger>().SingleInstance();
        containerBuilder.RegisterType<ErrorHandler>().As<IErrorHandler>();

        var services = new ServiceCollection();
        services.UseBtdbIoc(containerBuilder);
        var serviceProvider = services.BuildServiceProvider();

        var logger = serviceProvider.GetRequiredService<ILogger>();
        var errorHandler = serviceProvider.GetRequiredService<IErrorHandler>();
        var container = serviceProvider.GetRequiredService<IContainer>();

        Assert.IsType<TestLogger>(logger);
        Assert.IsType<ErrorHandler>(errorHandler);
        Assert.Same(logger, errorHandler.Logger);
        Assert.Same(container.Resolve<ILogger>(), logger);
    }

    [Fact]
    public async Task UseBtdbIocSharesAspNetScopesWithBtDb()
    {
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterType<ExportedScopedBtDbService>().AsSelf().Scoped();

        var services = new ServiceCollection();
        services.AddScoped<ScopedMsDiService>();
        services.UseBtdbIoc(containerBuilder);
        var serviceProvider = services.BuildServiceProvider();

        Assert.NotNull(serviceProvider.GetRequiredService<IContainer>());

        await using var scope1 = serviceProvider.CreateAsyncScope();
        var scope1Container = scope1.ServiceProvider.GetRequiredService<IContainer>();
        var scope1BtDb1 = scope1.ServiceProvider.GetRequiredService<ExportedScopedBtDbService>();
        var scope1BtDb2 = scope1.ServiceProvider.GetRequiredService<ExportedScopedBtDbService>();
        var scope1MsDi1 = scope1Container.Resolve<ScopedMsDiService>();
        var scope1MsDi2 = scope1.ServiceProvider.GetRequiredService<ScopedMsDiService>();
        Assert.Same(scope1BtDb1, scope1BtDb2);
        Assert.Same(scope1MsDi1, scope1MsDi2);

        await using var scope2 = serviceProvider.CreateAsyncScope();
        var scope2Container = scope2.ServiceProvider.GetRequiredService<IContainer>();
        var scope2BtDb = scope2.ServiceProvider.GetRequiredService<ExportedScopedBtDbService>();
        var scope2MsDi = scope2Container.Resolve<ScopedMsDiService>();
        Assert.NotSame(scope1Container, scope2Container);
        Assert.NotSame(scope1BtDb1, scope2BtDb);
        Assert.NotSame(scope1MsDi1, scope2MsDi);
    }

    [Fact]
    public void UseBtdbIocDetectsCrossContainerCycle()
    {
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterType<CyclicBtDbService>().AsSelf();

        var services = new ServiceCollection();
        services.AddTransient<CyclicMsDiService>();
        services.UseBtdbIoc(containerBuilder);
        var serviceProvider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(() => serviceProvider.GetRequiredService<CyclicBtDbService>());

        Assert.Contains("Detected circular dependency between BTDB IOC and IServiceProvider", exception.Message);
    }

    [Fact]
    public void UseBtdbIocSetsBuiltAspNetServiceProviderBackToContainerBuilder()
    {
        var containerBuilder = new ContainerBuilder();
        var services = new ServiceCollection();
        services.AddSingleton<ScopedMsDiService>();
        services.UseBtdbIoc(containerBuilder);
        var serviceProvider = services.BuildServiceProvider();

        _ = serviceProvider.GetRequiredService<IContainer>();
        var container = containerBuilder.Build();

        Assert.Same(serviceProvider.GetRequiredService<ScopedMsDiService>(), container.Resolve<ScopedMsDiService>());
    }
}
