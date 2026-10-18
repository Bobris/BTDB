using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB;
using BTDB.IOC;
using BTDB.KVDBLayer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BTDBTest;

// Types in this file are intentionally NOT marked [Generate], so they exercise the reflection fallback.
public interface ICovService
{
}

public class CovService : ICovService
{
}

public class CovLogger
{
}

public class CovUnregistered
{
}

public class CovThree(CovService a, CovLogger b, ICovService c)
{
    public CovService A { get; } = a;
    public CovLogger B { get; } = b;
    public ICovService C { get; } = c;
}

public class CovFour(CovService a, CovLogger b, ICovService c, CovLogger d)
{
    public object[] All { get; } = [a, b, c, d];
}

public class CovOptional(CovUnregistered? missing = null, string text = "dflt")
{
    public CovUnregistered? Missing { get; } = missing;
    public string Text { get; } = text;
}

public class CovWithDependencies(CovService service)
{
    public CovService Service { get; } = service;
    [Dependency] public CovLogger? Logger { get; set; }
    [Dependency("named")] public ICovService? Named { get; set; }
    [Dependency] public CovUnregistered? Missing { get; set; }
    [Dependency] public CovLogger? ReadOnly => null;
    public CovLogger? NotMarked { get; set; }
}

public class CovNeedsScoped(CovLogger logger)
{
    public CovLogger Logger { get; } = logger;
}

public sealed class CovDisposable : IDisposable
{
    public bool Disposed { get; private set; }
    public void Dispose() => Disposed = true;
}

public class CovFirstOfMany : ICovService
{
}

public class CovSecondOfMany : ICovService
{
}

[Generate]
public class CovGeneratedForScan
{
}

public class IOCCoverageTest
{
    [Fact]
    public void FallbackFactoryHandlesThreeAndMoreParameters()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback<CovService>().AsSelf().As<ICovService>().SingleInstance();
        builder.RegisterTypeWithFallback<CovLogger>().AsSelf();
        builder.RegisterTypeWithFallback<CovThree>().AsSelf();
        builder.RegisterTypeWithFallback<CovFour>().AsSelf();
        var container = builder.Build();
        var three = container.Resolve<CovThree>();
        Assert.Same(three.A, three.C);
        Assert.NotNull(three.B);
        var four = container.Resolve<CovFour>();
        Assert.Equal(4, four.All.Length);
        Assert.All(four.All, Assert.NotNull);
        Assert.NotSame(four.All[1], four.All[3]);
    }

    [Fact]
    public void FallbackFactoryUsesDefaultValuesForUnresolvableOptionalParameters()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback<CovOptional>().AsSelf();
        var obj = builder.Build().Resolve<CovOptional>();
        Assert.Null(obj.Missing);
        Assert.Equal("dflt", obj.Text);
    }

    [Fact]
    public void FallbackFactoryInjectsDependencyProperties()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback<CovService>().AsSelf();
        builder.RegisterTypeWithFallback<CovLogger>().AsSelf();
        builder.RegisterTypeWithFallback<CovFirstOfMany>().Named<IAsLiveScopeTrait>("named", typeof(ICovService));
        builder.RegisterTypeWithFallback<CovWithDependencies>().AsSelf();
        var obj = builder.Build().Resolve<CovWithDependencies>();
        Assert.NotNull(obj.Service);
        Assert.NotNull(obj.Logger);
        Assert.IsType<CovFirstOfMany>(obj.Named);
        Assert.Null(obj.Missing);
        Assert.Null(obj.NotMarked);
    }

    [Fact]
    public void FallbackFactoryWithoutUsableConstructorReportsTypeName()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback(typeof(ICovService)).AsSelf();
        var ex = Assert.Throws<BTDBException>(() => builder.Build());
        Assert.Contains("Cannot create factory for BTDBTest.ICovService", ex.Message);
        Assert.NotNull(ex.InnerException);
    }

    [Fact]
    public void NotGeneratedTypeWithoutFallbackIsRejectedOrOnlyReported()
    {
        var builder = new ContainerBuilder();
        builder.RegisterType<CovUnregistered>().AsSelf();
        var ex = Assert.Throws<BTDBException>(() => builder.Build());
        Assert.Contains("Factory for BTDBTest.CovUnregistered is not registered", ex.Message);
        var reported = builder.BuildAndVerify(ContainerVerification.ReportNotGeneratedTypes);
        Assert.Null(reported.ResolveOptional<CovUnregistered>());
    }

    [Fact]
    public void UniqueRegistrationsBehaviourAppliesToAllRegistrationKinds()
    {
        var builder = new ContainerBuilder(ContainerBuilderBehaviour.UniqueRegistrations);
        builder.RegisterTypeWithFallback<CovService>().As<ICovService>();
        builder.RegisterInstance(new CovFirstOfMany()).As<ICovService>();
        Assert.Throws<BTDBException>(() => builder.Build());

        builder = new ContainerBuilder(ContainerBuilderBehaviour.UniqueRegistrations);
        builder.RegisterInstance<ICovService>(new CovFirstOfMany()).As<ICovService>();
        builder.RegisterInstance<ICovService>(new CovSecondOfMany()).As<ICovService>();
        var ex = Assert.Throws<BTDBException>(() => builder.Build());
        Assert.Contains("is not unique", ex.Message);

        builder = new ContainerBuilder(ContainerBuilderBehaviour.UniqueRegistrations);
        var instance = new CovFirstOfMany();
        builder.RegisterInstance<ICovService>(instance);
        Assert.Same(instance, builder.Build().Resolve<ICovService>());
    }

    [Fact]
    public void KeyedAndNamedRegistrationsResolveByKey()
    {
        var builder = new ContainerBuilder();
        builder.RegisterInstance(new CovFirstOfMany()).Keyed(1, typeof(ICovService));
        builder.RegisterInstance(new CovSecondOfMany()).Named("second", typeof(ICovService));
        var container = builder.Build();
        Assert.IsType<CovFirstOfMany>(container.ResolveKeyed<ICovService>(1));
        Assert.IsType<CovSecondOfMany>(container.ResolveNamed<ICovService>("second"));
        Assert.Null(container.ResolveOptionalKeyed<ICovService>(2));
        var ex = Assert.Throws<ArgumentException>(() => container.ResolveKeyed<ICovService>(3));
        Assert.Equal("Type BTDBTest.ICovService with key 3 cannot be resolved", ex.Message);
        ex = Assert.Throws<ArgumentException>(() => container.Resolve<CovUnregistered>());
        Assert.Equal("Type BTDBTest.CovUnregistered cannot be resolved", ex.Message);
        Assert.Throws<ArgumentException>(() => ((ContainerImpl)container).CreateFactory(typeof(CovUnregistered)));
        Assert.NotNull(((ContainerImpl)container).CreateFactory(typeof(IContainer)));
    }

    [Fact]
    public void ObsoleteFactoryRegistrationsWork()
    {
        var builder = new ContainerBuilder();
        builder.RegisterFactory<CovLogger>(_ => new CovLogger()).AsSelf().SingleInstance();
        builder.RegisterFactory(c => new CovThree(new CovService(), c.Resolve<CovLogger>(), new CovService()),
            typeof(CovThree)).AsSelf();
        var container = builder.Build();
        Assert.Same(container.Resolve<CovLogger>(), container.Resolve<CovThree>().B);
    }

    [Fact]
    public void AssemblyScanningSupportsAllAssemblyParameterForms()
    {
        static bool Filter(Type t) => t == typeof(CovFirstOfMany) || t == typeof(CovSecondOfMany);
        var assembly = typeof(IOCCoverageTest).Assembly;
        var otherAssembly = typeof(ContainerBuilder).Assembly;
        void Check(Action<ContainerBuilder> register, bool singleton)
        {
            var builder = new ContainerBuilder();
            register(builder);
            var container = builder.BuildAndVerify(ContainerVerification.None);
            var all = container.Resolve<IEnumerable<ICovService>>().Select(s => s.GetType()).OrderBy(t => t.Name)
                .ToArray();
            Assert.Equal([typeof(CovFirstOfMany), typeof(CovSecondOfMany)], all);
            Assert.Equal(singleton, ReferenceEquals(container.Resolve<CovFirstOfMany>(),
                container.Resolve<CovFirstOfMany>()));
        }

        Check(b => b.RegisterAssemblyTypes(assembly).Where(Filter).AsImplementedInterfaces().AsSelf(), false);
        Check(b => b.RegisterAssemblyTypes(new[] { assembly }).Where(Filter).AsImplementedInterfaces().AsSelf()
            .SingleInstance(), true);
        Check(b => b.RegisterAssemblyTypes(assembly, otherAssembly).Where(Filter).AsImplementedInterfaces().AsSelf(),
            false);
        Check(b => b.RegisterAssemblyTypes((ReadOnlySpan<System.Reflection.Assembly>)[assembly]).Where(Filter)
            .AsImplementedInterfaces().AsSelf(), false);
        Check(b => b.RegisterAssemblyTypes((ReadOnlySpan<System.Reflection.Assembly>)[assembly, otherAssembly])
            .Where(Filter).AsImplementedInterfaces().AsSelf().Scoped(), true);
    }

    [Fact]
    public void EmptyAssemblySpanScansGeneratedTypes()
    {
        var builder = new ContainerBuilder();
        builder.RegisterAssemblyTypes(ReadOnlySpan<System.Reflection.Assembly>.Empty)
            .Where(t => t == typeof(CovGeneratedForScan)).AsSelf();
        Assert.NotNull(builder.Build().Resolve<CovGeneratedForScan>());
    }

    [Fact]
    public async Task AssemblyScanningExportsToServiceCollection()
    {
        static bool Filter(Type t) => t == typeof(CovFirstOfMany) || t == typeof(CovSecondOfMany);
        var assembly = typeof(IOCCoverageTest).Assembly;
        foreach (var register in new Action<ContainerBuilder>[]
                 {
                     b => b.RegisterAssemblyTypes(assembly).Where(Filter).AsImplementedInterfaces(),
                     b => b.RegisterAssemblyTypes(assembly, typeof(ContainerBuilder).Assembly).Where(Filter)
                         .AsImplementedInterfaces().SingleInstance(),
                 })
        {
            var containerBuilder = new ContainerBuilder();
            register(containerBuilder);
            containerBuilder.ServiceCollection.AddSingleton(new CovLogger());
            var services = new ServiceCollection();
            services.UseBtdbIoc(containerBuilder, ContainerVerification.None);
            await using var serviceProvider = services.BuildServiceProvider();
            var all = serviceProvider.GetServices<ICovService>().Select(s => s.GetType()).OrderBy(t => t.Name)
                .ToArray();
            Assert.Equal([typeof(CovFirstOfMany), typeof(CovSecondOfMany)], all);
            Assert.NotNull(serviceProvider.GetRequiredService<CovLogger>());
        }

        var autoBuilder = new ContainerBuilder();
        autoBuilder.AutoRegisterTypes().Where(t => t == typeof(CovGeneratedForScan)).AsSelf().Scoped();
        var autoServices = new ServiceCollection();
        autoServices.UseBtdbIoc(autoBuilder);
        await using var autoProvider = autoServices.BuildServiceProvider();
        await using var scope = autoProvider.CreateAsyncScope();
        Assert.Same(scope.ServiceProvider.GetRequiredService<CovGeneratedForScan>(),
            scope.ServiceProvider.GetRequiredService<CovGeneratedForScan>());
    }

    [Fact]
    public async Task FactoryAndInstanceRegistrationsExportToServiceCollection()
    {
        var containerBuilder = new ContainerBuilder();
        var instance = new CovFirstOfMany();
        containerBuilder.RegisterInstance(instance).As<ICovService>();
        containerBuilder.RegisterFactory<CovLogger>(_ => new CovLogger()).AsSelf().Scoped();
        var services = new ServiceCollection();
        services.UseBtdbIoc(containerBuilder);
        await using var serviceProvider = services.BuildServiceProvider();
        Assert.Same(instance, serviceProvider.GetRequiredService<ICovService>());
        await using var scope = serviceProvider.CreateAsyncScope();
        Assert.Same(scope.ServiceProvider.GetRequiredService<CovLogger>(),
            scope.ServiceProvider.GetRequiredService<CovLogger>());
    }

    [Fact]
    public void TupleResolutionSupportsUpToThreeReferenceTypes()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback<CovService>().AsSelf();
        builder.RegisterTypeWithFallback<CovLogger>().AsSelf().SingleInstance();
        var container = builder.Build();
        var t1 = container.Resolve<Tuple<CovService>>();
        Assert.NotNull(t1.Item1);
        var t3 = container.Resolve<Tuple<CovService, CovLogger, CovLogger>>();
        Assert.NotNull(t3.Item1);
        Assert.Same(t3.Item2, t3.Item3);
        Assert.Null(container.ResolveOptional<Tuple<CovUnregistered>>());
        Assert.Null(container.ResolveOptional<Tuple<CovUnregistered, CovService>>());
        Assert.Null(container.ResolveOptional<Tuple<CovService, CovUnregistered>>());
        Assert.Null(container.ResolveOptional<Tuple<CovUnregistered, CovService, CovService>>());
        Assert.Null(container.ResolveOptional<Tuple<CovService, CovUnregistered, CovService>>());
        Assert.Null(container.ResolveOptional<Tuple<CovService, CovService, CovUnregistered>>());
        Assert.Throws<NotSupportedException>(() => container.Resolve<Tuple<int>>());
        Assert.Throws<NotSupportedException>(() => container.Resolve<Tuple<CovService, CovService, Guid>>());
    }

    [Fact]
    public void FuncAndCollectionEdgeCases()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback<CovService>().AsSelf();
        var container = builder.Build();
        var withContainer = container.Resolve<Func<IContainer, CovService>>();
        Assert.NotNull(withContainer(container));
        Assert.Null(container.ResolveOptional<Func<IContainer, CovUnregistered>>());
        Assert.Null(container.ResolveOptional<Func<CovLogger, CovUnregistered>>());
        Assert.Throws<NotSupportedException>(() => container.Resolve<Func<int, CovService>>());
        Assert.Throws<NotSupportedException>(() => container.Resolve<IEnumerable<int>>());
        Assert.Throws<NotSupportedException>(() => container.Resolve<int[]>());
        Assert.Empty(container.Resolve<CovUnregistered[]>());
        Assert.Null(container.ResolveOptional(typeof(int?)));
        Assert.Null(container.ResolveOptionalKeyed("k", typeof(IEnumerable<CovUnregistered>)));
    }

    [Fact]
    public void GetRegistrationFactoryValidatesIndex()
    {
        var builder = new ContainerBuilder();
        builder.RegisterInstance(new CovFirstOfMany()).As<ICovService>();
        builder.RegisterInstance(new CovSecondOfMany()).As<ICovService>();
        var container = (ContainerImpl)builder.Build();
        Assert.IsType<CovFirstOfMany>(container.GetRegistrationFactory(typeof(ICovService), null, 0)(container, null));
        Assert.IsType<CovSecondOfMany>(container.GetRegistrationFactory(typeof(ICovService), null, 1)(container, null));
        Assert.Throws<ArgumentOutOfRangeException>(() => container.GetRegistrationFactory(typeof(ICovService), null, -1));
        Assert.Throws<ArgumentException>(() => container.GetRegistrationFactory(typeof(ICovService), null, 2));
        Assert.Throws<ArgumentException>(() => container.GetRegistrationFactory(typeof(CovUnregistered), "k", 0));
    }

    [Fact]
    public void PreserveExistingDefaultsKeepsFirstRegistrationAsDefault()
    {
        var builder = new ContainerBuilder();
        builder.RegisterInstance(new CovFirstOfMany()).As<ICovService>();
        builder.RegisterInstance(new CovSecondOfMany()).As<ICovService>().PreserveExistingDefaults();
        var container = builder.Build();
        Assert.IsType<CovFirstOfMany>(container.Resolve<ICovService>());
        Assert.Equal(2, container.Resolve<ICovService[]>().Length);
    }

    [Fact]
    public async Task SingletonFactoryFailureIsRetriedOnNextResolve()
    {
        var attempts = 0;
        var builder = new ContainerBuilder();
        builder.RegisterFactory<CovLogger>((_, _) => (_, _) =>
        {
            if (Interlocked.Increment(ref attempts) == 1) throw new InvalidOperationException("first");
            return new CovLogger();
        }).AsSelf().SingleInstance();
        await using var container = builder.Build();
        Assert.Equal("first", Assert.Throws<InvalidOperationException>(() => container.Resolve<CovLogger>()).Message);
        var logger = container.Resolve<CovLogger>();
        Assert.Same(logger, container.Resolve<CovLogger>());
        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task ScopedFactoryFailureIsRetriedOnNextResolve()
    {
        var attempts = 0;
        var builder = new ContainerBuilder();
        builder.RegisterFactory<CovLogger>((_, _) => (_, _) =>
        {
            if (Interlocked.Increment(ref attempts) == 1) throw new InvalidOperationException("first");
            return new CovLogger();
        }).AsSelf().Scoped();
        await using var container = builder.Build();
        await using var scope = container.CreateScope();
        Assert.Throws<InvalidOperationException>(() => scope.Resolve<CovLogger>());
        var logger = scope.Resolve<CovLogger>();
        Assert.Same(logger, scope.Resolve<CovLogger>());
    }

    [Fact]
    public async Task LifetimeFactoryCreationFailureIsRethrownAndRetried()
    {
        foreach (var scoped in new[] { false, true })
        {
            var attempts = 0;
            var builder = new ContainerBuilder();
            var registration = builder.RegisterFactory<CovLogger>((_, _) =>
            {
                if (Interlocked.Increment(ref attempts) == 1) throw new InvalidOperationException("factory");
                return (_, _) => new CovLogger();
            }).AsSelf();
            if (scoped) registration.Scoped();
            else registration.SingleInstance();
            await using var container = builder.Build();
            await using var scope = container.CreateScope();
            Assert.Equal("factory",
                Assert.Throws<InvalidOperationException>(() => scope.Resolve<CovLogger>()).Message);
            Assert.NotNull(scope.Resolve<CovLogger>());
        }
    }

    [Fact]
    public async Task ConcurrentSingletonResolutionWaitsForCreation()
    {
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var builder = new ContainerBuilder();
        builder.RegisterFactory<CovLogger>((_, _) => (_, _) =>
        {
            entered.Set();
            release.Wait();
            return new CovLogger();
        }).AsSelf().SingleInstance();
        await using var container = builder.Build();
        var first = Task.Run(() => container.Resolve<CovLogger>());
        entered.Wait();
        var second = Task.Run(() => container.Resolve<CovLogger>());
        await Task.Delay(50);
        release.Set();
        Assert.Same(await first, await second);
    }

    [Fact]
    public async Task ConcurrentScopedResolutionWaitsForCreation()
    {
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var builder = new ContainerBuilder();
        builder.RegisterFactory<CovLogger>((_, _) => (_, _) =>
        {
            entered.Set();
            release.Wait();
            return new CovLogger();
        }).AsSelf().Scoped();
        await using var container = builder.Build();
        await using var scope = container.CreateScope();
        var first = Task.Run(() => scope.Resolve<CovLogger>());
        entered.Wait();
        var second = Task.Run(() => scope.Resolve<CovLogger>());
        await Task.Delay(50);
        release.Set();
        Assert.Same(await first, await second);
    }

    [Fact]
    public void SingletonVerificationRejectsScopedDependency()
    {
        var builder = new ContainerBuilder();
        builder.RegisterTypeWithFallback<CovLogger>().AsSelf().Scoped();
        builder.RegisterTypeWithFallback<CovNeedsScoped>().AsSelf().SingleInstance();
        var ex = Assert.Throws<BTDBException>(() =>
            builder.BuildAndVerify(ContainerVerification.SingletonsUsingOnlySingletons));
        Assert.Contains("Scoped dependency", ex.Message);
    }

    [Fact]
    public async Task RootContainerDisposesOwnedServiceProvider()
    {
        var builder = new ContainerBuilder();
        builder.ServiceCollection.AddSingleton<CovDisposable>();
        var container = builder.Build();
        var disposable = container.Resolve<CovDisposable>();
        await using (var scope = container.CreateScope())
        {
            Assert.Same(disposable, scope.Resolve<CovDisposable>());
        }

        Assert.False(disposable.Disposed);
        await container.DisposeAsync();
        Assert.True(disposable.Disposed);
        await container.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => container.CreateScope());
    }

    [Fact]
    public void ServiceProviderLookupsAreCachedPerKeyAndFallback()
    {
        var builder = new ContainerBuilder();
        builder.ServiceCollection.AddSingleton<CovLogger>();
        builder.ServiceCollection.AddKeyedSingleton<ICovService, CovFirstOfMany>("k");
        builder.RegisterTypeWithFallback<CovNeedsScoped>().AsSelf();
        builder.RegisterTypeWithFallback<CovThree>().AsSelf();
        builder.RegisterTypeWithFallback<CovService>().AsSelf().As<ICovService>();
        var container = builder.Build();
        Assert.Same(container.Resolve<CovNeedsScoped>().Logger, container.Resolve<CovThree>().B);
        Assert.IsType<CovFirstOfMany>(container.ResolveKeyed<ICovService>("k"));
        Assert.IsType<CovFirstOfMany>(container.ResolveKeyed<IEnumerable<ICovService>>("k").Single());
        Assert.Null(container.ResolveOptionalKeyed<CovUnregistered>("k"));
        Assert.Null(container.ResolveOptionalKeyed<IEnumerable<CovUnregistered>>("k"));
        Assert.Single(container.Resolve<IEnumerable<CovLogger>>());
    }

    [Fact]
    public void UnusedTraitImplementationsBehave()
    {
        var scan = new ScanTraitImpl();
        Assert.True(scan.MatchFilter(typeof(string)));
        scan.Where(t => t.IsClass);
        scan.Where(t => t != typeof(object));
        Assert.True(scan.MatchFilter(typeof(string)));
        Assert.False(scan.MatchFilter(typeof(object)));
        Assert.False(scan.MatchFilter(typeof(int)));

        var live = new LiveScopeTraitImpl();
        Assert.Equal(Lifetime.AlwaysNew, live.Lifetime);
        live.SingleInstance();
        Assert.Equal(Lifetime.Singleton, live.Lifetime);
        live.Scoped();
        Assert.Equal(Lifetime.Scoped, live.Lifetime);

        Assert.Equal(typeof(IContainer), Need.ContainerNeed.ClrType);
        Assert.Equal(NeedKind.Internal, Need.ContainerNeed.Kind);
        Assert.False(Need.ContainerNeed.Optional);
        var need = new Need
        {
            ParentType = typeof(string), OptionalValue = 1, PropertyInfo = typeof(string).GetProperty("Length"),
            ForcedKey = true, Key = "k"
        };
        Assert.Equal(1, need.OptionalValue);
        Assert.Equal("Length", need.PropertyInfo!.Name);
        Assert.Equal(typeof(string), need.ParentType);
        Assert.True(need.ForcedKey);
        Assert.Equal("k", need.Key);
    }

    [Fact]
    public void DependencyAttributeKeepsName()
    {
        Assert.Null(new DependencyAttribute().Name);
        Assert.Equal("x", new DependencyAttribute("x").Name);
    }
}
