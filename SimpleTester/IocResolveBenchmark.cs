using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BTDB;
using BTDB.IOC;
using BtdbRegisteredService = BTDBTest.IocTests.TestLogger;
using Microsoft.Extensions.DependencyInjection;

namespace SimpleTester;

[MemoryDiagnoser]
[ShortRunJob]
public class IocResolveBenchmark
{
    IContainer _transientContainer = null!;
    IServiceProvider _transientServiceProvider = null!;
    Func<IContainer, BtdbRegisteredService> _btdbRegistrationFuncResolve = null!;
    Func<IContainer, MsDiRegisteredTransient> _msDiRegistrationFuncResolve = null!;
    Func<BtdbRegisteredService> _btdbRegistrationZeroArgFuncResolve = null!;
    Func<MsDiRegisteredTransient> _msDiRegistrationZeroArgFuncResolve = null!;
    IServiceProvider _scopedRootServiceProvider = null!;
    AsyncServiceScope _scopedServiceScope;
    IServiceProvider _scopedServiceProvider = null!;
    IContainer _scopedContainer = null!;
    IServiceProvider _singletonServiceProvider = null!;
    IContainer _singletonContainer = null!;
    Func<IContainer, BtdbRegisteredService> _scopedBtdbRegistrationFuncResolve = null!;
    Func<IContainer, MsDiRegisteredScoped> _scopedMsDiRegistrationFuncResolve = null!;
    Func<BtdbRegisteredService> _scopedBtdbRegistrationZeroArgFuncResolve = null!;
    Func<MsDiRegisteredScoped> _scopedMsDiRegistrationZeroArgFuncResolve = null!;
    Func<IContainer, BtdbRegisteredService> _singletonBtdbRegistrationFuncResolve = null!;
    Func<IContainer, MsDiRegisteredSingleton> _singletonMsDiRegistrationFuncResolve = null!;
    Func<BtdbRegisteredService> _singletonBtdbRegistrationZeroArgFuncResolve = null!;
    Func<MsDiRegisteredSingleton> _singletonMsDiRegistrationZeroArgFuncResolve = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        SetupTransient();
        SetupScoped();
        SetupSingleton();
    }

    void SetupTransient()
    {
        var services = new ServiceCollection();
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterType<BtdbRegisteredService>().AsSelf();
        services.AddTransient<MsDiRegisteredTransient>();
        services.UseBtdbIoc(containerBuilder);

        _transientServiceProvider = services.BuildServiceProvider();
        _transientContainer = _transientServiceProvider.GetRequiredService<IRootContainer>();
        _btdbRegistrationFuncResolve = _transientContainer.Resolve<Func<IContainer, BtdbRegisteredService>>();
        _msDiRegistrationFuncResolve = _transientContainer.Resolve<Func<IContainer, MsDiRegisteredTransient>>();
        _btdbRegistrationZeroArgFuncResolve = _transientContainer.Resolve<Func<BtdbRegisteredService>>();
        _msDiRegistrationZeroArgFuncResolve = _transientContainer.Resolve<Func<MsDiRegisteredTransient>>();

        _ = _transientContainer.Resolve<BtdbRegisteredService>();
        _ = _transientServiceProvider.GetRequiredService<BtdbRegisteredService>();
        _ = _transientContainer.Resolve<MsDiRegisteredTransient>();
        _ = _transientServiceProvider.GetRequiredService<MsDiRegisteredTransient>();
        _ = _btdbRegistrationFuncResolve(_transientContainer);
        _ = _msDiRegistrationFuncResolve(_transientContainer);
        _ = _btdbRegistrationZeroArgFuncResolve();
        _ = _msDiRegistrationZeroArgFuncResolve();
    }

    void SetupScoped()
    {
        var services = new ServiceCollection();
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterType<BtdbRegisteredService>().AsSelf().Scoped();
        services.AddScoped<MsDiRegisteredScoped>();
        services.UseBtdbIoc(containerBuilder);

        _scopedRootServiceProvider = services.BuildServiceProvider();
        _scopedServiceScope = _scopedRootServiceProvider.CreateAsyncScope();
        _scopedServiceProvider = _scopedServiceScope.ServiceProvider;
        _scopedContainer = _scopedServiceProvider.GetRequiredService<IContainer>();
        _scopedBtdbRegistrationFuncResolve = _scopedContainer.Resolve<Func<IContainer, BtdbRegisteredService>>();
        _scopedMsDiRegistrationFuncResolve = _scopedContainer.Resolve<Func<IContainer, MsDiRegisteredScoped>>();
        _scopedBtdbRegistrationZeroArgFuncResolve = _scopedContainer.Resolve<Func<BtdbRegisteredService>>();
        _scopedMsDiRegistrationZeroArgFuncResolve = _scopedContainer.Resolve<Func<MsDiRegisteredScoped>>();

        _ = _scopedContainer.Resolve<BtdbRegisteredService>();
        _ = _scopedServiceProvider.GetRequiredService<BtdbRegisteredService>();
        _ = _scopedContainer.Resolve<MsDiRegisteredScoped>();
        _ = _scopedServiceProvider.GetRequiredService<MsDiRegisteredScoped>();
        _ = _scopedBtdbRegistrationFuncResolve(_scopedContainer);
        _ = _scopedMsDiRegistrationFuncResolve(_scopedContainer);
        _ = _scopedBtdbRegistrationZeroArgFuncResolve();
        _ = _scopedMsDiRegistrationZeroArgFuncResolve();
    }

    void SetupSingleton()
    {
        var services = new ServiceCollection();
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterType<BtdbRegisteredService>().AsSelf().SingleInstance();
        services.AddSingleton<MsDiRegisteredSingleton>();
        services.UseBtdbIoc(containerBuilder);

        _singletonServiceProvider = services.BuildServiceProvider();
        _singletonContainer = _singletonServiceProvider.GetRequiredService<IRootContainer>();
        _singletonBtdbRegistrationFuncResolve = _singletonContainer.Resolve<Func<IContainer, BtdbRegisteredService>>();
        _singletonMsDiRegistrationFuncResolve = _singletonContainer.Resolve<Func<IContainer, MsDiRegisteredSingleton>>();
        _singletonBtdbRegistrationZeroArgFuncResolve = _singletonContainer.Resolve<Func<BtdbRegisteredService>>();
        _singletonMsDiRegistrationZeroArgFuncResolve = _singletonContainer.Resolve<Func<MsDiRegisteredSingleton>>();

        _ = _singletonContainer.Resolve<BtdbRegisteredService>();
        _ = _singletonServiceProvider.GetRequiredService<BtdbRegisteredService>();
        _ = _singletonContainer.Resolve<MsDiRegisteredSingleton>();
        _ = _singletonServiceProvider.GetRequiredService<MsDiRegisteredSingleton>();
        _ = _singletonBtdbRegistrationFuncResolve(_singletonContainer);
        _ = _singletonMsDiRegistrationFuncResolve(_singletonContainer);
        _ = _singletonBtdbRegistrationZeroArgFuncResolve();
        _ = _singletonMsDiRegistrationZeroArgFuncResolve();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await DisposeAsync(_scopedServiceScope);
        await DisposeAsync(_scopedRootServiceProvider);
        await DisposeAsync(_singletonServiceProvider);
        await DisposeAsync(_transientServiceProvider);
    }

    static async Task DisposeAsync(object? service)
    {
        switch (service)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync();
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }
    }

    [Benchmark(Baseline = true)]
    public object BtdbRegistration_BtdbResolve()
    {
        return _transientContainer.Resolve<BtdbRegisteredService>();
    }

    [Benchmark]
    public object BtdbRegistration_MsDiResolve()
    {
        return _transientServiceProvider.GetRequiredService<BtdbRegisteredService>();
    }

    [Benchmark]
    public object MsDiRegistration_BtdbResolve()
    {
        return _transientContainer.Resolve<MsDiRegisteredTransient>();
    }

    [Benchmark]
    public object BtdbRegistration_BtdbFuncResolve()
    {
        return _btdbRegistrationFuncResolve(_transientContainer);
    }

    [Benchmark]
    public object MsDiRegistration_BtdbFuncResolve()
    {
        return _msDiRegistrationFuncResolve(_transientContainer);
    }

    [Benchmark]
    public object BtdbRegistration_BtdbZeroArgFuncResolve()
    {
        return _btdbRegistrationZeroArgFuncResolve();
    }

    [Benchmark]
    public object MsDiRegistration_BtdbZeroArgFuncResolve()
    {
        return _msDiRegistrationZeroArgFuncResolve();
    }

    [Benchmark]
    public object MsDiRegistration_MsDiResolve()
    {
        return _transientServiceProvider.GetRequiredService<MsDiRegisteredTransient>();
    }

    [Benchmark]
    public object Scoped_BtdbRegistration_BtdbResolve()
    {
        return _scopedContainer.Resolve<BtdbRegisteredService>();
    }

    [Benchmark]
    public object Scoped_BtdbRegistration_MsDiResolve()
    {
        return _scopedServiceProvider.GetRequiredService<BtdbRegisteredService>();
    }

    [Benchmark]
    public object Scoped_MsDiRegistration_BtdbResolve()
    {
        return _scopedContainer.Resolve<MsDiRegisteredScoped>();
    }

    [Benchmark]
    public object Scoped_MsDiRegistration_MsDiResolve()
    {
        return _scopedServiceProvider.GetRequiredService<MsDiRegisteredScoped>();
    }

    [Benchmark]
    public object Scoped_BtdbRegistration_BtdbFuncResolve()
    {
        return _scopedBtdbRegistrationFuncResolve(_scopedContainer);
    }

    [Benchmark]
    public object Scoped_MsDiRegistration_BtdbFuncResolve()
    {
        return _scopedMsDiRegistrationFuncResolve(_scopedContainer);
    }

    [Benchmark]
    public object Scoped_BtdbRegistration_BtdbZeroArgFuncResolve()
    {
        return _scopedBtdbRegistrationZeroArgFuncResolve();
    }

    [Benchmark]
    public object Scoped_MsDiRegistration_BtdbZeroArgFuncResolve()
    {
        return _scopedMsDiRegistrationZeroArgFuncResolve();
    }

    [Benchmark]
    public object Singleton_BtdbRegistration_BtdbResolve()
    {
        return _singletonContainer.Resolve<BtdbRegisteredService>();
    }

    [Benchmark]
    public object Singleton_BtdbRegistration_MsDiResolve()
    {
        return _singletonServiceProvider.GetRequiredService<BtdbRegisteredService>();
    }

    [Benchmark]
    public object Singleton_MsDiRegistration_BtdbResolve()
    {
        return _singletonContainer.Resolve<MsDiRegisteredSingleton>();
    }

    [Benchmark]
    public object Singleton_MsDiRegistration_MsDiResolve()
    {
        return _singletonServiceProvider.GetRequiredService<MsDiRegisteredSingleton>();
    }

    [Benchmark]
    public object Singleton_BtdbRegistration_BtdbFuncResolve()
    {
        return _singletonBtdbRegistrationFuncResolve(_singletonContainer);
    }

    [Benchmark]
    public object Singleton_MsDiRegistration_BtdbFuncResolve()
    {
        return _singletonMsDiRegistrationFuncResolve(_singletonContainer);
    }

    [Benchmark]
    public object Singleton_BtdbRegistration_BtdbZeroArgFuncResolve()
    {
        return _singletonBtdbRegistrationZeroArgFuncResolve();
    }

    [Benchmark]
    public object Singleton_MsDiRegistration_BtdbZeroArgFuncResolve()
    {
        return _singletonMsDiRegistrationZeroArgFuncResolve();
    }
}

public sealed class MsDiRegisteredTransient;

public sealed class MsDiRegisteredScoped;

public sealed class MsDiRegisteredSingleton;
