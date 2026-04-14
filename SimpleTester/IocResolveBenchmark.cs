using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BTDB;
using BTDB.IOC;
using BtdbRegisteredTransient = BTDBTest.IocTests.TestLogger;
using Microsoft.Extensions.DependencyInjection;

namespace SimpleTester;

[MemoryDiagnoser]
[ShortRunJob]
public class IocResolveBenchmark
{
    IContainer _container = null!;
    IServiceProvider _serviceProvider = null!;
    Func<IContainer, BtdbRegisteredTransient> _btdbRegistrationFuncResolve = null!;
    Func<IContainer, MsDiRegisteredTransient> _msDiRegistrationFuncResolve = null!;
    Func<BtdbRegisteredTransient> _btdbRegistrationZeroArgFuncResolve = null!;
    Func<MsDiRegisteredTransient> _msDiRegistrationZeroArgFuncResolve = null!;
    ObjectFactory<BtdbRegisteredTransient> _btdbRegistrationObjectFactory = null!;
    ObjectFactory<MsDiRegisteredTransient> _msDiRegistrationObjectFactory = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        var services = new ServiceCollection();
        var containerBuilder = new ContainerBuilder();
        containerBuilder.RegisterType<BtdbRegisteredTransient>().AsSelf();
        services.AddTransient<MsDiRegisteredTransient>();
        services.UseBtdbIoc(containerBuilder);

        _serviceProvider = services.BuildServiceProvider();
        _container = _serviceProvider.GetRequiredService<IRootContainer>();
        _btdbRegistrationFuncResolve = _container.Resolve<Func<IContainer, BtdbRegisteredTransient>>();
        _msDiRegistrationFuncResolve = _container.Resolve<Func<IContainer, MsDiRegisteredTransient>>();
        _btdbRegistrationZeroArgFuncResolve = _container.Resolve<Func<BtdbRegisteredTransient>>();
        _msDiRegistrationZeroArgFuncResolve = _container.Resolve<Func<MsDiRegisteredTransient>>();
        _btdbRegistrationObjectFactory = ActivatorUtilities.CreateFactory<BtdbRegisteredTransient>(Type.EmptyTypes);
        _msDiRegistrationObjectFactory = ActivatorUtilities.CreateFactory<MsDiRegisteredTransient>(Type.EmptyTypes);

        _ = _container.Resolve<BtdbRegisteredTransient>();
        _ = _serviceProvider.GetRequiredService<BtdbRegisteredTransient>();
        _ = _container.Resolve<MsDiRegisteredTransient>();
        _ = _serviceProvider.GetRequiredService<MsDiRegisteredTransient>();
        _ = _btdbRegistrationFuncResolve(_container);
        _ = _msDiRegistrationFuncResolve(_container);
        _ = _btdbRegistrationZeroArgFuncResolve();
        _ = _msDiRegistrationZeroArgFuncResolve();
        _ = _btdbRegistrationObjectFactory(_serviceProvider, null);
        _ = _msDiRegistrationObjectFactory(_serviceProvider, null);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        switch (_serviceProvider)
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
        return _container.Resolve<BtdbRegisteredTransient>();
    }

    [Benchmark]
    public object BtdbRegistration_MsDiResolve()
    {
        return _serviceProvider.GetRequiredService<BtdbRegisteredTransient>();
    }

    [Benchmark]
    public object MsDiRegistration_BtdbResolve()
    {
        return _container.Resolve<MsDiRegisteredTransient>();
    }

    [Benchmark]
    public object BtdbRegistration_BtdbFuncResolve()
    {
        return _btdbRegistrationFuncResolve(_container);
    }

    [Benchmark]
    public object MsDiRegistration_BtdbFuncResolve()
    {
        return _msDiRegistrationFuncResolve(_container);
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
    public object BtdbRegistration_MsDiObjectFactoryActivate()
    {
        return _btdbRegistrationObjectFactory(_serviceProvider, null);
    }

    [Benchmark]
    public object MsDiRegistration_MsDiObjectFactoryActivate()
    {
        return _msDiRegistrationObjectFactory(_serviceProvider, null);
    }

    [Benchmark]
    public object MsDiRegistration_MsDiResolve()
    {
        return _serviceProvider.GetRequiredService<MsDiRegisteredTransient>();
    }
}

public sealed class MsDiRegisteredTransient
{
}
