using System;
using System.Reflection;
using BTDB.Collections;

namespace BTDB.IOC;

[Flags]
public enum ContainerVerification
{
    SingletonsUsingOnlySingletons = 1,
    AllTypesAreGenerated = 2,
    ReportNotGeneratedTypes = 4,
    None = 0,
    All = int.MaxValue - ReportNotGeneratedTypes
}

[Flags]
public enum ContainerBuilderBehaviour
{
    UniqueRegistrations = 1,
    None = 0
}

public class ContainerBuilder
{
    StructList<IRegistration> _registrations;

    readonly ContainerBuilderBehaviour _builderBehaviour;

    public ContainerBuilder(ContainerBuilderBehaviour builderBehaviour = ContainerBuilderBehaviour.None)
    {
        _builderBehaviour = builderBehaviour;
    }

    public IRegistration<IAsLiveScopeTrait> RegisterType(Type type)
    {
        var registration = new SingleRegistration(type);
        _registrations.Add(registration);
        if (_builderBehaviour.HasFlag(ContainerBuilderBehaviour.UniqueRegistrations))
            registration.UniqueRegistration(true);
        return registration;
    }

    /// Like RegisterType but with explicitly allowed fallback to reflection if type is not generated
    public IRegistration<IAsLiveScopeTrait> RegisterTypeWithFallback(Type type)
    {
        var registration = new SingleRegistration(type, true);
        _registrations.Add(registration);
        if (_builderBehaviour.HasFlag(ContainerBuilderBehaviour.UniqueRegistrations))
            registration.UniqueRegistration(true);
        return registration;
    }

    public IRegistration<IAsTrait> RegisterInstance(object instance)
    {
        var instanceType = instance.GetType();
        var registration = new SingleInstanceRegistration(instance, instanceType);
        if (_builderBehaviour.HasFlag(ContainerBuilderBehaviour.UniqueRegistrations))
            registration.UniqueRegistration(true);
        _registrations.Add(registration);
        return registration;
    }

    public IRegistration<IAsTrait> RegisterInstance<T>(object instance)
    {
        var instanceType = typeof(T);
        var registration = new SingleInstanceRegistration(instance, instanceType);
        if (_builderBehaviour.HasFlag(ContainerBuilderBehaviour.UniqueRegistrations))
            registration.UniqueRegistration(true);
        _registrations.Add(registration);
        return registration;
    }

    public IRegistration<IAsLiveScopeTrait> RegisterFactory<T>(
        Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx?, object>> factory) where T : class
    {
        var registration = new SingleFactoryRegistration(factory, typeof(T));
        _registrations.Add(registration);
        return registration;
    }

    public IRegistration<IAsLiveScopeTrait> RegisterFactory(
        Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx?, object>> factory, Type instanceType)
    {
        var registration = new SingleFactoryRegistration(factory, instanceType);
        _registrations.Add(registration);
        return registration;
    }

    /// Partially obsolete, it is better to use RegisterFactory with Func&lt;IContainer, ICreateFactoryCtx, Func&lt;IContainer, IResolvingCtx?, object&gt;&gt; factory
    public IRegistration<IAsLiveScopeTrait> RegisterFactory<T>(Func<IContainer, T> factory) where T : class
    {
        var registration = new SingleFactoryRegistration(factory, typeof(T));
        _registrations.Add(registration);
        return registration;
    }

    /// Partially obsolete, it is better to use RegisterFactory with Func&lt;IContainer, ICreateFactoryCtx, Func&lt;IContainer, IResolvingCtx?, object&gt;&gt; factory
    public IRegistration<IAsLiveScopeTrait> RegisterFactory(Func<IContainer, object> factory, Type instanceType)
    {
        var registration = new SingleFactoryRegistration(factory, instanceType);
        _registrations.Add(registration);
        return registration;
    }

    public IRegistration<IAsLiveScopeScanTrait> RegisterAssemblyTypes(Assembly from)
    {
        return RegisterAssemblyTypes(new[] { from });
    }

    public IRegistration<IAsLiveScopeScanTrait> RegisterAssemblyTypes(params Assembly[] fromParams)
    {
        var registration = new MultiRegistration(fromParams);
        _registrations.Add(registration);
        return registration;
    }

    public IRegistration<IAsLiveScopeScanTrait> AutoRegisterTypes()
    {
        var registration = new MultiRegistration();
        _registrations.Add(registration);
        return registration;
    }

    public IContainer Build()
    {
        return new ContainerImpl(_registrations.AsReadOnlySpan(), ContainerVerification.AllTypesAreGenerated);
    }

    public IContainer BuildAndVerify(ContainerVerification options = ContainerVerification.All)
    {
        return new ContainerImpl(_registrations.AsReadOnlySpan(), options);
    }
}
