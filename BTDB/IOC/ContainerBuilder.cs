using System;
using System.Reflection;
using BTDB.Collections;

namespace BTDB.IOC;

[Flags]
public enum ContainerVerification
{
    SingletonsUsingOnlySingletons = 1,
    None = 0,
    All = int.MaxValue
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

    public IRegistration<IAsLiveScopeConstructorPropertiesTrait> RegisterType(Type type)
    {
        var registration = new SingleRegistration(type);
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

    public IRegistration<IAsLiveScopeTrait> RegisterFactory<T>(Func<IContainer, T> factory) where T : class
    {
        var registration = new SingleFactoryRegistration(factory, typeof(T));
        _registrations.Add(registration);
        return registration;
    }

    public IRegistration<IAsLiveScopeTrait> RegisterFactory(Func<IContainer, object> factory, Type instanceType)
    {
        var registration = new SingleFactoryRegistration(factory, instanceType);
        _registrations.Add(registration);
        return registration;
    }

    public IRegistration<IAsLiveScopeConstructorPropertiesScanTrait> RegisterAssemblyTypes(Assembly from)
    {
        return RegisterAssemblyTypes(new[] { from });
    }

    public IRegistration<IAsLiveScopeConstructorPropertiesScanTrait> RegisterAssemblyTypes(params Assembly[] froms)
    {
        var registration = new MultiRegistration(froms);
        _registrations.Add(registration);
        return registration;
    }

    public IContainer Build()
    {
        return new ContainerImpl(_registrations.AsReadOnlySpan(), ContainerVerification.None);
    }

    public IContainer BuildAndVerify(ContainerVerification options = ContainerVerification.All)
    {
        return new ContainerImpl(_registrations.AsReadOnlySpan(), options);
    }
}
