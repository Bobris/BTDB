using System;

namespace BTDB.IOC;

public static class BuilderExtensions
{
    public static IRegistration<IAsLiveScopeTrait> RegisterType<T>(this ContainerBuilder builder)
    {
        return builder.RegisterType(typeof(T));
    }

    public static IRegistration<IAsLiveScopeTrait> RegisterTypeWithConstructorParameters<T>(
        this ContainerBuilder builder, params Type[] constructorParameters) where T : class
    {
        return builder.RegisterFactory<T>(SingleRegistration.BuildFactory(typeof(T),
            typeof(T).GetConstructor(constructorParameters) ?? throw new ArgumentException("Constructor not found")));
    }

    public static IRegistration<TTraits> SingleInstance<TTraits>(this IRegistration<TTraits> registration)
        where TTraits : ILiveScopeTrait
    {
        ((ILiveScopeTrait)registration).SingleInstance();
        return registration;
    }

    public static IRegistration<TTraits> As<TTraits>(this IRegistration<TTraits> registration, Type serviceType)
        where TTraits : IAsTrait
    {
        ((IAsTrait)registration).As(serviceType);
        return registration;
    }

    public static IRegistration<TTraits> PreserveExistingDefaults<TTraits>(this IRegistration<TTraits> registration)
        where TTraits : IAsTrait
    {
        ((IAsTrait)registration).SetPreserveExistingDefaults();
        return registration;
    }

    public static IRegistration<TTraits> UniqueRegistration<TTraits>(this IRegistration<TTraits> registration,
        bool value) where TTraits : IAsTrait
    {
        ((IAsTrait)registration).UniqueRegistration = value;
        return registration;
    }

    public static IRegistration<TTraits> Named<TTraits>(this IRegistration<TTraits> registration, string serviceName,
        Type serviceType) where TTraits : IAsTrait
    {
        ((IAsTrait)registration).Keyed(serviceName, serviceType);
        return registration;
    }

    public static IRegistration<TTraits> Keyed<TTraits>(this IRegistration<TTraits> registration, object serviceKey,
        Type serviceType) where TTraits : IAsTrait
    {
        ((IAsTrait)registration).Keyed(serviceKey, serviceType);
        return registration;
    }

    public static IRegistration<TTraits> AsSelf<TTraits>(this IRegistration<TTraits> registration)
        where TTraits : IAsTrait
    {
        ((IAsTrait)registration).AsSelf();
        return registration;
    }

    public static IRegistration<TTraits> AsImplementedInterfaces<TTraits>(this IRegistration<TTraits> registration)
        where TTraits : IAsTrait
    {
        ((IAsTrait)registration).AsImplementedInterfaces();
        return registration;
    }

    public static IRegistration<TTraits> Where<TTraits>(this IRegistration<TTraits> registration,
        Predicate<Type> filter) where TTraits : IScanTrait
    {
        ((IScanTrait)registration).Where(filter);
        return registration;
    }
}
