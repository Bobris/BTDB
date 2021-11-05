using System;

namespace BTDB.IOC;

public static class BuilderExtensions
{
    public static IRegistration<IAsLiveScopeConstructorPropertiesTrait> RegisterType<T>(this ContainerBuilder builder)
    {
        return builder.RegisterType(typeof(T));
    }

    public static IRegistration<TTraits> SingleInstance<TTraits>(this IRegistration<TTraits> registration) where TTraits : ILiveScopeTrait
    {
        ((ILiveScopeTrait)registration.InternalTraits(typeof(ILiveScopeTrait))).SingleInstance();
        return registration;
    }

    public static IRegistration<TTraits> UsingConstructor<TTraits>(this IRegistration<TTraits> registration, params Type[] parameterTypes) where TTraits : IConstructorTrait
    {
        ((IConstructorTrait)registration.InternalTraits(typeof(IConstructorTrait))).UsingConstructor(parameterTypes);
        return registration;
    }

    public static IRegistration<TTraits> As<TTraits>(this IRegistration<TTraits> registration, Type serviceType) where TTraits : IAsTrait
    {
        ((IAsTrait)registration.InternalTraits(typeof(IAsTrait))).As(serviceType);
        return registration;
    }

    public static IRegistration<TTraits> PreserveExistingDefaults<TTraits>(this IRegistration<TTraits> registration) where TTraits : IAsTrait
    {
        ((IAsTrait)registration.InternalTraits(typeof(IAsTrait))).SetPreserveExistingDefaults();
        return registration;
    }

    public static IRegistration<TTraits> UniqueRegistration<TTraits>(this IRegistration<TTraits> registration, bool value) where TTraits : IAsTrait
    {
        ((IAsTrait)registration.InternalTraits(typeof(IAsTrait))).UniqueRegistration = value;
        return registration;
    }

    public static IRegistration<TTraits> Named<TTraits>(this IRegistration<TTraits> registration, string serviceName, Type serviceType) where TTraits : IAsTrait
    {
        ((IAsTrait)registration.InternalTraits(typeof(IAsTrait))).Keyed(serviceName, serviceType);
        return registration;
    }

    public static IRegistration<TTraits> Keyed<TTraits>(this IRegistration<TTraits> registration, object serviceKey, Type serviceType) where TTraits : IAsTrait
    {
        ((IAsTrait)registration.InternalTraits(typeof(IAsTrait))).Keyed(serviceKey, serviceType);
        return registration;
    }

    public static IRegistration<TTraits> AsSelf<TTraits>(this IRegistration<TTraits> registration) where TTraits : IAsTrait
    {
        ((IAsTrait)registration.InternalTraits(typeof(IAsTrait))).AsSelf();
        return registration;
    }

    public static IRegistration<TTraits> AsImplementedInterfaces<TTraits>(this IRegistration<TTraits> registration) where TTraits : IAsTrait
    {
        ((IAsTrait)registration.InternalTraits(typeof(IAsTrait))).AsImplementedInterfaces();
        return registration;
    }

    public static IRegistration<TTraits> Where<TTraits>(this IRegistration<TTraits> registration, Predicate<Type> filter) where TTraits : IScanTrait
    {
        ((IScanTrait)registration.InternalTraits(typeof(IScanTrait))).Where(filter);
        return registration;
    }

    public static IRegistration<TTraits> PropertiesAutowired<TTraits>(this IRegistration<TTraits> registration) where TTraits : IPropertiesTrait
    {
        ((IPropertiesTrait)registration.InternalTraits(typeof(IPropertiesTrait))).PropertiesAutowired();
        return registration;
    }
}
