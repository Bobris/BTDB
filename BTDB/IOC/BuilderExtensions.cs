using System;

namespace BTDB.IOC
{
    public static class BuilderExtensions
    {
        public static IRegistration<IAsTraitAndLiveScopeTrait> RegisterType<T>(this ContainerBuilder builder)
        {
            return builder.RegisterType(typeof (T));
        }

        public static IRegistration<TTraits> SingleInstance<TTraits>(this IRegistration<TTraits> registration) where TTraits : ILiveScopeTrait
        {
            ((ILiveScopeTrait)registration.InternalTraits(typeof(ILiveScopeTrait))).SingleInstance();
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
    }
}