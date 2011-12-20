namespace BTDB.IOC
{
    public static class BuilderExtensions
    {
        public static IRegistration RegisterType<T>(this ContainerBuilder builder)
        {
            return builder.RegisterType(typeof (T));
        }

        public static IRegistration As<T>(this IRegistration registration)
        {
            registration.As(typeof (T));
            return registration;
        }
    }
}