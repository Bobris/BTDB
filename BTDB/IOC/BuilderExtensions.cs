namespace BTDB.IOC
{
    public static class BuilderExtensions
    {
        public static IRegistration Register<T>(this ContainerBuilder builder)
        {
            return builder.Register(typeof (T));
        }

        public static IRegistration As<T>(this IRegistration registration)
        {
            registration.As(typeof (T));
            return registration;
        }
    }
}