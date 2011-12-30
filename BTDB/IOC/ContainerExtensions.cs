namespace BTDB.IOC
{
    public static class ContainerExtensions
    {
        public static T Resolve<T>(this IContainer container) where T : class
        {
            return (T)container.Resolve(typeof(T));
        }

        public static T ResolveKeyed<T>(this IContainer container, object key) where T: class
        {
            return (T) container.ResolveKeyed(key, typeof (T));
        }

        public static T ResolveNamed<T>(this IContainer container, string name) where T : class
        {
            return (T)container.ResolveNamed(name, typeof(T));
        }
    }
}