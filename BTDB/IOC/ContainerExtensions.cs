namespace BTDB.IOC
{
    public static class ContainerExtensions
    {
        public static T Resolve<T>(this IContainer container) where T : class
        {
            return (T)container.Resolve(typeof(T));
        }
    }
}