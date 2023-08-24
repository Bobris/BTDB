namespace BTDB.IOC;

public interface IRegistration
{
}

public interface IRegistration<TTraits> : IRegistration
{
    IRegistration<TTraits> As<T>();
    IRegistration<TTraits> Keyed<T>(object key);
    IRegistration<TTraits> Named<T>(string name);
}
