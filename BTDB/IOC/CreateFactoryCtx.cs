namespace BTDB.IOC;

public class CreateFactoryCtx : ICreateFactoryCtx
{
    internal uint SingletonDeepness;
    internal bool VerifySingletons;
    internal bool Enumerate;
}
