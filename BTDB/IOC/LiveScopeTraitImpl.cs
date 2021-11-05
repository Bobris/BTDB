namespace BTDB.IOC;

class LiveScopeTraitImpl : ILiveScopeTrait, ILiveScopeTraitImpl
{
    Lifetime _lifetime = Lifetime.AlwaysNew;

    public void SingleInstance()
    {
        _lifetime = Lifetime.Singleton;
    }

    public Lifetime Lifetime => _lifetime;
}
