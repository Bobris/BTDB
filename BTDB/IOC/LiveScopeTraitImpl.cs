namespace BTDB.IOC;

class LiveScopeTraitImpl : ILiveScopeTrait, ILiveScopeTraitImpl
{
    Lifetime _lifetime = Lifetime.AlwaysNew;

    public void SingleInstance()
    {
        _lifetime = Lifetime.Singleton;
    }

    public void Scoped()
    {
        _lifetime = Lifetime.Scoped;
    }

    public Lifetime Lifetime => _lifetime;
}
