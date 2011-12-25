namespace BTDB.IOC
{
    internal class LiveScopeTraitImpl : ILiveScopeTrait, ILiveScopeTraitImpl
    {
        Lifetime _lifetime = Lifetime.AlwaysNew;

        public void SingleInstance()
        {
            _lifetime = Lifetime.Singleton;
        }

        public Lifetime Lifetime
        {
            get { return _lifetime; }
        }
    }
}