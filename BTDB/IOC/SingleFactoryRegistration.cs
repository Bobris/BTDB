using System;

namespace BTDB.IOC;

class SingleFactoryRegistration : RegistrationBaseImpl<IAsLiveScopeTrait>, ILiveScopeTrait, ILiveScopeTraitImpl,
    IContanerRegistration
{
    readonly Type _implementationType;
    readonly Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx?, object>> _factory;

    Lifetime _lifetime = Lifetime.AlwaysNew;

    public void SingleInstance()
    {
        _lifetime = Lifetime.Singleton;
    }

    public Lifetime Lifetime => _lifetime;

    public SingleFactoryRegistration(
        Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx?, object>> factory, Type type)
    {
        _factory = factory;
        _implementationType = type;
    }

    public SingleFactoryRegistration(Func<IContainer, object> factory, Type instanceType)
    {
        _factory = (_, _) => (c, _) => factory(c);
        _implementationType = instanceType;
    }

    public void Register(ContainerRegistrationContext context)
    {
        context.AddCReg(GetAsTypesFor(_implementationType), PreserveExistingDefaults, UniqueRegistration,
            new CReg
            {
                Factory = _factory, Lifetime = _lifetime,
                SingletonId = _lifetime == Lifetime.Singleton ? uint.MaxValue : 0
            });
    }
}
