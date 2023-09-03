using System;

namespace BTDB.IOC;

class SingleRegistration : RegistrationBaseImpl<IAsLiveScopeTrait>, IContanerRegistration, ILiveScopeTrait,
    ILiveScopeTraitImpl
{
    readonly Type _implementationType;

    Lifetime _lifetime = Lifetime.AlwaysNew;

    public void SingleInstance()
    {
        _lifetime = Lifetime.Singleton;
    }

    public Lifetime Lifetime => _lifetime;

    public SingleRegistration(Type implementationType)
    {
        _implementationType = implementationType;
    }

    internal SingleRegistration(Type implementationType, IAsTraitImpl asTrait, Lifetime lifetime)
    {
        _implementationType = implementationType;
        UniqueRegistration = asTrait.UniqueRegistration;
        _preserveExistingDefaults = asTrait.PreserveExistingDefaults;
        foreach (var keyAndType in asTrait.GetAsTypesFor(_implementationType))
        {
            _asTypes.Add(keyAndType);
        }

        _lifetime = lifetime;
    }

    public void Register(ContainerRegistrationContext context)
    {
        if (!IContainer.FactoryRegistry.TryGetValue(_implementationType.TypeHandle.Value, out var factory))
        {
            throw new ArgumentException($"Factory is not registered for type {_implementationType.FullName}");
        }

        context.AddCReg(GetAsTypesFor(_implementationType), PreserveExistingDefaults, UniqueRegistration,
            new()
            {
                Factory = factory, Lifetime = Lifetime, SingletonId = Lifetime == Lifetime.Singleton ? uint.MaxValue : 0
            });
    }
}
