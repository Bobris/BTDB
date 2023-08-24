using System;

namespace BTDB.IOC;

class SingleInstanceRegistration : RegistrationBaseImpl<IAsTrait>, IContanerRegistration
{
    readonly object _instance;
    readonly Type _implementationType;

    public SingleInstanceRegistration(object instance, Type type)
    {
        _instance = instance;
        _implementationType = type;
    }

    public void Register(ContainerRegistrationContext context)
    {
        object Factory(IContainer container, IResolvingCtx? ctx)
        {
            return _instance;
        }

        Func<IContainer, IResolvingCtx?, object> FactoryFactory(IContainer container,
            ICreateFactoryCtx createFactoryCtx)
        {
            return Factory;
        }

        context.AddCReg(GetAsTypesFor(_implementationType), PreserveExistingDefaults, UniqueRegistration,
            new() { Factory = FactoryFactory, Lifetime = Lifetime.AlwaysNew, SingletonId = uint.MaxValue });
    }
}

