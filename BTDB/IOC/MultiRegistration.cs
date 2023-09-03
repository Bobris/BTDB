using System;
using System.Reflection;
using BTDB.Collections;
using BTDB.IL;

namespace BTDB.IOC;

class MultiRegistration : RegistrationBaseImpl<IAsLiveScopeScanTrait>, ILiveScopeTrait, ILiveScopeTraitImpl, IScanTrait, IScanTraitImpl, IContanerRegistration
{
    Lifetime _lifetime = Lifetime.AlwaysNew;

    public void SingleInstance()
    {
        _lifetime = Lifetime.Singleton;
    }

    public Lifetime Lifetime => _lifetime;

    StructList<Predicate<Type>> _filters;

    public void Where(Predicate<Type> filter)
    {
        _filters.Add(filter);
    }

    public bool MatchFilter(Type type)
    {
        foreach (var predicate in _filters)
        {
            if (!predicate(type)) return false;
        }
        return true;
    }

    public void Register(ContainerRegistrationContext context)
    {
        foreach (var (typeToken, value) in IContainer.FactoryRegistry)
        {
            var type = Type.GetTypeFromHandle(RuntimeTypeHandle.FromIntPtr(typeToken));
            if (type is not { IsClass: true }) continue;
            if (!MatchFilter(type)) continue;
            ((IContanerRegistration)new SingleRegistration(type, this, _lifetime)).Register(context);
        }
    }
}
