using System;
using System.Reflection;
using BTDB.Collections;
using BTDB.IL;

namespace BTDB.IOC;

class MultiRegistration : RegistrationBaseImpl<IAsLiveScopeScanTrait>, ILiveScopeTrait, ILiveScopeTraitImpl, IScanTrait,
    IScanTraitImpl, IContanerRegistration
{
    public void SingleInstance()
    {
        Lifetime = Lifetime.Singleton;
    }

    public Lifetime Lifetime { get; private set; } = Lifetime.AlwaysNew;

    StructList<Predicate<Type>> _filters = new();

    readonly Assembly[]? _fromAssemblies;

    public MultiRegistration()
    {
        _fromAssemblies = null;
    }

    public MultiRegistration(Assembly[] fromParams)
    {
        _fromAssemblies = fromParams;
    }

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
        if (_fromAssemblies == null)
        {
            foreach (var (typeToken, value) in IContainer.FactoryRegistry)
            {
                var type = Type.GetTypeFromHandle(RuntimeTypeHandle.FromIntPtr(typeToken));
                if (type is not { IsClass: true }) continue;
                if (!MatchFilter(type)) continue;
                ((IContanerRegistration)new SingleRegistration(type, this, Lifetime)).Register(context);
            }
        }
        else
        {
            foreach (var assembly in _fromAssemblies)
            {
                foreach (var type in assembly.GetTypes())
                {
                    if (!type.IsClass || type.IsAbstract || !MatchFilter(type)) continue;
                    ((IContanerRegistration)new SingleRegistration(type, this, Lifetime)).Register(context);
                }
            }
        }
    }
}
