using System;
using System.Reflection;
using BTDB.Collections;

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
    readonly Assembly? _fromAssembly;

    public MultiRegistration()
    {
        _fromAssemblies = null;
    }

    public MultiRegistration(Assembly fromAssembly)
    {
        _fromAssembly = fromAssembly;
    }

    public MultiRegistration(Assembly[] fromParams)
    {
        if (fromParams.Length == 1)
        {
            _fromAssembly = fromParams[0];
            return;
        }

        _fromAssemblies = fromParams;
    }

    public MultiRegistration(ReadOnlySpan<Assembly> fromParams)
    {
        if (fromParams.Length == 1)
        {
            _fromAssembly = fromParams[0];
            return;
        }

        if (!fromParams.IsEmpty)
        {
            _fromAssemblies = fromParams.ToArray();
        }
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
        void RegisterType(Type type)
        {
            if (!type.IsClass || type.IsAbstract || !MatchFilter(type)) return;
            ((IContanerRegistration)new SingleRegistration(type, this, Lifetime)).Register(context);
        }

        if (_fromAssemblies == null && _fromAssembly == null)
        {
            foreach (var (typeToken, value) in IContainer.FactoryRegistry)
            {
                var type = Type.GetTypeFromHandle(RuntimeTypeHandle.FromIntPtr(typeToken));
                if (type is null) continue;
                RegisterType(type);
            }
        }
        else if (_fromAssembly != null)
        {
            foreach (var type in _fromAssembly.GetTypes())
            {
                RegisterType(type);
            }
        }
        else
        {
            foreach (var assembly in _fromAssemblies!)
            {
                foreach (var type in assembly.GetTypes())
                {
                    RegisterType(type);
                }
            }
        }
    }
}
