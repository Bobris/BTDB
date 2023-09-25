using System;
using System.Linq;
using System.Reflection;
using BTDB.Collections;
using BTDB.IL;
using BTDB.KVDBLayer;

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
            try
            {
                factory = BuildFactory(_implementationType);
                IContainer.RegisterFactory(_implementationType, factory);
            }
            catch (Exception e)
            {
                throw new BTDBException($"Cannot create factory for {_implementationType.ToSimpleName()}", e);
            }
        }

        context.AddCReg(GetAsTypesFor(_implementationType), PreserveExistingDefaults, UniqueRegistration,
            new()
            {
                Factory = factory, Lifetime = Lifetime, SingletonId = Lifetime == Lifetime.Singleton ? uint.MaxValue : 0
            });
    }

    static Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx, object>> BuildFactory(
        Type implementationType)
    {
        var ci = implementationType
            .GetConstructors(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)
            .OrderByDescending(c => c.GetParameters().Length).First();
        var invoker = ConstructorInvoker.Create(ci);
        var parameters = ci.GetParameters();
        var dependencies = new StructList<(string, Type, MethodInvoker)>();
        foreach (var propertyInfo in implementationType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!propertyInfo.CanWrite) continue;
            var depAttr = propertyInfo.GetCustomAttribute<DependencyAttribute>();
            if (depAttr == null) continue;
            var setter = propertyInfo.GetSetMethod(true);
            if (setter == null) continue;
            var setterInvoker = MethodInvoker.Create(setter);
            dependencies.Add((depAttr.Name ?? propertyInfo.Name, propertyInfo.PropertyType, setterInvoker));
        }

        return (container, ctx) =>
        {
            StructList<(Func<IContainer, IResolvingCtx, object>?, object?)> parameterFactories = new();
            foreach (var parameter in parameters)
            {
                parameterFactories.Add((container.CreateFactory(ctx, parameter.ParameterType, parameter.Name),
                    parameter.DefaultValue));
            }

            StructList<(MethodInvoker, Func<IContainer, IResolvingCtx, object>)> dependencyFactories = new();
            foreach (var dependency in dependencies)
            {
                var f = container.CreateFactory(ctx, dependency.Item2, dependency.Item1);
                if (f == null) continue;
                dependencyFactories.Add((dependency.Item3, f));
            }

            if (dependencyFactories.Count == 0)
            {
                if (parameterFactories.Count == 0)
                {
                    return (_, _) => invoker.Invoke();
                }

                if (parameterFactories.Count == 1)
                {
                    return (container2, ctx2) => invoker.Invoke(parameterFactories[0].Item1?.Invoke(container2, ctx2) ??
                                                                parameterFactories[0].Item2);
                }

                if (parameterFactories.Count == 2)
                {
                    return (container2, ctx2) =>
                    {
                        var p1 = parameterFactories[0].Item1?.Invoke(container2, ctx2) ??
                                 parameterFactories[0].Item2;
                        var p2 = parameterFactories[1].Item1?.Invoke(container2, ctx2) ??
                                 parameterFactories[1].Item2;
                        return invoker.Invoke(p1, p2);
                    };
                }

                return (container2, ctx2) =>
                {
                    Span<object?> parameterInstances = new object[(int)parameterFactories.Count];
                    for (var i = 0; i < parameterFactories.Count; i++)
                    {
                        parameterInstances[i] = parameterFactories[i].Item1?.Invoke(container2, ctx2) ??
                                                parameterFactories[i].Item2;
                    }

                    var res = invoker.Invoke(parameterInstances);
                    return res;
                };
            }

            return (container2, ctx2) =>
            {
                Span<object?> parameterInstances = new object[(int)parameterFactories.Count];
                for (var i = 0; i < parameterFactories.Count; i++)
                {
                    parameterInstances[i] = parameterFactories[i].Item1?.Invoke(container2, ctx2) ??
                                            parameterFactories[i].Item2;
                }

                var res = invoker.Invoke(parameterInstances);
                foreach (var dependencyFactory in dependencyFactories)
                {
                    dependencyFactory.Item1.Invoke(res, dependencyFactory.Item2(container2, ctx2));
                }

                return res;
            };
        };
    }
}
