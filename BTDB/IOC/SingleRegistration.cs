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
    readonly bool _withFallback;

    public void SingleInstance()
    {
        _lifetime = Lifetime.Singleton;
    }

    public void Scoped()
    {
        _lifetime = Lifetime.Scoped;
    }

    public Lifetime Lifetime => _lifetime;

    public SingleRegistration(Type implementationType, bool withFallback = false)
    {
        _implementationType = implementationType;
        _withFallback = withFallback;
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
            if (context.ReportNotGeneratedTypes)
            {
                Console.WriteLine(_implementationType.ToSimpleName());
                return;
            }

            if (context.AllowReflectionFallback || _withFallback)
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
            else
            {
                throw new BTDBException(
                    $"Factory for {_implementationType.ToSimpleName()} is not registered. Add [Generate] attribute to sourcecode generate it.");
            }
        }

        context.AddCReg(GetAsTypesFor(_implementationType), PreserveExistingDefaults, UniqueRegistration,
            new()
            {
                Factory = factory, Lifetime = Lifetime,
                ScopedId = Lifetime == Lifetime.Scoped ? uint.MaxValue : 0
            });
    }

    public void RegisterForServiceCollection(ServiceCollectionRegistrationContext context)
    {
        context.Add(GetAsTypesFor(_implementationType), _lifetime.ToServiceLifetime());
    }

    static Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx, object>> BuildFactory(
        Type implementationType)
    {
        var ci = implementationType
            .GetConstructors(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)
            .Where(ci => ci.GetParameters().All(p => p.ParameterType != implementationType))
            .OrderByDescending(c => c.GetParameters().Length).First();
        return BuildFactory(implementationType, ci);
    }

    static bool IsNullable(ParameterInfo parameter)
    {
        if (Nullable.GetUnderlyingType(parameter.ParameterType) != null) return true;
        return !parameter.ParameterType.IsValueType &&
               new NullabilityInfoContext().Create(parameter).WriteState == NullabilityState.Nullable;
    }

    internal static Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx, object>> BuildFactory(
        Type implementationType, ConstructorInfo constructorInfo)
    {
        var invoker = ConstructorInvoker.Create(constructorInfo);
        var parameters = constructorInfo.GetParameters();
        var dependencies = new StructList<(string, Type, MethodInvoker)>();
        foreach (var propertyInfo in implementationType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var depAttr = propertyInfo.GetCustomAttribute<DependencyAttribute>();
            if (depAttr == null) continue;
            // A private setter of a base class is visible only through the base class's own PropertyInfo, CanWrite of the
            // inherited one is false
            var setter = propertyInfo.GetSetMethod(true) ??
                         propertyInfo.DeclaringType!.GetProperty(propertyInfo.Name,
                             BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance |
                             BindingFlags.DeclaredOnly)?.GetSetMethod(true);
            if (setter == null) continue;
            var setterInvoker = MethodInvoker.Create(setter);
            dependencies.Add((depAttr.Name ?? propertyInfo.Name, propertyInfo.PropertyType, setterInvoker));
        }

        return (container, ctx) =>
        {
            StructList<(Func<IContainer, IResolvingCtx, object>?, object?)> parameterFactories = new();
            foreach (var parameter in parameters)
            {
                var factory = container.CreateFactory(ctx, parameter.ParameterType, parameter.Name);
                // Like generated factories: a parameter without a default value is null when nullable, else required.
                // Its DefaultValue would be DBNull.Value, which the constructor cannot take.
                var fallback = parameter.HasDefaultValue ? parameter.DefaultValue : null;
                if (factory == null && !parameter.HasDefaultValue && !IsNullable(parameter))
                    throw new ArgumentException(
                        $"Cannot resolve {parameter.ParameterType.ToSimpleName()} {parameter.Name} parameter of {implementationType.ToSimpleName()}");
                parameterFactories.Add((factory, fallback));
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
                return parameterFactories.Count switch
                {
                    0 => (_, _) => invoker.Invoke(),
                    1 => (container2, ctx2) => invoker.Invoke(parameterFactories[0].Item1?.Invoke(container2, ctx2) ??
                                                              parameterFactories[0].Item2),
                    2 => (container2, ctx2) =>
                    {
                        var p1 = parameterFactories[0].Item1?.Invoke(container2, ctx2) ?? parameterFactories[0].Item2;
                        var p2 = parameterFactories[1].Item1?.Invoke(container2, ctx2) ?? parameterFactories[1].Item2;
                        return invoker.Invoke(p1, p2);
                    },
                    3 => (container2, ctx2) =>
                    {
                        var p1 = parameterFactories[0].Item1?.Invoke(container2, ctx2) ?? parameterFactories[0].Item2;
                        var p2 = parameterFactories[1].Item1?.Invoke(container2, ctx2) ?? parameterFactories[1].Item2;
                        var p3 = parameterFactories[2].Item1?.Invoke(container2, ctx2) ?? parameterFactories[2].Item2;
                        return invoker.Invoke(p1, p2, p3);
                    },
                    _ => (container2, ctx2) =>
                    {
                        Span<object?> parameterInstances = new object[(int)parameterFactories.Count];
                        for (var i = 0; i < parameterFactories.Count; i++)
                        {
                            parameterInstances[i] = parameterFactories[i].Item1?.Invoke(container2, ctx2) ??
                                                    parameterFactories[i].Item2;
                        }

                        var res = invoker.Invoke(parameterInstances);
                        return res;
                    }
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
