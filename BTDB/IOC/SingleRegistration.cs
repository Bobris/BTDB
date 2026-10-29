using System;
using System.Collections.Generic;
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

    /// The key a constructor parameter is resolved by, named like generated factories name it.
    static object? DependencyKey(ParameterInfo parameter)
    {
        return parameter.GetCustomAttribute<DependencyAttribute>()?.Name ??
               parameter.GetCustomAttribute<Microsoft.Extensions.DependencyInjection.FromKeyedServicesAttribute>()
                   ?.Key ?? parameter.Name;
    }

    // The default of a nullable enum parameter is reported as its underlying number, which the constructor cannot take
    static object? DefaultValueOf(ParameterInfo parameter)
    {
        var value = parameter.DefaultValue;
        var enumType = Nullable.GetUnderlyingType(parameter.ParameterType) ?? parameter.ParameterType;
        if (value != null && enumType.IsEnum && value.GetType() != enumType) return Enum.ToObject(enumType, value);
        return value;
    }

    static bool IsNullable(ParameterInfo parameter)
    {
        if (Nullable.GetUnderlyingType(parameter.ParameterType) != null) return true;
        return !parameter.ParameterType.IsValueType &&
               new NullabilityInfoContext().Create(parameter).WriteState == NullabilityState.Nullable;
    }

    /// The [Dependency] properties and fields of a type and its base types, like generated factories inject them: a
    /// member hidden or overridden in a derived type is taken from the most derived one.
    static StructList<(string, Type, Action<object, object?>)> CollectDependencies(Type implementationType)
    {
        const BindingFlags declared = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance |
                                      BindingFlags.DeclaredOnly;
        var dependencies = new StructList<(string, Type, Action<object, object?>)>();
        var seen = new HashSet<string>();
        for (var type = implementationType; type != null && type != typeof(object); type = type.BaseType)
        {
            foreach (var propertyInfo in type.GetProperties(declared))
            {
                if (!seen.Add(propertyInfo.Name) || propertyInfo.GetIndexParameters().Length != 0) continue;
                var depAttr = propertyInfo.GetCustomAttribute<DependencyAttribute>();
                if (depAttr == null) continue;
                // An override may declare only the getter, the setter is then declared in a base class
                MethodInfo? setter = null;
                for (var t = type; setter == null && t != null; t = t.BaseType)
                    setter = t.GetProperty(propertyInfo.Name, declared)?.GetSetMethod(true);
                if (setter == null) continue;
                var setterInvoker = MethodInvoker.Create(setter);
                dependencies.Add((depAttr.Name ?? propertyInfo.Name, propertyInfo.PropertyType,
                    (obj, value) => setterInvoker.Invoke(obj, value)));
            }

            foreach (var fieldInfo in type.GetFields(declared))
            {
                if (!seen.Add(fieldInfo.Name)) continue;
                var depAttr = fieldInfo.GetCustomAttribute<DependencyAttribute>();
                if (depAttr == null) continue;
                dependencies.Add((depAttr.Name ?? fieldInfo.Name, fieldInfo.FieldType, fieldInfo.SetValue));
            }
        }

        return dependencies;
    }

    internal static Func<IContainer, ICreateFactoryCtx, Func<IContainer, IResolvingCtx, object>> BuildFactory(
        Type implementationType, ConstructorInfo constructorInfo)
    {
        var invoker = ConstructorInvoker.Create(constructorInfo);
        var parameters = constructorInfo.GetParameters();
        var dependencies = CollectDependencies(implementationType);

        return (container, ctx) =>
        {
            StructList<(Func<IContainer, IResolvingCtx, object>?, object?)> parameterFactories = new();
            foreach (var parameter in parameters)
            {
                var factory = container.CreateFactory(ctx, parameter.ParameterType, DependencyKey(parameter));
                // Like generated factories: a parameter without a default value is null when nullable, else required.
                // Its DefaultValue would be DBNull.Value, which the constructor cannot take.
                var fallback = parameter.HasDefaultValue ? DefaultValueOf(parameter) : null;
                if (factory == null && !parameter.HasDefaultValue && !IsNullable(parameter))
                    throw new ArgumentException(
                        $"Cannot resolve {parameter.ParameterType.ToSimpleName()} {parameter.Name} parameter of {implementationType.ToSimpleName()}");
                parameterFactories.Add((factory, fallback));
            }

            StructList<(Action<object, object?>, Func<IContainer, IResolvingCtx, object>)> dependencyFactories = new();
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
                    dependencyFactory.Item1(res, dependencyFactory.Item2(container2, ctx2));
                }

                return res;
            };
        };
    }
}
