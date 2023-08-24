using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using BTDB.IL;
using BTDB.KVDBLayer;

namespace BTDB.IOC;

public class ContainerImpl : IContainer
{
    internal readonly Dictionary<KeyAndType, CReg> Registrations = new();

    internal readonly object?[] Singletons;

    internal ContainerImpl(ReadOnlySpan<IRegistration> registrations, ContainerVerification containerVerification)
    {
        var context = new ContainerRegistrationContext(this, Registrations);
        foreach (var registration in registrations)
        {
            ((IContanerRegistration)registration).Register(context);
        }

        foreach (var cReg in Registrations.Values)
        {
            if (cReg.Lifetime == Lifetime.Singleton)
            {
                cReg.SingletonId = context.SingletonCount++;
            }
        }

        Singletons = new object[context.SingletonCount];
        Registrations[new(null, typeof(IContainer))] = new()
        {
            Factory = (_, _) => (container, _) => container,
            Lifetime = Lifetime.Singleton,
            SingletonId = uint.MaxValue
        };
        if (containerVerification == ContainerVerification.None) return;
        if ((containerVerification & ContainerVerification.SingletonsUsingOnlySingletons) != 0)
        {
            foreach (var (_, reg) in Registrations)
            {
                if (reg.Lifetime == Lifetime.Singleton)
                {
                    var ctx = new CreateFactoryCtx();
                    ctx.VerifySingletons = true;
                    reg.Factory(this, ctx);
                }
            }
        }
    }

    public object Resolve(Type type)
    {
        return ResolveKeyed(null, type);
    }

    public object ResolveNamed(string name, Type type)
    {
        return ResolveKeyed(name, type);
    }

    public object ResolveKeyed(object? key, Type type)
    {
        var factory = CreateFactory(new CreateFactoryCtx(), type, key);
        if (factory == null) ThrowNotResolvable(key, type);
        return factory(this, null);
    }

    public object? ResolveOptional(Type type)
    {
        return ResolveOptionalKeyed(null, type);
    }

    public object? ResolveOptionalNamed(string name, Type type)
    {
        return ResolveOptionalKeyed(name, type);
    }

    public object? ResolveOptionalKeyed(object key, Type type)
    {
        var factory = CreateFactory(new CreateFactoryCtx(), type, key);
        return factory?.Invoke(this, null);
    }

    public Func<IContainer, IResolvingCtx?, object?>? CreateFactory(ICreateFactoryCtx ctx, Type type, object? key)
    {
        var ctxImpl = (CreateFactoryCtx)ctx;
        if (Registrations.TryGetValue(new(key, type), out var cReg))
        {
            goto haveFactory;
        }

        if (Registrations.TryGetValue(new(null, type), out cReg))
        {
            goto haveFactory;
        }

        if (type.IsGenericType)
        {
            var genericTypeDefinition = type.GetGenericTypeDefinition();
            if (genericTypeDefinition == typeof(Func<>))
            {
                var nestedType = type.GetGenericArguments()[0];
                var nestedFactory = CreateFactory(ctx, nestedType, key);
                if (nestedFactory == null)
                {
                    throw new BTDBException("Unable to resolve dependency of Func<> " + nestedType.ToSimpleName());
                }

                return (c, r) => () => nestedFactory(c, r);
            }

            if (genericTypeDefinition == typeof(IEnumerable<>))
            {
                var nestedType = type.GetGenericArguments()[0];
                var enumerableBackup = ctxImpl.Enumerate;
                ctxImpl.Enumerate = true;
                var nestedFactory = CreateFactory(ctx, nestedType, key);
                if (nestedFactory == null)
                {
                    var emptyArray = Array.CreateInstance(nestedType, 0);
                    return (c, r) => emptyArray;
                }

                ctxImpl.Enumerate = enumerableBackup;
            }
        }

        if (type.IsInterface || type.IsAbstract)
            return null;
        throw new NotImplementedException();
        haveFactory:
        if (cReg.Lifetime == Lifetime.Singleton && cReg.SingletonId != uint.MaxValue)
        {
            ctxImpl.SingletonDeepness++;
            try
            {
                var f = cReg.Factory(this, ctx);
                var sid = cReg.SingletonId;
                return (container, resolvingCtx) =>
                {
                    ref var singleton = ref ((ContainerImpl)container).Singletons[sid];
                    var instance = Volatile.Read(ref singleton);
                    if (instance != null)
                    {
                        if (instance.GetType() == typeof(SingletonLocker))
                        {
                            lock (instance)
                            {
                            }

                            return Volatile.Read(ref singleton);
                        }

                        return instance;
                    }

                    var mySingletonLocker = new SingletonLocker();
                    Monitor.Enter(mySingletonLocker);
                    instance = Interlocked.CompareExchange(ref singleton, mySingletonLocker, null);
                    if (instance == null)
                    {
                        instance = f(container, resolvingCtx);
                        Volatile.Write(ref singleton, instance);
                        Monitor.Exit(mySingletonLocker);
                        return instance;
                    }

                    if (instance!.GetType() == typeof(SingletonLocker))
                    {
                        lock (instance)
                        {
                        }

                        return Volatile.Read(ref singleton);
                    }

                    return instance;
                };
            }
            finally
            {
                ctxImpl.SingletonDeepness--;
            }
        }

        if (ctxImpl.VerifySingletons && ctxImpl.SingletonDeepness > 0 && cReg.SingletonId != uint.MaxValue)
        {
            throw new BTDBException("Transient dependency " + new KeyAndType(key, type));
        }

        return cReg.Factory(this, ctx);
    }

    class SingletonLocker
    {
    }

    [DoesNotReturn]
    static void ThrowNotResolvable(object? key, Type type)
    {
        if (key == null)
        {
            throw new ArgumentException($"Type {type.ToSimpleName()} cannot be resolved");
        }

        throw new ArgumentException($"Type {type.ToSimpleName()} with key {key} cannot be resolved");
    }
}
