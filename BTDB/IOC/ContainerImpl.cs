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
        if (ctxImpl.IsBound(type, out var paramIdx))
        {
            return (c, r) => r!.Get(paramIdx);
        }

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
                if (nestedFactory == null) return null;

                return (c, r) => () => nestedFactory(c, r);
            }

            if (genericTypeDefinition == typeof(Func<,>))
            {
                var genericArguments = type.GetGenericArguments();
                var nestedType = genericArguments[1];
                if (genericArguments[0] == typeof(IContainer))
                {
                    var nestedFactory = CreateFactory(ctx, nestedType, key);
                    if (nestedFactory == null) return null;

                    return (_, r) => (IContainer c) => nestedFactory(c, r);
                }
                else
                {
                    if (genericArguments[0].IsValueType)
                        throw new NotSupportedException("Func<,> with value type argument is not supported");
                    var hasResolvingCtx = ctxImpl.HasResolvingCtx();
                    var p1Idx = ctxImpl.AddInstanceToCtx(genericArguments[0]);
                    var nestedFactory = CreateFactory(ctx, nestedType, key);
                    if (nestedFactory == null) return null;
                    if (hasResolvingCtx)
                    {
                        return (c, r) => (object p1) =>
                        {
                            var p1Backup = r.Exchange(p1Idx, p1);
                            try
                            {
                                return nestedFactory(c, r);
                            }
                            finally
                            {
                                r.Set(p1Idx, p1Backup);
                            }
                        };
                    }
                    else
                    {
                        var paramSize = ctxImpl.GetParamSize();
                        return (c, _) => (object p1) =>
                        {
                            var r = new ResolvingCtx(paramSize);
                            r.Set(p1Idx, p1);
                            return nestedFactory(c, r);
                        };
                    }
                }
            }

            if (genericTypeDefinition == typeof(IEnumerable<>))
            {
                var nestedType = type.GetGenericArguments()[0];
                var enumerableBackup = ctxImpl.Enumerate;
                ctxImpl.Enumerate = true;
                var nestedFactory = CreateFactory(ctx, nestedType, key);
                if (nestedFactory == null)
                {
                    return (_, _) => Array.Empty<object>();
                }

                ctxImpl.Enumerate = enumerableBackup;
            }
            throw new NotImplementedException();
        }

        return null;
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

sealed class ResolvingCtx : IResolvingCtx
{
    readonly object[] _params;

    public object Get(int idx)
    {
        return _params[idx];
    }

    public void Set(int idx, object value)
    {
        _params[idx] = value;
    }

    public object Exchange(int idx, object value)
    {
        var old = _params[idx];
        _params[idx] = value;
        return old;
    }

    internal ResolvingCtx(int paramSize)
    {
        _params = new object[paramSize];
    }
}
