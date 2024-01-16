using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using BTDB.Collections;
using BTDB.IL;
using BTDB.KVDBLayer;

namespace BTDB.IOC;

public class ContainerImpl : IContainer
{
    internal readonly Dictionary<KeyAndType, CReg> Registrations = new();

    internal readonly object?[] Singletons;

    internal ContainerImpl(ReadOnlySpan<IRegistration> registrations, ContainerVerification containerVerification)
    {
        var context = new ContainerRegistrationContext(Registrations,
            !containerVerification.HasFlag(ContainerVerification.AllTypesAreGenerated),
            containerVerification.HasFlag(ContainerVerification.ReportNotGeneratedTypes));

        foreach (var registration in registrations)
        {
            ((IContanerRegistration)registration).Register(context);
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
                    ctx.SingletonDeepness = 1;
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
        if (ctxImpl.IsBound(type, key as string, out var paramIdx))
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

        if (type.IsDelegate())
        {
            if (IContainer.FactoryRegistry.TryGetValue(type.TypeHandle.Value, out var factory))
            {
                return factory(this, ctx);
            }
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
                        throw new NotSupportedException(
                            "Func<,> with value type argument is not supported, if you define identical delegate type with [Generate], it will be supported.");
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

            if (genericTypeDefinition == typeof(Tuple<>))
            {
                var nestedType = type.GetGenericArguments()[0];
                VerifyNonValueType(nestedType);
                var nestedFactory = CreateFactory(ctx, nestedType, key);
                if (nestedFactory == null) return null;

                return (c, r) => Tuple.Create(nestedFactory(c, r));
            }

            if (genericTypeDefinition == typeof(Tuple<,>))
            {
                var genericArguments = type.GetGenericArguments();
                var nestedType1 = genericArguments[0];
                VerifyNonValueType(nestedType1);
                var nestedType2 = genericArguments[1];
                VerifyNonValueType(nestedType2);
                var nestedFactory1 = CreateFactory(ctx, nestedType1, key);
                if (nestedFactory1 == null) return null;
                var nestedFactory2 = CreateFactory(ctx, nestedType2, key);
                if (nestedFactory2 == null) return null;

                return (c, r) => Tuple.Create(nestedFactory1(c, r), nestedFactory2(c, r));
            }

            if (genericTypeDefinition == typeof(Tuple<,,>))
            {
                var genericArguments = type.GetGenericArguments();
                var nestedType1 = genericArguments[0];
                VerifyNonValueType(nestedType1);
                var nestedType2 = genericArguments[1];
                VerifyNonValueType(nestedType2);
                var nestedType3 = genericArguments[2];
                VerifyNonValueType(nestedType3);
                var nestedFactory1 = CreateFactory(ctx, nestedType1, key);
                if (nestedFactory1 == null) return null;
                var nestedFactory2 = CreateFactory(ctx, nestedType2, key);
                if (nestedFactory2 == null) return null;
                var nestedFactory3 = CreateFactory(ctx, nestedType3, key);
                if (nestedFactory3 == null) return null;

                return (c, r) => Tuple.Create(nestedFactory1(c, r), nestedFactory2(c, r), nestedFactory3(c, r));
            }

            if (genericTypeDefinition == typeof(IEnumerable<>))
            {
                var nestedType = type.GetGenericArguments()[0];
                return CreateArrayFactory(ctx, key, ctxImpl, nestedType);
            }

            if (genericTypeDefinition == typeof(Lazy<>))
            {
                var nestedType = type.GetGenericArguments()[0];
                if (ctxImpl.GetLazyFactory(nestedType, out var lazyFactory))
                {
                    return lazyFactory;
                }

                Func<IContainer, IResolvingCtx?, object>? nestedFactory = null;
                lazyFactory = (c, r) =>
                {
                    // ReSharper disable once AccessToModifiedClosure - solving chicken egg problem
                    return new Lazy<object>(() => nestedFactory!(c, r));
                };
                ctxImpl.RegisterLazyFactory(nestedType, lazyFactory);
                var backupResolvingStack = ctxImpl.BackupResolvingStack();
                nestedFactory = CreateFactory(ctx, nestedType, key);
                ctxImpl.RestoreResolvingStack(backupResolvingStack);
                if (nestedFactory == null) return null;
                return lazyFactory;
            }

            if (genericTypeDefinition == typeof(Nullable<>))
            {
                return null;
            }
        }

        if (type.IsSZArray)
        {
            var nestedType = type.GetElementType()!;
            return CreateArrayFactory(ctx, key, ctxImpl, nestedType);
        }

        return null;
        haveFactory:
        if (ctxImpl.Enumerate >= 0 && cReg.Multi.Count > 1)
        {
            cReg = ctxImpl.Enumerating(cReg);
        }

        ctxImpl.PushResolving(cReg);
        try
        {
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
        finally
        {
            ctxImpl.PopResolving();
        }
    }

    public Func<IContainer, IResolvingCtx?, object?> CreateFactory(Type type)
    {
        var res = CreateFactory(new CreateFactoryCtx(), type, null);
        if (res == null) ThrowNotResolvable(null, type);
        return res;
    }

    Func<IContainer, IResolvingCtx?, object?>? CreateArrayFactory(ICreateFactoryCtx ctx, object? key,
        CreateFactoryCtx ctxImpl, Type nestedType)
    {
        if (nestedType.IsValueType)
            throw new NotSupportedException("IEnumerable<> or Array<> with value type argument is not supported.");
        var enumerableBackup = ctxImpl.StartEnumerate();
        var nestedFactory = CreateFactory(ctx, nestedType, key);
        if (nestedFactory == null)
        {
            return (_, _) => Array.CreateInstance(nestedType, 0);
        }

        StructList<Func<IContainer, IResolvingCtx?, object?>> factories = new();
        factories.Add(nestedFactory);
        while (ctxImpl.IncrementEnumerable())
        {
            factories.Add(CreateFactory(ctx, nestedType, key));
        }

        ctxImpl.FinishEnumerate(enumerableBackup);

        return Factory;

        Array Factory(IContainer c, IResolvingCtx? r)
        {
            var res = Array.CreateInstance(nestedType, factories.Count);
            var i = 0;
            ref var dataRef = ref Unsafe.As<byte, object>(ref MemoryMarshal.GetArrayDataReference(res));
            foreach (var factory in factories)
            {
                Unsafe.Add(ref dataRef, i++) = factory(c, r);
            }

            return res;
        }
    }

    static void VerifyNonValueType(Type nestedType)
    {
        if (nestedType.IsValueType)
            throw new NotSupportedException(
                "Tuple<> with value type argument is not supported.");
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

public sealed class ResolvingCtx : IResolvingCtx
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
