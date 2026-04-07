using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Collections;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.Locks;
using BTDB.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace BTDB.IOC;

public class ContainerImpl : IContainer
{
    internal readonly Dictionary<KeyAndType, CReg> Registrations;

    internal readonly object?[] Singletons;
    readonly ContainerImpl _root;
    readonly IServiceProvider? _serviceProvider;
    readonly IServiceProviderIsKeyedService? _serviceProviderIsKeyedService;
    readonly ServiceProviderIntegration? _serviceProviderIntegration;
    readonly AsyncServiceScope? _serviceScope;
    readonly RefDictionary<uint, object?> _scopedInstances = new();
    SeqLock _scopedInstancesLock;
    OwnedInstanceNode? _ownedInstances;
    int _disposeState;

    internal ContainerImpl(ReadOnlySpan<IRegistration> registrations, ContainerVerification containerVerification,
        IServiceProvider? serviceProvider, ServiceProviderIntegration? serviceProviderIntegration)
    {
        _root = this;
        _serviceProvider = serviceProvider;
        _serviceProviderIsKeyedService = serviceProvider?.GetService<IServiceProviderIsKeyedService>();
        _serviceProviderIntegration = serviceProviderIntegration;
        Registrations = new();

        var context = new ContainerRegistrationContext(Registrations,
            !containerVerification.HasFlag(ContainerVerification.AllTypesAreGenerated),
            containerVerification.HasFlag(ContainerVerification.ReportNotGeneratedTypes));

        foreach (var registration in registrations)
        {
            ((IContanerRegistration)registration).Register(context);
            if (registration is SingleInstanceRegistration singleInstanceRegistration)
            {
                TrackOwnedInstance(singleInstanceRegistration.Instance);
            }
        }

        Singletons = new object[context.SingletonCount];
        Registrations[new(null, typeof(IContainer))] = new()
        {
            Factory = (_, _) => (container, _) => container,
            Lifetime = Lifetime.Singleton,
            SingletonId = uint.MaxValue,
            ScopedId = uint.MaxValue
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

    ContainerImpl(ContainerImpl parent, IServiceProvider? serviceProvider, AsyncServiceScope? serviceScope)
    {
        _root = parent._root;
        Registrations = _root.Registrations;
        Singletons = _root.Singletons;
        _serviceScope = serviceScope;
        _serviceProvider = serviceProvider ?? serviceScope?.ServiceProvider ?? parent._serviceProvider;
        _serviceProviderIsKeyedService = _serviceProvider?.GetService<IServiceProviderIsKeyedService>();
        _serviceProviderIntegration = parent._serviceProviderIntegration;
    }

    public IContainer CreateScope()
    {
        ThrowIfDisposed();
        return new ContainerImpl(this, null, _serviceProvider?.CreateAsyncScope());
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
        var factory = CreateFactory(new CreateFactoryCtx { ForbidKeylessFallback = key != null }, type, key);
        if (factory is null)
        {
            ThrowNotResolvable(key, type);
        }

        return factory(this, null)!;
    }

    public object? ResolveOptional(Type type)
    {
        return ResolveOptionalKeyed(null, type);
    }

    public object? ResolveOptionalNamed(string name, Type type)
    {
        return ResolveOptionalKeyed(name, type);
    }

    public object? ResolveOptionalKeyed(object? key, Type type)
    {
        var factory = CreateFactory(new CreateFactoryCtx { ForbidKeylessFallback = key != null }, type, key);
        return factory?.Invoke(this, null);
    }

    bool HasRegistration(Type type, object? key, bool allowKeylessFallback)
    {
        return Registrations.ContainsKey(new(key, type)) ||
               (allowKeylessFallback && Registrations.ContainsKey(new(null, type)));
    }

    bool ShouldResolveFromServiceProvider(Type type, object? key, bool allowKeylessFallback)
    {
        if (_serviceProviderIsKeyedService == null) return false;
        if (HasRegistration(type, key, allowKeylessFallback)) return false;
        if (type.IsAssignableTo(typeof(IEnumerable)) && type.IsGenericType)
        {
            var genericType = type.GenericTypeArguments[0];
            if (HasRegistration(genericType, key, allowKeylessFallback)) return false;
            return key != null
                ? _serviceProviderIsKeyedService.IsKeyedService(genericType, key)
                : _serviceProviderIsKeyedService.IsService(genericType);
        }

        return true;
    }

    public Func<IContainer, IResolvingCtx?, object?>? CreateFactory(ICreateFactoryCtx ctx, Type type, object? key)
    {
        return CreateFactory((CreateFactoryCtx)ctx, type, key);
    }

    Func<IContainer, IResolvingCtx?, object?>? CreateFactory(CreateFactoryCtx ctxImpl, Type type, object? key)
    {
        if (ctxImpl.IsBound(type, key as string, out var paramIdx))
        {
            return (c, r) => r!.Get(paramIdx);
        }

        if (Registrations.TryGetValue(new(key, type), out var cReg))
        {
            ctxImpl.ForbidKeylessFallback = false;
            return CreateFactoryFromRegistration(ctxImpl, cReg, key, type);
        }

        if (!ctxImpl.ForbidKeylessFallback && Registrations.TryGetValue(new(null, type), out cReg))
        {
            return CreateFactoryFromRegistration(ctxImpl, cReg, key, type);
        }

        if (_serviceProviderIsKeyedService != null)
        {
            if (key != null && _serviceProviderIsKeyedService.IsKeyedService(type, key) &&
                ShouldResolveFromServiceProvider(type, key, !ctxImpl.ForbidKeylessFallback))
            {
                ctxImpl.ForbidKeylessFallback = false;
                return (c, r) => ((ContainerImpl)c).ResolveFromServiceProvider(type, key);
            }

            if ((key == null || !ctxImpl.ForbidKeylessFallback) && _serviceProviderIsKeyedService.IsService(type) &&
                ShouldResolveFromServiceProvider(type, key, !ctxImpl.ForbidKeylessFallback))
            {
                return (c, r) => ((ContainerImpl)c).ResolveFromServiceProvider(type, null);
            }
        }

        if (type.IsDelegate())
        {
            if (ctxImpl.VerifySingletons) return (_, _) => null;
            if (IContainer.FactoryRegistry.TryGetValue(type.TypeHandle.Value, out var factory))
            {
                return factory(this, ctxImpl);
            }
        }

        if (type.IsGenericType)
        {
            var genericTypeDefinition = type.GetGenericTypeDefinition();
            if (genericTypeDefinition == typeof(Func<>))
            {
                if (ctxImpl.VerifySingletons) return (_, _) => null;
                var nestedType = type.GetGenericArguments()[0];
                var nestedFactory = CreateFactory(ctxImpl, nestedType, key);
                if (nestedFactory == null) return null;

                return (c, r) =>
                {
                    var res = () => nestedFactory(c, r);
                    RawData.SetMethodTable(res, type);
                    return res;
                };
            }

            if (genericTypeDefinition == typeof(Func<,>))
            {
                if (ctxImpl.VerifySingletons) return (_, _) => null;
                var genericArguments = type.GetGenericArguments();
                var nestedType = genericArguments[1];
                if (genericArguments[0] == typeof(IContainer))
                {
                    var nestedFactory = CreateFactory(ctxImpl, nestedType, key);
                    if (nestedFactory == null) return null;

                    return (_, r) =>
                    {
                        var res = (IContainer c) => nestedFactory(c, r);
                        RawData.SetMethodTable(res, type);
                        return res;
                    };
                }
                else
                {
                    if (genericArguments[0].IsValueType)
                        throw new NotSupportedException(
                            "Func<,> with value type argument is not supported, if you define identical delegate type with [Generate], it will be supported.");
                    using var resolvingCtxRestorer = ctxImpl.ResolvingCtxRestorer();
                    var hasResolvingCtx = ctxImpl.HasResolvingCtx();
                    var p1Idx = ctxImpl.AddInstanceToCtx(genericArguments[0]);
                    var nestedFactory = CreateFactory(ctxImpl, nestedType, key);
                    if (nestedFactory == null) return null;
                    if (hasResolvingCtx)
                    {
                        return (c, r) =>
                        {
                            var res = (object p1) =>
                            {
                                var p1Backup = r!.Exchange(p1Idx, p1);
                                try
                                {
                                    return nestedFactory(c, r);
                                }
                                finally
                                {
                                    r.Set(p1Idx, p1Backup);
                                }
                            };
                            RawData.SetMethodTable(res, type);
                            return res;
                        };
                    }
                    else
                    {
                        var paramSize = ctxImpl.GetParamSize();
                        return (c, _) =>
                        {
                            var res = (object p1) =>
                            {
                                var r = new ResolvingCtx(paramSize);
                                r.Set(p1Idx, p1);
                                return nestedFactory(c, r);
                            };
                            RawData.SetMethodTable(res, type);
                            return res;
                        };
                    }
                }
            }

            if (genericTypeDefinition == typeof(Tuple<>))
            {
                var nestedType = type.GetGenericArguments()[0];
                VerifyNonValueType(nestedType);
                var nestedFactory = CreateFactory(ctxImpl, nestedType, key);
                if (nestedFactory == null) return null;

                return (c, r) =>
                {
                    var res = Tuple.Create(nestedFactory(c, r));
                    RawData.SetMethodTable(res, type);
                    return res;
                };
            }

            if (genericTypeDefinition == typeof(Tuple<,>))
            {
                var genericArguments = type.GetGenericArguments();
                var nestedType1 = genericArguments[0];
                VerifyNonValueType(nestedType1);
                var nestedType2 = genericArguments[1];
                VerifyNonValueType(nestedType2);
                var nestedFactory1 = CreateFactory(ctxImpl, nestedType1, key);
                if (nestedFactory1 == null) return null;
                var nestedFactory2 = CreateFactory(ctxImpl, nestedType2, key);
                if (nestedFactory2 == null) return null;

                return (c, r) =>
                {
                    var res = Tuple.Create(nestedFactory1(c, r), nestedFactory2(c, r));
                    RawData.SetMethodTable(res, type);
                    return res;
                };
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
                var nestedFactory1 = CreateFactory(ctxImpl, nestedType1, key);
                if (nestedFactory1 == null) return null;
                var nestedFactory2 = CreateFactory(ctxImpl, nestedType2, key);
                if (nestedFactory2 == null) return null;
                var nestedFactory3 = CreateFactory(ctxImpl, nestedType3, key);
                if (nestedFactory3 == null) return null;

                return (c, r) =>
                {
                    var res = Tuple.Create(nestedFactory1(c, r), nestedFactory2(c, r), nestedFactory3(c, r));
                    RawData.SetMethodTable(res, type);
                    return res;
                };
            }

            if (genericTypeDefinition == typeof(IEnumerable<>))
            {
                var nestedType = type.GetGenericArguments()[0];
                return CreateArrayFactory(ctxImpl, key, nestedType, ctxImpl.ForbidKeylessFallback);
            }

            if (genericTypeDefinition == typeof(Lazy<>))
            {
                var nestedType = type.GetGenericArguments()[0];
                if (ctxImpl.GetLazyFactory(nestedType, out var lazyFactory))
                {
                    return lazyFactory;
                }

                Func<IContainer, IResolvingCtx?, object?>? nestedFactory = null;
                lazyFactory = (c, r) =>
                {
                    var res = new Lazy<object>(() => nestedFactory!(c, r));
                    RawData.SetMethodTable(res, type);
                    return res;
                };
                ctxImpl.RegisterLazyFactory(nestedType, lazyFactory);
                var backupResolvingStack = ctxImpl.BackupResolvingStack();
                try
                {
                    nestedFactory = CreateFactory(ctxImpl, nestedType, key);
                }
                finally
                {
                    ctxImpl.RestoreResolvingStack(backupResolvingStack);
                }

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
            return CreateArrayFactory(ctxImpl, key, nestedType, ctxImpl.ForbidKeylessFallback);
        }

        return null;
    }

    object ResolveFromServiceProvider(Type type, object? key)
    {
        if (_serviceProvider == null)
        {
            ThrowNotResolvable(key, type);
        }

        if (_serviceProviderIntegration == null)
        {
            return key == null
                ? _serviceProvider.GetRequiredService(type)
                : _serviceProvider.GetRequiredKeyedService(type, key);
        }

        return _serviceProviderIntegration.ResolveRequiredFromServiceProvider(_serviceProvider, type, key);
    }

    Func<IContainer, IResolvingCtx?, object?>? CreateFactoryFromRegistration(CreateFactoryCtx ctxImpl, CReg cReg, object? key,
        Type type)
    {
        if (ctxImpl.Enumerate >= 0 && cReg.Multi.Count > 1)
        {
            cReg = ctxImpl.Enumerating(cReg);
        }

        ctxImpl.PushResolving(cReg);
        try
        {
            if (cReg.Lifetime == Lifetime.Singleton && cReg.SingletonId != uint.MaxValue)
            {
                if (ctxImpl.VerifySingletons) return (_, _) => null;
                var rootContainer = _root;
                var singletonInstance = Volatile.Read(ref Singletons[cReg.SingletonId]);
                if (singletonInstance is SingletonLocker)
                {
                    return (_, _) => rootContainer.WaitForSingleton(cReg.SingletonId);
                }

                if (singletonInstance != null)
                {
                    return (_, _) => singletonInstance;
                }

                var singletonFactoryCache = Volatile.Read(ref cReg.SingletonFactoryCache);
                if (singletonFactoryCache != null) return singletonFactoryCache;

                Func<IContainer, IResolvingCtx?, object>? f = null;

                var ff = (IContainer container, IResolvingCtx? resolvingCtx) =>
                {
                    ref var singleton = ref rootContainer.Singletons[cReg.SingletonId];
                    var instance = Volatile.Read(ref singleton);
                    if (instance != null)
                    {
                        if (instance is SingletonLocker)
                        {
                            return rootContainer.WaitForSingleton(cReg.SingletonId);
                        }

                        return instance;
                    }

                    var mySingletonLocker = new SingletonLocker();
                    Monitor.Enter(mySingletonLocker);
                    try
                    {
                        instance = Interlocked.CompareExchange(ref singleton, mySingletonLocker, null);
                        if (instance == null)
                        {
                            Func<IContainer, IResolvingCtx?, object>? f1;
                            while ((f1 = Volatile.Read(ref f)) == null)
                            {
                                Thread.Yield();
                            }

                            try
                            {
                                instance = f1(rootContainer, resolvingCtx);
                                rootContainer.TrackOwnedInstance(instance);
                                Volatile.Write(ref singleton, instance);
                                cReg.SingletonFactoryCache = null;
                                return instance;
                            }
                            catch
                            {
                                Volatile.Write(ref singleton, null);
                                throw;
                            }
                        }
                    }
                    finally
                    {
                        Monitor.Exit(mySingletonLocker);
                    }

                    if (instance is SingletonLocker)
                    {
                        return rootContainer.WaitForSingleton(cReg.SingletonId);
                    }

                    return instance;
                };
                if (Interlocked.CompareExchange(ref cReg.SingletonFactoryCache, ff, null) == null)
                {
                    using var resolvingCtxRestorer = ctxImpl.ResolvingCtxRestorer();
                    using var enumerableRestorer = ctxImpl.EnumerableRestorer();
                    ctxImpl.SingletonDeepness++;
                    try
                    {
                        f = cReg.Factory(rootContainer, ctxImpl);
                    }
                    finally
                    {
                        ctxImpl.SingletonDeepness--;
                    }
                }

                return cReg.SingletonFactoryCache;
            }

            if (cReg.Lifetime == Lifetime.Scoped && cReg.ScopedId != uint.MaxValue)
            {
                if (ctxImpl.VerifySingletons && ctxImpl.SingletonDeepness > 0)
                {
                    throw new BTDBException("Scoped dependency " + new KeyAndType(key, type));
                }

                if (TryGetScopedValue(cReg.ScopedId, out var scopedInstance))
                {
                    if (scopedInstance is ScopedLocker)
                    {
                        return (container, _) => ((ContainerImpl)container).WaitForScoped(cReg.ScopedId);
                    }

                    return (_, _) => scopedInstance;
                }

                var scopedFactoryCache = Volatile.Read(ref cReg.ScopedFactoryCache);
                if (scopedFactoryCache != null) return scopedFactoryCache;

                Func<IContainer, IResolvingCtx?, object>? f = null;
                var ff = (IContainer container, IResolvingCtx? resolvingCtx) =>
                {
                    var currentContainer = (ContainerImpl)container;
                    if (currentContainer.TryGetScopedValue(cReg.ScopedId, out var instance))
                    {
                        if (instance is ScopedLocker)
                        {
                            return currentContainer.WaitForScoped(cReg.ScopedId);
                        }

                        return instance;
                    }

                    var myScopedLocker = new ScopedLocker();
                    Monitor.Enter(myScopedLocker);
                    try
                    {
                        currentContainer._scopedInstancesLock.StartWrite();
                        try
                        {
                            ref var instanceRef = ref currentContainer._scopedInstances.GetOrFakeValueRef(cReg.ScopedId,
                                out var found);
                            if (!found)
                            {
                                instanceRef = myScopedLocker;
                                instance = null;
                            }
                            else
                            {
                                instance = instanceRef;
                            }
                        }
                        finally
                        {
                            currentContainer._scopedInstancesLock.EndWrite();
                        }

                        if (instance == null)
                        {
                            Func<IContainer, IResolvingCtx?, object>? f1;
                            while ((f1 = Volatile.Read(ref f)) == null)
                            {
                                Thread.Yield();
                            }

                            try
                            {
                                instance = f1(container, resolvingCtx);
                                currentContainer.TrackOwnedInstance(instance);
                                currentContainer.SetScopedValue(cReg.ScopedId, instance);
                                cReg.ScopedFactoryCache = null;
                                return instance;
                            }
                            catch
                            {
                                currentContainer.RemoveScopedValue(cReg.ScopedId);
                                throw;
                            }
                        }
                    }
                    finally
                    {
                        Monitor.Exit(myScopedLocker);
                    }

                    if (instance is ScopedLocker)
                    {
                        return currentContainer.WaitForScoped(cReg.ScopedId);
                    }

                    return instance;
                };
                if (Interlocked.CompareExchange(ref cReg.ScopedFactoryCache, ff, null) == null)
                {
                    f = cReg.Factory(this, ctxImpl);
                }

                return cReg.ScopedFactoryCache;
            }

            if (ctxImpl.VerifySingletons && ctxImpl.SingletonDeepness > 0 && cReg.SingletonId != uint.MaxValue)
            {
                throw new BTDBException("Transient dependency " + new KeyAndType(key, type));
            }

            return cReg.Factory(this, ctxImpl);
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

    internal object ResolveRegistration(Type type, object? key, int registrationIndex)
    {
        ThrowIfDisposed();
        var factory = CreateRegistrationFactory(new CreateFactoryCtx { ForbidKeylessFallback = key != null }, type, key,
            registrationIndex);
        if (factory == null)
        {
            ThrowNotResolvable(key, type);
        }

        return factory(this, null)!;
    }

    internal IContainer CreateScopeForServiceProvider(IServiceProvider serviceProvider)
    {
        ThrowIfDisposed();
        return new ContainerImpl(this, serviceProvider, null);
    }

    Func<IContainer, IResolvingCtx?, object?>? CreateRegistrationFactory(CreateFactoryCtx ctxImpl, Type type, object? key,
        int registrationIndex)
    {
        if (registrationIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(registrationIndex));

        var enumerableBackup = ctxImpl.StartEnumerate();
        try
        {
            var factory = CreateFactory(ctxImpl, type, key);
            if (factory == null) return null;

            for (var i = 0; i < registrationIndex; i++)
            {
                if (!ctxImpl.IncrementEnumerable()) return null;
                factory = CreateFactory(ctxImpl, type, key);
                if (factory == null) return null;
            }

            return factory;
        }
        finally
        {
            ctxImpl.FinishEnumerate(enumerableBackup);
        }
    }

    Func<IContainer, IResolvingCtx?, object?>? CreateArrayFactory(CreateFactoryCtx ctxImpl, object? key,
        Type nestedType, bool forbidKeylessFallback)
    {
        if (nestedType.IsValueType)
            throw new NotSupportedException("IEnumerable<> or Array<> with value type argument is not supported.");
        var enumerableBackup = ctxImpl.StartEnumerate();
        var nestedFactory = CreateFactory(ctxImpl, nestedType, key);
        if (nestedFactory == null)
        {
            ctxImpl.FinishEnumerate(enumerableBackup);
            if (forbidKeylessFallback) return null;
            return (_, _) => Array.CreateInstance(nestedType, 0);
        }

        StructList<Func<IContainer, IResolvingCtx?, object?>> factories = new();
        factories.Add(nestedFactory);
        while (ctxImpl.IncrementEnumerable())
        {
            factories.Add(CreateFactory(ctxImpl, nestedType, key)!);
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

    bool TryGetScopedValue(uint scopedId, out object? instance)
    {
        return _scopedInstances.TryGetValueSeqLock(scopedId, out instance, ref _scopedInstancesLock);
    }

    void SetScopedValue(uint scopedId, object? instance)
    {
        _scopedInstancesLock.StartWrite();
        try
        {
            _scopedInstances.GetOrAddValueRef(scopedId) = instance;
        }
        finally
        {
            _scopedInstancesLock.EndWrite();
        }
    }

    void RemoveScopedValue(uint scopedId)
    {
        _scopedInstancesLock.StartWrite();
        try
        {
            _scopedInstances.Remove(scopedId);
        }
        finally
        {
            _scopedInstancesLock.EndWrite();
        }
    }

    object WaitForSingleton(uint singletonId)
    {
        while (true)
        {
            var instance = Volatile.Read(ref Singletons[singletonId]);
            if (instance is not SingletonLocker locker)
            {
                if (instance == null) throw new InvalidOperationException("Singleton creation failed.");
                return instance;
            }

            lock (locker)
            {
            }
        }
    }

    object WaitForScoped(uint scopedId)
    {
        while (true)
        {
            if (!TryGetScopedValue(scopedId, out var instance))
                throw new InvalidOperationException("Scoped instance creation failed.");
            if (instance is not ScopedLocker locker)
            {
                return instance!;
            }

            lock (locker)
            {
            }
        }
    }

    void TrackOwnedInstance(object? instance)
    {
        if (instance is not (IDisposable or IAsyncDisposable)) return;
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposeState) != 0, this);
        var node = new OwnedInstanceNode(instance);
        node.Next = Interlocked.Exchange(ref _ownedInstances, node);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeState, 1) != 0) return;

        var ownedInstances = Interlocked.Exchange(ref _ownedInstances, null);
        while (ownedInstances != null)
        {
            await DisposeOwnedAsync(ownedInstances.Instance);
            ownedInstances = ownedInstances.Next;
        }

        if (_serviceScope is { } serviceScope)
        {
            await serviceScope.DisposeAsync();
            return;
        }

        if (!ReferenceEquals(this, _root) || _serviceProvider == null) return;
        await DisposeOwnedAsync(_serviceProvider);
    }

    static async ValueTask DisposeOwnedAsync(object instance)
    {
        switch (instance)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync();
                return;
            case IDisposable disposable:
                disposable.Dispose();
                return;
        }
    }

    void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposeState) != 0, this);
    }

    static void VerifyNonValueType(Type nestedType)
    {
        if (nestedType.IsValueType)
            throw new NotSupportedException(
                "Tuple<> with value type argument is not supported.");
    }

    sealed class SingletonLocker
    {
    }

    sealed class ScopedLocker
    {
    }

    sealed class OwnedInstanceNode(object instance)
    {
        public readonly object Instance = instance;
        public OwnedInstanceNode? Next;
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

    public ResolvingCtx(int paramSize)
    {
        _params = new object[paramSize];
    }
}
