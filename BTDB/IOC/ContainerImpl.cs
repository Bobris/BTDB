using System;
using System.Collections;
using System.Collections.Concurrent;
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

public class ContainerImpl : IRootContainer
{
    sealed class ResolveFactoryCache
    {
        public readonly RefDictionary<nint, Func<IContainer, IResolvingCtx?, object?>?> ByType = new();
        public readonly RefDictionary<KeyAndType, Func<IContainer, IResolvingCtx?, object?>?> ByKeyAndType = new();
        public SeqLock Lock;
    }

    readonly struct ServiceProviderFactoryCacheKey(Type type, object? key, bool allowKeylessFallback) :
        IEquatable<ServiceProviderFactoryCacheKey>
    {
        readonly KeyAndType _keyAndType = new(key, type);

        public Type Type => _keyAndType.Type;
        public object? Key => _keyAndType.Key;
        public bool AllowKeylessFallback { get; } = allowKeylessFallback;

        public bool Equals(ServiceProviderFactoryCacheKey other)
        {
            return AllowKeylessFallback == other.AllowKeylessFallback && _keyAndType.Equals(other._keyAndType);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(_keyAndType, AllowKeylessFallback);
        }
    }

    readonly struct ServiceProviderFactoryCacheValue(
        Func<IContainer, IResolvingCtx?, object?>? factory,
        bool clearKeylessFallback)
    {
        public Func<IContainer, IResolvingCtx?, object?>? Factory { get; } = factory;
        public bool ClearKeylessFallback { get; } = clearKeylessFallback;
    }

    internal readonly Dictionary<KeyAndType, CReg> Registrations;

    readonly ContainerImpl _root;
    readonly ResolveFactoryCache _resolveFactoryCache;

    readonly ConcurrentDictionary<ServiceProviderFactoryCacheKey, ServiceProviderFactoryCacheValue>
        _serviceProviderFactoryCache;

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
        _resolveFactoryCache = new();
        _serviceProviderFactoryCache = new();
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

        Registrations[new(null, typeof(IContainer))] = new()
        {
            Factory = (_, _) => (container, _) => container,
            Lifetime = Lifetime.Singleton,
            ScopedId = uint.MaxValue,
            IsSingletonSafe = true
        };
        Registrations[new(null, typeof(IRootContainer))] = new()
        {
            Factory = (_, _) => (_, _) => _root,
            Lifetime = Lifetime.Singleton,
            ScopedId = uint.MaxValue
        };
        if (containerVerification == ContainerVerification.None) return;
        if ((containerVerification & ContainerVerification.SingletonsUsingOnlySingletons) != 0)
        {
            foreach (var (_, reg) in Registrations)
            {
                var singletonReg = reg.DefaultRegistration ?? reg;
                if (singletonReg.Lifetime == Lifetime.Singleton)
                {
                    var ctx = new CreateFactoryCtx();
                    ctx.VerifySingletons = true;
                    singletonReg.Factory(this, ctx);
                }
            }
        }
    }

    ContainerImpl(ContainerImpl parent, IServiceProvider? serviceProvider, AsyncServiceScope? serviceScope)
    {
        _root = parent._root;
        _resolveFactoryCache = _root._resolveFactoryCache;
        _serviceProviderFactoryCache = _root._serviceProviderFactoryCache;
        Registrations = _root.Registrations;
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
        var factory = GetResolveFactory(key, type);
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
        var factory = GetResolveFactory(key, type);
        return factory?.Invoke(this, null);
    }

    Func<IContainer, IResolvingCtx?, object?>? GetResolveFactory(object? key, Type type)
    {
        return key == null
            ? GetOrCreateResolveFactory(type)
            : GetOrCreateResolveFactory(new KeyAndType(key, type));
    }

    Func<IContainer, IResolvingCtx?, object?>? GetOrCreateResolveFactory(Type type)
    {
        var cache = _resolveFactoryCache;
        var cacheKey = type.TypeHandle.Value;
        if (cache.ByType.TryGetValueSeqLock(cacheKey, out var factory, ref cache.Lock))
        {
            return factory;
        }

        factory = CreateFactory(new CreateFactoryCtx(), type, null);
        cache.Lock.StartWrite();
        try
        {
            if (cache.ByType.TryGetValue(cacheKey, out var cachedFactory))
            {
                return cachedFactory;
            }

            cache.ByType.TryAdd(cacheKey, factory);
            return factory;
        }
        finally
        {
            cache.Lock.EndWrite();
        }
    }

    Func<IContainer, IResolvingCtx?, object?>? GetOrCreateResolveFactory(KeyAndType keyAndType)
    {
        var cache = _resolveFactoryCache;
        if (cache.ByKeyAndType.TryGetValueSeqLock(keyAndType, out var factory, ref cache.Lock))
        {
            return factory;
        }

        factory = CreateFactory(new CreateFactoryCtx { ForbidKeylessFallback = true }, keyAndType.Type, keyAndType.Key);
        cache.Lock.StartWrite();
        try
        {
            if (cache.ByKeyAndType.TryGetValue(keyAndType, out var cachedFactory))
            {
                return cachedFactory;
            }

            cache.ByKeyAndType.TryAdd(keyAndType, factory);
            return factory;
        }
        finally
        {
            cache.Lock.EndWrite();
        }
    }

    bool HasRegistration(Type type, object? key, bool allowKeylessFallback)
    {
        return Registrations.ContainsKey(new(key, type)) ||
               (key != null && allowKeylessFallback && Registrations.ContainsKey(new(null, type)));
    }

    Func<IContainer, IResolvingCtx?, object?>? TryCreateServiceProviderFactory(CreateFactoryCtx ctxImpl, Type type,
        object? key)
    {
        if (_serviceProviderIsKeyedService == null) return null;
        var cacheKey = new ServiceProviderFactoryCacheKey(type, key, !ctxImpl.ForbidKeylessFallback);
        if (!_serviceProviderFactoryCache.TryGetValue(cacheKey, out var cacheValue))
        {
            cacheValue = _root.CreateServiceProviderFactoryCacheValue(cacheKey);
            cacheValue = _serviceProviderFactoryCache.GetOrAdd(cacheKey, cacheValue);
        }

        if (cacheValue.ClearKeylessFallback)
        {
            ctxImpl.ForbidKeylessFallback = false;
        }

        return cacheValue.Factory;
    }

    ServiceProviderFactoryCacheValue CreateServiceProviderFactoryCacheValue(ServiceProviderFactoryCacheKey cacheKey)
    {
        if (_serviceProviderIsKeyedService == null) return default;

        var type = cacheKey.Type;
        var key = cacheKey.Key;
        if (type.IsAssignableTo(typeof(IEnumerable)) && type.IsGenericType)
        {
            var genericType = type.GenericTypeArguments[0];
            if (HasRegistration(genericType, key, cacheKey.AllowKeylessFallback)) return default;
            if (key != null)
            {
                if (_serviceProviderIsKeyedService.IsKeyedService(genericType, key))
                {
                    return new((c, r) => ((ContainerImpl)c).ResolveFromServiceProvider(type, key), true);
                }

                if (cacheKey.AllowKeylessFallback && _serviceProviderIsKeyedService.IsService(genericType))
                {
                    return new((c, r) => ((ContainerImpl)c).ResolveFromServiceProvider(type, null), false);
                }

                return default;
            }

            return _serviceProviderIsKeyedService.IsService(genericType)
                ? new((c, r) => ((ContainerImpl)c).ResolveFromServiceProvider(type, null), false)
                : default;
        }

        if (key != null)
        {
            if (_serviceProviderIsKeyedService.IsKeyedService(type, key))
            {
                return new((c, r) => ((ContainerImpl)c).ResolveFromServiceProvider(type, key), true);
            }

            if (cacheKey.AllowKeylessFallback && _serviceProviderIsKeyedService.IsService(type))
            {
                return new((c, r) => ((ContainerImpl)c).ResolveFromServiceProvider(type, null), false);
            }

            return default;
        }

        return _serviceProviderIsKeyedService.IsService(type)
            ? new((c, r) => ((ContainerImpl)c).ResolveFromServiceProvider(type, null), false)
            : default;
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

        if (key != null && !ctxImpl.ForbidKeylessFallback && Registrations.TryGetValue(new(null, type), out cReg))
        {
            return CreateFactoryFromRegistration(ctxImpl, cReg, key, type);
        }

        var serviceProviderFactory = TryCreateServiceProviderFactory(ctxImpl, type, key);
        if (serviceProviderFactory != null)
        {
            return serviceProviderFactory;
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

        return key == null
            ? _serviceProvider.GetRequiredService(type)
            : _serviceProvider.GetRequiredKeyedService(type, key);
    }

    Func<IContainer, IResolvingCtx?, object?>? CreateFactoryFromRegistration(CreateFactoryCtx ctxImpl, CReg cReg,
        object? key,
        Type type)
    {
        if (ctxImpl.Enumerate >= 0 && cReg.Multi.Count > 1)
        {
            cReg = ctxImpl.Enumerating(cReg);
        }
        else if (cReg.DefaultRegistration != null)
        {
            cReg = cReg.DefaultRegistration;
        }

        ctxImpl.PushResolving(cReg);
        try
        {
            if (cReg.Lifetime == Lifetime.Singleton && !cReg.IsSingletonSafe)
            {
                if (ctxImpl.VerifySingletons) return (_, _) => null;
                var rootContainer = _root;
                var singletonInstance = cReg.SingletonValue;
                if (singletonInstance is InstanceLocker)
                {
                    return (_, _) => rootContainer.WaitForSingleton(cReg);
                }

                if (singletonInstance != null)
                {
                    return (_, _) => singletonInstance;
                }

                var singletonFactoryCache = Volatile.Read(ref cReg.LifetimeFactoryCache);
                if (singletonFactoryCache != null) return singletonFactoryCache;

                Func<IContainer, IResolvingCtx?, object>? f = null;

                var ff = (IContainer container, IResolvingCtx? resolvingCtx) =>
                {
                    ref var singleton = ref cReg.SingletonValue;
                    var instance = singleton;
                    if (instance != null)
                    {
                        if (instance is InstanceLocker)
                        {
                            return rootContainer.WaitForSingleton(cReg);
                        }

                        return instance;
                    }

                    var mySingletonLocker = new InstanceLocker();
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
                                cReg.LifetimeFactoryCache = null;
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

                    if (instance is InstanceLocker)
                    {
                        return rootContainer.WaitForSingleton(cReg);
                    }

                    return instance;
                };
                if (Interlocked.CompareExchange(ref cReg.LifetimeFactoryCache, ff, null) == null)
                {
                    using var resolvingCtxRestorer = ctxImpl.ResolvingCtxRestorer();
                    using var enumerableRestorer = ctxImpl.EnumerableRestorer();
                    f = cReg.Factory(rootContainer, ctxImpl);
                }

                return cReg.LifetimeFactoryCache;
            }

            if (cReg.Lifetime == Lifetime.Scoped && cReg.ScopedId != uint.MaxValue)
            {
                if (ctxImpl.VerifySingletons)
                {
                    throw new BTDBException("Scoped dependency " + new KeyAndType(key, type));
                }

                var scopedFactoryCache = Volatile.Read(ref cReg.LifetimeFactoryCache);
                if (scopedFactoryCache != null) return scopedFactoryCache;

                Func<IContainer, IResolvingCtx?, object>? f = null;
                var ff = (IContainer container, IResolvingCtx? resolvingCtx) =>
                {
                    var currentContainer = (ContainerImpl)container;
                    if (currentContainer.TryGetScopedValue(cReg.ScopedId, out var instance))
                    {
                        if (instance is InstanceLocker)
                        {
                            return currentContainer.WaitForScoped(cReg.ScopedId);
                        }

                        return instance;
                    }

                    var myScopedLocker = new InstanceLocker();
                    Monitor.Enter(myScopedLocker);
                    try
                    {
                        currentContainer._scopedInstancesLock.StartWrite();
                        try
                        {
                            ref var instanceRef = ref currentContainer._scopedInstances.GetOrAddValueRef(cReg.ScopedId);
                            if (instanceRef == null)
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
                                cReg.LifetimeFactoryCache = null;
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

                    if (instance is InstanceLocker)
                    {
                        return currentContainer.WaitForScoped(cReg.ScopedId);
                    }

                    return instance;
                };
                if (Interlocked.CompareExchange(ref cReg.LifetimeFactoryCache, ff, null) == null)
                {
                    f = cReg.Factory(this, ctxImpl);
                }

                return cReg.LifetimeFactoryCache;
            }

            if (ctxImpl.VerifySingletons)
            {
                if (cReg.IsSingletonSafe) return (_, _) => null;
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

    internal Func<IContainer, IResolvingCtx?, object?> GetRegistrationFactory(Type type, object? key,
        int registrationIndex)
    {
        ThrowIfDisposed();
        var factory = CreateRegistrationFactory(new() { ForbidKeylessFallback = key != null }, type, key,
            registrationIndex);
        if (factory == null)
        {
            ThrowNotResolvable(key, type);
        }

        return factory;
    }

    internal IContainer CreateScopeForServiceProvider(IServiceProvider serviceProvider)
    {
        ThrowIfDisposed();
        return new ContainerImpl(this, serviceProvider, null);
    }

    Func<IContainer, IResolvingCtx?, object?>? CreateRegistrationFactory(CreateFactoryCtx ctxImpl, Type type,
        object? key,
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

    object WaitForSingleton(CReg singletonReg)
    {
        while (true)
        {
            var instance = Volatile.Read(ref singletonReg.SingletonValue);
            if (instance is not InstanceLocker locker)
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
            if (instance is not InstanceLocker locker)
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

    sealed class InstanceLocker
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
