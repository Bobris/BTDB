using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using BTDB.IL;
using BTDB.IOC.CRegs;

namespace BTDB.IOC;

public class ContainerImpl : IContainer
{
    readonly ConcurrentDictionary<KeyAndType, Func<object>> _workers =
        new ConcurrentDictionary<KeyAndType, Func<object>>();

    readonly object _buildingLock = new object();
    internal readonly Dictionary<KeyAndType, ICReg> Registrations = new Dictionary<KeyAndType, ICReg>();

    // ReSharper disable MemberCanBePrivate.Global
    public readonly object[] SingletonLocks;
    public readonly object?[] Singletons;

    public object[] Instances;
    // ReSharper restore MemberCanBePrivate.Global

    internal ContainerImpl(ReadOnlySpan<IRegistration> registrations, ContainerVerification containerVerification)
    {
        var context = new ContainerRegistrationContext(this, Registrations);
        foreach (var registration in registrations)
        {
            ((IContanerRegistration)registration).Register(context);
        }

        SingletonLocks = new object[context.SingletonCount];
        for (var i = 0; i < context.SingletonCount; i++)
        {
            SingletonLocks[i] = new object();
        }

        Singletons = new object[context.SingletonCount];
        Instances = context.Instances;
        context.AddCReg(new KeyAndType(null, typeof(IContainer)), true, false,
            new ContainerInjectImpl());
        if (containerVerification == ContainerVerification.None) return;
        foreach (var (_, reg) in Registrations)
        {
            reg.Verify(containerVerification, this);
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
        // ReSharper disable once InconsistentlySynchronizedField
        if (_workers.TryGetValue(new KeyAndType(key, type), out var worker))
        {
            return worker();
        }

        lock (_buildingLock)
        {
            worker = TryBuild(key, type);
        }

        if (worker == null) ThrowNotResolvable(key, type);
        return worker();
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
        if (_workers.TryGetValue(new KeyAndType(key, type), out var worker))
        {
            return worker?.Invoke();
        }

        lock (_buildingLock)
        {
            worker = TryBuild(key, type);
        }

        return worker?.Invoke();
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

    Func<object>? TryBuild(object? key, Type type)
    {
        if (!_workers.TryGetValue(new KeyAndType(key, type), out var worker))
        {
            worker = Build(key, type);
            if (worker == null) return null;
            _workers.TryAdd(new KeyAndType(key, type), worker);
        }

        return worker;
    }

    internal Func<object> BuildFromRegistration(ICRegILGen registration, IBuildContext buildContext)
    {
        if (registration is ICRegFuncOptimized regOpt)
        {
            var result = (Func<object>)regOpt.BuildFuncOfT(this, typeof(Func<object>));
            if (result != null)
            {
                return result;
            }
        }

        lock (_buildingLock) // Lazy builder could call this method outside of normal Container builder
        {
            var context = new GenerationContext(this, registration, buildContext);
            return (Func<object>)context.GenerateFunc(typeof(Func<object>));
        }
    }

    Func<object>? Build(object? key, Type type)
    {
        var buildContext = new BuildContext(this);
        var registration = buildContext.ResolveNeedBy(type, key);
        return registration != null ? BuildFromRegistration(registration, buildContext) : null;
    }

    public int AddInstance(object instance)
    {
        var result = Instances.Length;
        Array.Resize(ref Instances, result + 1);
        Instances[result] = instance;
        return result;
    }
}
