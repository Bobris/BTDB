using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using BTDB.IL;

namespace BTDB.IOC
{
    public class ContainerImpl : IContainer
    {
        readonly ConcurrentDictionary<Type, Func<ContainerImpl, object>> _workers = new ConcurrentDictionary<Type, Func<ContainerImpl, object>>();
        readonly object _buildingLock = new object();
        readonly Dictionary<Type, ICReg> _registrations = new Dictionary<Type, ICReg>();
// ReSharper disable MemberCanBePrivate.Global
        public readonly object[] SingletonLocks;
        public readonly object[] Singletons;
// ReSharper restore MemberCanBePrivate.Global

        internal ContainerImpl(IEnumerable<IRegistration> registrations)
        {
            int singletonCount = 0;
            foreach (var registration in registrations)
            {
                var singleReg = registration as SingleRegistration;
                ICReg reg;
                switch (singleReg.HasLifetime)
                {
                    case Lifetime.AlwaysNew:
                        reg = new AlwaysNewImpl(singleReg.ImplementationType);
                        break;
                    case Lifetime.Singleton:
                        reg = new SingletonImpl(singleReg.ImplementationType, singletonCount);
                        singletonCount++;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                foreach (var asType in singleReg.AsTypes)
                {
                    _registrations.Add(asType, reg);
                }
            }
            SingletonLocks = new object[singletonCount];
            for (int i = 0; i < singletonCount; i++)
            {
                SingletonLocks[i] = new object();
            }
            Singletons = new object[singletonCount];
        }

        public object Resolve(Type type)
        {
            Func<ContainerImpl, object> worker;
            if (_workers.TryGetValue(type, out worker))
            {
                return worker(this);
            }
            lock (_buildingLock)
            {
                worker = TryBuild(type);
            }
            return worker(this);
        }

        Func<ContainerImpl, object> TryBuild(Type type)
        {
            Func<ContainerImpl, object> worker;
            if (!_workers.TryGetValue(type, out worker))
            {
                worker = Build(type);
                _workers.TryAdd(type, worker);
            }
            return worker;
        }

        Func<ContainerImpl, object> Build(Type type)
        {
            ICReg registration;
            if (_registrations.TryGetValue(type, out registration))
            {
                if (registration.Single)
                    return BuildSingle(registration);
            }
            if (type.GetGenericTypeDefinition() == typeof(Func<>))
            {
                var worker = TryBuild(type.GetGenericArguments()[0]);
            }
            return c => null;
        }

        Func<ContainerImpl, object> BuildSingle(ICReg registration)
        {
            if (registration is SingletonImpl) return BuildSingleton((SingletonImpl)registration);
            if (registration is AlwaysNewImpl) return BuildAlwaysNew((AlwaysNewImpl)registration);
            throw new NotImplementedException();
        }

        Func<ContainerImpl, object> BuildAlwaysNew(AlwaysNewImpl registration)
        {
            var method = ILBuilder.Instance.NewMethod<Func<ContainerImpl, object>>("AlwaysNew" + registration.ImplementationType.ToSimpleName());
            var il = method.Generator;
            il
                .Newobj(registration.ImplementationType.GetConstructor(Type.EmptyTypes))
                .Ret();
            return method.Create();
        }

        Func<ContainerImpl, object> BuildSingleton(SingletonImpl registration)
        {
            var method = ILBuilder.Instance.NewMethod<Func<ContainerImpl, object>>("Singleton" + registration.ImplementationType.ToSimpleName());
            var il = method.Generator;
            var localLockTaken = il.DeclareLocal(typeof(bool), "lockTaken");
            var localLock = il.DeclareLocal(typeof(object), "lock");
            var localSingleton = il.DeclareLocal(typeof(object), "singleton");
            var localSingletons = il.DeclareLocal(typeof(object[]), "singletons");
            var labelNull1 = il.DefineLabel();
            var labelNotNull2 = il.DefineLabel();
            var labelNotTaken = il.DefineLabel();
            bool boolPlaceholder = false;
            il
                .Ldarg(0)
                .Ldfld(() => default(ContainerImpl).Singletons)
                .Dup()
                .Stloc(localSingletons)
                .LdcI4(registration.SingletonIndex)
                .LdelemRef()
                .Dup()
                .Stloc(localSingleton)
                .BrfalseS(labelNull1)
                .Ldloc(localSingleton)
                .Ret()
                .Mark(labelNull1)
                .LdcI4(0)
                .Stloc(localLockTaken)
                .Ldarg(0)
                .Ldfld(() => default(ContainerImpl).SingletonLocks)
                .LdcI4(registration.SingletonIndex)
                .LdelemRef()
                .Stloc(localLock)
                .Try()
                .Ldloc(localLock)
                .Ldloca(localLockTaken)
                .Call(() => Monitor.Enter(null, ref boolPlaceholder))
                .Ldloc(localSingletons)
                .LdcI4(registration.SingletonIndex)
                .LdelemRef()
                .Dup()
                .Stloc(localSingleton)
                .Brtrue(labelNotNull2)
                .Newobj(registration.ImplementationType.GetConstructor(Type.EmptyTypes))
                .Stloc(localSingleton)
                .Ldloc(localSingletons)
                .LdcI4(registration.SingletonIndex)
                .Ldloc(localSingleton)
                .StelemRef()
                .Mark(labelNotNull2)
                .Finally()
                .Ldloc(localLockTaken)
                .BrfalseS(labelNotTaken)
                .Ldloc(localLock)
                .Call(() => Monitor.Exit(null))
                .Mark(labelNotTaken)
                .EndTry()
                .Ldloc(localSingleton)
                .Ret();
            return method.Create();
        }
    }

    internal class AlwaysNewImpl : ICReg
    {
        readonly Type _implementationType;

        internal AlwaysNewImpl(Type implementationType)
        {
            _implementationType = implementationType;
        }

        internal Type ImplementationType
        {
            get { return _implementationType; }
        }

        public bool Single
        {
            get { return true; }
        }
    }

    internal class SingletonImpl : ICReg
    {
        readonly Type _implementationType;
        readonly int _singletonIndex;

        public SingletonImpl(Type implementationType, int singletonIndex)
        {
            _implementationType = implementationType;
            _singletonIndex = singletonIndex;
        }

        public int SingletonIndex
        {
            get { return _singletonIndex; }
        }

        public Type ImplementationType
        {
            get { return _implementationType; }
        }

        public bool Single
        {
            get { return true; }
        }
    }

    internal interface ICReg
    {
        bool Single { get; }
    }
}
