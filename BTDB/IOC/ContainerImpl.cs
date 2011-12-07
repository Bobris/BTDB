using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
                        reg = new AlwaysNewImpl(singleReg.ImplementationType, FindBestConstructor(singleReg.ImplementationType));
                        break;
                    case Lifetime.Singleton:
                        reg = new SingletonImpl(singleReg.ImplementationType, FindBestConstructor(singleReg.ImplementationType), singletonCount);
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

        ConstructorInfo FindBestConstructor(Type type)
        {
            return type.GetConstructors().OrderByDescending(ci => ci.GetParameters().Length).First();
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
                var resultType = type.GetGenericArguments()[0];
                var worker = TryBuild(resultType);
                var result = Delegate.CreateDelegate(type,
                                        typeof(ClosureOfFunc<>).MakeGenericType(resultType).GetConstructors()[0].Invoke
                                            (new object[] { this, worker }), "Call");
                return c => result;
            }
            return c => null;
        }

        internal class ClosureOfFunc<T> where T : class
        {
            readonly ContainerImpl _container;
            readonly Func<ContainerImpl, object> _func;

            public ClosureOfFunc(ContainerImpl container, Func<ContainerImpl, object> func)
            {
                _container = container;
                _func = func;
            }

            public T Call()
            {
                return (T)_func(_container);
            }
        }

        Func<ContainerImpl, object> BuildSingle(ICReg registration)
        {
            if (registration is ICRegILGen)
            {
                var regILGen = (ICRegILGen)registration;
                var context = new Dictionary<string, object>();
                var method = ILBuilder.Instance.NewMethod<Func<ContainerImpl, object>>(regILGen.GenFuncName);
                var il = method.Generator;
                regILGen.GenInitialization(this, il, context);
                regILGen.GenMain(this,il,context);
                il.Ret();
                return method.Create();
            }
            if (registration is SingletonImpl) return BuildSingleton((SingletonImpl)registration);
            throw new NotImplementedException();
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
                .Brtrue(labelNull1)
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
                .Mark(labelNull1)
                .Ldloc(localSingleton)
                .Ret();
            return method.Create();
        }

        internal ICRegILGen FindCRegILGen(Type type)
        {
            ICReg registration;
            if (_registrations.TryGetValue(type, out registration))
            {
                var result = registration as ICRegILGen;
                if (result != null) return result;
                throw new ArgumentException("Builder for "+type.ToSimpleName()+" is not ILGen capable");
            }
            throw new ArgumentException("Don't know how to build "+type.ToSimpleName());
        }
    }

    internal class AlwaysNewImpl : ICReg, ICRegILGen
    {
        readonly Type _implementationType;
        readonly ConstructorInfo _constructorInfo;

        internal AlwaysNewImpl(Type implementationType, ConstructorInfo constructorInfo)
        {
            _implementationType = implementationType;
            _constructorInfo = constructorInfo;
        }

        public bool Single
        {
            get { return true; }
        }

        public string GenFuncName
        {
            get { return "AlwaysNew_" + _implementationType.ToSimpleName(); }
        }

        public void GenInitialization(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
        }

        public void GenMain(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
            var pars = _constructorInfo.GetParameters();
            foreach (var parameterInfo in pars)
            {
                var regILGen = container.FindCRegILGen(parameterInfo.ParameterType);
                regILGen.GenMain(container,il,context);
            }
            il.Newobj(_constructorInfo);
        }
    }

    internal class SingletonImpl : ICReg
    {
        readonly Type _implementationType;
        readonly ConstructorInfo _constructorInfo;
        readonly int _singletonIndex;

        public SingletonImpl(Type implementationType, ConstructorInfo constructorInfo, int singletonIndex)
        {
            _implementationType = implementationType;
            _constructorInfo = constructorInfo;
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
}
