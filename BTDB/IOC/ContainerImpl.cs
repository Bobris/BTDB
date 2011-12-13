using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
        public object[] Instances;
        // ReSharper restore MemberCanBePrivate.Global

        internal ContainerImpl(IEnumerable<IRegistration> registrations)
        {
            var context = new ContanerRegistrationContext(this, _registrations);
            foreach (var registration in registrations)
            {
                ((IContanerRegistration)registration).Register(context);
            }
            SingletonLocks = new object[context.SingletonCount];
            for (int i = 0; i < context.SingletonCount; i++)
            {
                SingletonLocks[i] = new object();
            }
            Singletons = new object[context.SingletonCount];
            Instances = context.Instances.ToArray();
        }

        internal static ConstructorInfo FindBestConstructor(Type type)
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
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Func<>))
            {
                var resultType = type.GetGenericArguments()[0];
                if (_registrations.TryGetValue(resultType, out registration))
                {
                    var optimizedFuncCreg = registration as ICRegFuncOptimized;
                    if (optimizedFuncCreg != null)
                    {
                        var optimizedFunc = optimizedFuncCreg.BuildFuncOfT(this, type);
                        if (optimizedFunc != null) return c => optimizedFunc;
                    }
                }
                var worker = TryBuild(resultType);
                if (worker != null)
                {
                    var result = Delegate.CreateDelegate(type,
                        typeof(ClosureOfFunc<>).MakeGenericType(resultType).GetConstructors()[0].Invoke(new object[] { this, worker }), "Call");
                    return c => result;
                }
            }
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Lazy<>))
            {
                var resultType = type.GetGenericArguments()[0];
                return ((IFuncBuilder)
                        typeof(ClosureOfLazy<>).MakeGenericType(resultType).GetConstructors()[0].Invoke(new object[0])).Build();
            }
            return c => null;
        }

        public interface IFuncBuilder
        {
            Func<ContainerImpl, object> Build();
        }

        public class ClosureOfLazy<T> : IFuncBuilder where T : class
        {
            public ClosureOfLazy()
            {
            }

            public Func<ContainerImpl, object> Build()
            {
                return c => new Lazy<T>(() => c.Resolve<T>());
            }
        }

        public class ClosureOfFunc<T> where T : class
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
            if (registration is ICRegFuncOptimized)
            {
                var regOpt = (ICRegFuncOptimized)registration;
                var result = regOpt.BuildFuncContainer2Object(this);
                if (result != null)
                {
                    return result;
                }
            }
            if (registration is ICRegILGen)
            {
                var regILGen = (ICRegILGen)registration;
                var context = new Dictionary<string, object>();
                var method = ILBuilder.Instance.NewMethod<Func<ContainerImpl, object>>(regILGen.GenFuncName);
                var il = method.Generator;
                regILGen.GenInitialization(this, il, context);
                var local = regILGen.GenMain(this, il, context);
                il.Ldloc(local).Ret();
                return method.Create();
            }
            throw new NotImplementedException();
        }

        internal ICRegILGen FindCRegILGen(Type type)
        {
            ICReg registration;
            if (_registrations.TryGetValue(type, out registration))
            {
                var result = registration as ICRegILGen;
                if (result != null) return result;
                throw new ArgumentException("Builder for " + type.ToSimpleName() + " is not ILGen capable");
            }
            var buildFunc = TryBuild(type);
            if (buildFunc != null)
            {
                var result = new CRegILGenWrapper(this, buildFunc, type);
                _registrations.Add(type, result);
                return result;
            }
            throw new ArgumentException("Don't know how to build " + type.ToSimpleName());
        }

        public int AddInstance(object instance)
        {
            var result = Instances.Length;
            Array.Resize(ref Instances, result + 1);
            Instances[result] = instance;
            return result;
        }
    }

    internal class CRegILGenWrapper : ICReg, ICRegILGen
    {
        readonly ContainerImpl _container;
        readonly Func<ContainerImpl, object> _buildFunc;
        readonly Type _type;
        readonly int _instanceIndex;

        public CRegILGenWrapper(ContainerImpl container, Func<ContainerImpl, object> buildFunc, Type type)
        {
            _container = container;
            _buildFunc = buildFunc;
            _type = type;
            _instanceIndex = _container.AddInstance(buildFunc);
        }

        public bool Single
        {
            get { return true; }
        }

        public string GenFuncName
        {
            get { return "Wrapper_" + _type.ToSimpleName(); }
        }

        public void GenInitialization(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
            if (!context.ContainsKey("InstancesLocal"))
            {
                var localInstances = il.DeclareLocal(typeof(object[]), "instances");
                il
                    .Ldarg(0)
                    .Ldfld(() => default(ContainerImpl).Instances)
                    .Stloc(localInstances);
                context.Add("InstancesLocal", localInstances);
            }
        }

        public IILLocal GenMain(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
            var localInstances = (IILLocal)context["InstancesLocal"];
            var localInstance = il.DeclareLocal(_type, "instance");
            il
                .Ldloc(localInstances)
                .LdcI4(_instanceIndex)
                .LdelemRef()
                .Castclass(typeof(Func<ContainerImpl, object>))
                .Ldarg(0)
                .Call(() => default(Func<ContainerImpl, object>).Invoke(null))
                .Castclass(_type)
                .Stloc(localInstance);
            return localInstance;
        }
    }
}
