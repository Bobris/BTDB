using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.IL;
using BTDB.IOC.CRegs;

namespace BTDB.IOC
{
    public class ContainerImpl : IContainer
    {

        readonly ConcurrentDictionary<KeyAndType, Func<ContainerImpl, object>> _workers = new ConcurrentDictionary<KeyAndType, Func<ContainerImpl, object>>();
        readonly object _buildingLock = new object();
        readonly Dictionary<KeyAndType, ICReg> _registrations = new Dictionary<KeyAndType, ICReg>();
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
            return type.GetConstructors().OrderByDescending(ci => ci.GetParameters().Length).FirstOrDefault();
        }

        public object Resolve(Type type)
        {
            return ResolveKeyed(null, type);
        }

        public object ResolveNamed(string name, Type type)
        {
            return ResolveKeyed(name, type);
        }

        public object ResolveKeyed(object key, Type type)
        {
            Func<ContainerImpl, object> worker;
            if (_workers.TryGetValue(new KeyAndType(key, type), out worker))
            {
                return worker(this);
            }
            lock (_buildingLock)
            {
                worker = TryBuild(key, type);
            }
            if (worker == null) throwNotResolvable(key, type);
            return worker(this);
        }

        void throwNotResolvable(object key, Type type)
        {
            if (key == null)
            {
                throw new ArgumentException(string.Format("Type {0} cannot be resolved", type.ToSimpleName()));
            }
            throw new ArgumentException(string.Format("Type {0} with key {1} cannot be resolved", type.ToSimpleName(), key));
        }

        Func<ContainerImpl, object> TryBuild(object key, Type type)
        {
            Func<ContainerImpl, object> worker;
            if (!_workers.TryGetValue(new KeyAndType(key, type), out worker))
            {
                worker = Build(key, type);
                if (worker == null) return null;
                _workers.TryAdd(new KeyAndType(key, type), worker);
            }
            return worker;
        }

        ICReg FindChosenReg(object key, Type type)
        {
            ICReg registration;
            if (_registrations.TryGetValue(new KeyAndType(key, type), out registration))
            {
                var multi = registration as ICRegMulti;
                if (multi != null) registration = multi.ChosenOne;
                return registration;
            }
            return null;
        }

        Func<ContainerImpl, object> Build(object key, Type type)
        {
            var registration = FindChosenReg(key, type);
            if (registration != null)
            {
                return BuildSingle(registration);
            }
            if (type == typeof(IContainer))
            {
                return c => c;
            }
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Func<>))
            {
                var resultType = type.GetGenericArguments()[0];
                registration = FindChosenReg(key, resultType);
                if (registration != null)
                {
                    var optimizedFuncCreg = registration as ICRegFuncOptimized;
                    if (optimizedFuncCreg != null)
                    {
                        var optimizedFunc = optimizedFuncCreg.BuildFuncOfT(this, type);
                        if (optimizedFunc != null) return c => optimizedFunc;
                    }
                }
                var worker = TryBuild(key, resultType);
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
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                var resultType = type.GetGenericArguments()[0];
                if (_registrations.TryGetValue(new KeyAndType(key, resultType), out registration))
                {
                    var multi = registration as ICRegMulti;
                    var regs = multi != null ? multi.Regs.ToArray() : new[] {registration};
                    var context = new Dictionary<string, object>();
                    var method = ILBuilder.Instance.NewMethod<Func<ContainerImpl, object>>(type.ToSimpleName());
                    var il = method.Generator;
                    var resultLocal = il.DeclareLocal(typeof (List<>).MakeGenericType(resultType));
                    var itemLocal = il.DeclareLocal(resultType);
                    il
                        .LdcI4(regs.Length)
                        .Newobj(resultLocal.LocalType.GetConstructor(new[] {typeof (int)}))
                        .Stloc(resultLocal);
                    foreach (var cReg in regs)
                    {
                        var regILGen = (ICRegILGen)cReg;
                        regILGen.GenInitialization(this, il, context);
                    }
                    foreach (var cReg in regs)
                    {
                        var regILGen = (ICRegILGen) cReg;
                        var local = regILGen.GenMain(this, il, context);
                        if (local == null)
                        {
                            il.Stloc(itemLocal);
                            local = itemLocal;
                        }
                        il.Ldloc(resultLocal).Ldloc(local).Callvirt(resultLocal.LocalType.GetMethod("Add"));
                    }
                    il
                        .Ldloc(resultLocal)
                        .Castclass(type)
                        .Ret();
                    return method.Create();

                }
            }
            return null;
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
                var result = (Func<ContainerImpl, object>)regOpt.BuildFuncOfT(this, typeof(Func<ContainerImpl, object>));
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
                if (local != null)
                {
                    il.Ldloc(local);
                }
                il.Ret();
                return method.Create();
            }
            throw new NotImplementedException();
        }

        internal ICRegILGen FindCRegILGen(object key, Type type)
        {
            var registration = FindChosenReg(key, type);
            if (registration != null)
            {
                var result = registration as ICRegILGen;
                if (result != null) return result;
                throw new ArgumentException("Builder for " + type.ToSimpleName() + " is not ILGen capable");
            }
            var buildFunc = TryBuild(key, type);
            if (buildFunc != null)
            {
                var result = new FactoryImpl(this, buildFunc, type);
                _registrations.Add(new KeyAndType(key, type), result);
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

        internal void CallInjectingInitializations(ConstructorInfo constructorInfo, IILGen il, IDictionary<string, object> context)
        {
            var pars = constructorInfo.GetParameters();
            foreach (var parameterInfo in pars)
            {
                var regILGen = FindCRegILGen(null, parameterInfo.ParameterType);
                regILGen.GenInitialization(this, il, context);
            }
        }

        internal void CallInjectedConstructor(ConstructorInfo constructorInfo, IILGen il, IDictionary<string, object> context)
        {
            var pars = constructorInfo.GetParameters();
            var regs = new List<ICRegILGen>(pars.Length);
            foreach (var parameterInfo in pars)
            {
                regs.Add(FindCRegILGen(null, parameterInfo.ParameterType));
            }
            var parsLocals = new List<IILLocal>(pars.Length);
            foreach (var reg in regs)
            {
                parsLocals.Add(reg.CorruptingILStack ? reg.GenMain(this, il, context) : null);
            }
            for (int i = 0; i < regs.Count; i++)
            {
                if (regs[i].CorruptingILStack)
                {
                    il.Ldloc(parsLocals[i]);
                }
                else
                {
                    var local = regs[i].GenMain(this, il, context);
                    if (local != null)
                    {
                        il.Ldloc(local);
                    }
                }
            }
            il.Newobj(constructorInfo);
        }
    }
}
