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

        readonly ConcurrentDictionary<KeyAndType, Func<object>> _workers = new ConcurrentDictionary<KeyAndType, Func<object>>();
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
            context.AddCReg(Enumerable.Repeat(new KeyAndType(null, typeof(IContainer)), 1), true, new ContainerInjectImpl());
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
            Func<object> worker;
            if (_workers.TryGetValue(new KeyAndType(key, type), out worker))
            {
                return worker();
            }
            lock (_buildingLock)
            {
                worker = TryBuild(key, type);
            }
            if (worker == null) throwNotResolvable(key, type);
            return worker();
        }

        void throwNotResolvable(object key, Type type)
        {
            if (key == null)
            {
                throw new ArgumentException(string.Format("Type {0} cannot be resolved", type.ToSimpleName()));
            }
            throw new ArgumentException(string.Format("Type {0} with key {1} cannot be resolved", type.ToSimpleName(), key));
        }

        Func<object> TryBuild(object key, Type type)
        {
            Func<object> worker;
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

        Func<object> Build(object key, Type type)
        {
            var registration = FindChosenReg(key, type);
            if (registration != null)
            {
                return BuildCReg(registration);
            }
            if (type.IsDelegate())
            {
                var methodInfo = type.GetMethod("Invoke");
                var resultType = methodInfo.ReturnType;
                registration = FindChosenReg(key, resultType);
                if (registration != null)
                {
                    var optimizedFuncCreg = registration as ICRegFuncOptimized;
                    if (optimizedFuncCreg != null)
                    {
                        var optimizedFunc = optimizedFuncCreg.BuildFuncOfT(this, type);
                        if (optimizedFunc != null) return () => optimizedFunc;
                    }
                }
                if (registration is ICRegILGen)
                {
                    var regILGen = (ICRegILGen)registration;
                    var context = new GenerationContext(this, methodInfo.GetParameters());
                    var method = ILBuilder.Instance.NewMethod(regILGen.GenFuncName(context), type, typeof(ContainerImpl));
                    var il = method.Generator;
                    context.GenerateBody(il, regILGen);
                    il.Ret();
                    var func = method.Create(this);
                    return () => func;
                }
            }
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Lazy<>))
            {
                var resultType = type.GetGenericArguments()[0];
                return ((IFuncBuilder)
                        typeof(ClosureOfLazy<>).MakeGenericType(resultType).GetConstructors()[0].Invoke(new object[0])).Build(this);
            }
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                var resultType = type.GetGenericArguments()[0];
                if (_registrations.TryGetValue(new KeyAndType(key, resultType), out registration))
                {
                    var multi = registration as ICRegMulti;
                    var regs = multi != null ? multi.Regs.ToArray() : new[] { registration };
                    var context = new GenerationContext(this);
                    foreach (var cReg in regs)
                    {
                        var regILGen = (ICRegILGen)cReg;
                        context.GatherNeeds(regILGen, new HashSet<ICRegILGen>());
                    }
                    var method = ILBuilder.Instance.NewMethod(type.ToSimpleName(), typeof(Func<object>), typeof(ContainerImpl));
                    var il = method.Generator;
                    context.IL = il;
                    var resultLocal = il.DeclareLocal(typeof(List<>).MakeGenericType(resultType));
                    var itemLocal = il.DeclareLocal(resultType);
                    il
                        .LdcI4(regs.Length)
                        .Newobj(resultLocal.LocalType.GetConstructor(new[] { typeof(int) }))
                        .Stloc(resultLocal);
                    foreach (var cReg in regs)
                    {
                        var regILGen = (ICRegILGen)cReg;
                        regILGen.GenInitialization(context);
                    }
                    foreach (var cReg in regs)
                    {
                        var regILGen = (ICRegILGen)cReg;
                        var local = regILGen.GenMain(context);
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
                    return (Func<object>)method.Create(this);

                }
            }
            return null;
        }

        public interface IFuncBuilder
        {
            Func<object> Build(ContainerImpl c);
        }

        public class ClosureOfLazy<T> : IFuncBuilder where T : class
        {
            public Func<object> Build(ContainerImpl c)
            {
                return () => new Lazy<T>(c.Resolve<T>);
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

        Func<object> BuildCReg(ICReg registration)
        {
            if (registration is ICRegFuncOptimized)
            {
                var regOpt = (ICRegFuncOptimized)registration;
                var result = (Func<object>)regOpt.BuildFuncOfT(this, typeof(Func<object>));
                if (result != null)
                {
                    return result;
                }
            }
            if (registration is ICRegILGen)
            {
                var regILGen = (ICRegILGen)registration;
                var context = new GenerationContext(this);
                var method = ILBuilder.Instance.NewMethod(regILGen.GenFuncName(context), typeof(Func<object>),typeof(ContainerImpl));
                var il = method.Generator;
                context.GenerateBody(il, regILGen);
                il.Ret();
                return (Func<object>)method.Create(this);
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
            return null;
        }

        public int AddInstance(object instance)
        {
            var result = Instances.Length;
            Array.Resize(ref Instances, result + 1);
            Instances[result] = instance;
            return result;
        }
    }
}
