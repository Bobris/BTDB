using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using BTDB.IL;
using BTDB.ODBLayer;

namespace BTDB.IOC
{
    internal class SingletonImpl : ICReg, ICRegILGen, ICRegFuncOptimized
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

        public bool Single
        {
            get { return true; }
        }

        public string GenFuncName
        {
            get { return "Singleton_" + _implementationType.ToSimpleName(); }
        }

        public void GenInitialization(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
            var pars = _constructorInfo.GetParameters();
            foreach (var parameterInfo in pars)
            {
                var regILGen = container.FindCRegILGen(parameterInfo.ParameterType);
                regILGen.GenInitialization(container, il, context);
            }
            if (!context.ContainsKey("SingletonsLocal"))
            {
                var localSingletons = il.DeclareLocal(typeof(object[]), "singletons");
                il
                    .Ldarg(0)
                    .Ldfld(() => default(ContainerImpl).Singletons)
                    .Stloc(localSingletons);
                context.Add("SingletonsLocal", localSingletons);
            }
            if (!context.ContainsKey("BuiltSingletons"))
            {
                context.Add("BuiltSingletons", new Dictionary<ICReg, IILLocal>(ReferenceEqualityComparer<ICReg>.Instance));
            }
        }

        public IILLocal GenMain(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
            var builtSingletons = (Dictionary<ICReg, IILLocal>)context["BuiltSingletons"];
            IILLocal localSingleton;
            if (builtSingletons.TryGetValue(this, out localSingleton))
            {
                return localSingleton;
            }
            var localSingletons = (IILLocal)context["SingletonsLocal"];
            localSingleton = il.DeclareLocal(_implementationType, "singleton");
            var obj = container.Singletons[_singletonIndex];
            if (obj != null)
            {
                il
                    .Ldloc(localSingletons)
                    .LdcI4(_singletonIndex)
                    .LdelemRef()
                    .Castclass(_implementationType)
                    .Stloc(localSingleton);
                return localSingleton;
            }
            var localLockTaken = il.DeclareLocal(typeof(bool), "lockTaken");
            var localLock = il.DeclareLocal(typeof(object), "lock");
            var labelNull1 = il.DefineLabel();
            var labelNotNull2 = il.DefineLabel();
            var labelNotTaken = il.DefineLabel();
            bool boolPlaceholder = false;
            il
                .Ldloc(localSingletons)
                .LdcI4(_singletonIndex)
                .LdelemRef()
                .Dup()
                .Castclass(_implementationType)
                .Stloc(localSingleton)
                .Brtrue(labelNull1)
                .LdcI4(0)
                .Stloc(localLockTaken)
                .Ldarg(0)
                .Ldfld(() => default(ContainerImpl).SingletonLocks)
                .LdcI4(_singletonIndex)
                .LdelemRef()
                .Stloc(localLock)
                .Try()
                .Ldloc(localLock)
                .Ldloca(localLockTaken)
                .Call(() => Monitor.Enter(null, ref boolPlaceholder))
                .Ldloc(localSingletons)
                .LdcI4(_singletonIndex)
                .LdelemRef()
                .Dup()
                .Castclass(_implementationType)
                .Stloc(localSingleton)
                .Brtrue(labelNotNull2);
            context["BuiltSingletons"] = new Dictionary<ICReg, IILLocal>(builtSingletons, ReferenceEqualityComparer<ICReg>.Instance);
            var pars = _constructorInfo.GetParameters();
            var parsLocals = new List<IILLocal>(pars.Length);
            foreach (var parameterInfo in pars)
            {
                var regILGen = container.FindCRegILGen(parameterInfo.ParameterType);
                parsLocals.Add(regILGen.GenMain(container, il, context));
            }
            foreach (var parLocal in parsLocals)
            {
                il.Ldloc(parLocal);
            }
            context["BuiltSingletons"] = builtSingletons;
            il
                .Newobj(_constructorInfo)
                .Stloc(localSingleton)
                .Ldloc(localSingletons)
                .LdcI4(_singletonIndex)
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
                .Mark(labelNull1);
            builtSingletons.Add(this, localSingleton);
            return localSingleton;
        }

        public object BuildFuncOfT(ContainerImpl container, Type funcType)
        {
            var obj = container.Singletons[_singletonIndex];
            return obj == null ? null : ClosureOfObjBuilder.Build(funcType, obj);
        }

        public Func<ContainerImpl, object> BuildFuncContainer2Object(ContainerImpl container)
        {
            var obj = container.Singletons[_singletonIndex];
            if (obj == null) return null;
            return c => obj;
        }
    }

    internal static class ClosureOfObjBuilder
    {
        internal static object Build(Type funcType, object toReturn)
        {
            return Delegate.CreateDelegate(funcType,
                typeof(ClosureOfObj<>).MakeGenericType(funcType.GetGenericArguments()[0]).GetConstructors()[0].Invoke(new[] { toReturn }),
                "Call");
        }

        public class ClosureOfObj<T>
        {
            readonly T _obj;

            public ClosureOfObj(object obj)
            {
                _obj = (T)obj;
            }

            public T Call()
            {
                return _obj;
            }
        }
    }
}
