using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using BTDB.IL;
using BTDB.ODBLayer;

namespace BTDB.IOC.CRegs
{
    internal class SingletonImpl : ICReg, ICRegILGen, ICRegFuncOptimized
    {
        readonly Type _implementationType;
        readonly ICRegILGen _wrapping;
        readonly int _singletonIndex;

        public SingletonImpl(Type implementationType, ICRegILGen wrapping, int singletonIndex)
        {
            _implementationType = implementationType;
            _wrapping = wrapping;
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
            _wrapping.GenInitialization(container,il,context);
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

        public bool CorruptingILStack
        {
            get { return true; }
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
            var nestedLocal = _wrapping.GenMain(container, il, context);
            if (nestedLocal!=null)
            {
                il.Ldloc(nestedLocal);
            }
            context["BuiltSingletons"] = builtSingletons;
            il
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
}
