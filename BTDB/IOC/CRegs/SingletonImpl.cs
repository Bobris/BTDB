using System;
using System.Collections.Generic;
using System.Linq;
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

        public object BuildFuncOfT(ContainerImpl container, Type funcType)
        {
            var obj = container.Singletons[_singletonIndex];
            return obj == null ? null : ClosureOfObjBuilder.Build(funcType, obj);
        }

        string ICRegILGen.GenFuncName(IGenerationContext context)
        {
            return "Singleton_" + _implementationType.ToSimpleName();
        }

        public void GenInitialization(IGenerationContext context)
        {
            _wrapping.GenInitialization(context);
            context.GetSpecific<SingletonsLocal>().Prepare();
        }

        internal class BuildCRegLocals
        {
            Dictionary<ICReg, IILLocal> _map = new Dictionary<ICReg, IILLocal>(ReferenceEqualityComparer<ICReg>.Instance);
            readonly Stack<Dictionary<ICReg, IILLocal>> _stack = new Stack<Dictionary<ICReg, IILLocal>>();

            internal void Push()
            {
                _stack.Push(_map);
                _map = new Dictionary<ICReg, IILLocal>(_map, ReferenceEqualityComparer<ICReg>.Instance);
            }

            internal void Pop()
            {
                _map = _stack.Pop();
            }

            internal IILLocal Get(ICReg key)
            {
                IILLocal result;
                return _map.TryGetValue(key, out result) ? result : null;
            }

            public void Add(ICReg key, IILLocal local)
            {
                _map.Add(key, local);
            }
        }

        internal class SingletonsLocal : IGenerationContextSetter
        {
            IGenerationContext _context;

            public void Set(IGenerationContext context)
            {
                _context = context;
            }

            internal void Prepare()
            {
                if (MainLocal != null) return;
                MainLocal = _context.IL.DeclareLocal(typeof(object[]), "singletons");
                _context.IL
                    .Ldarg(0)
                    .Ldfld(() => default(ContainerImpl).Singletons)
                    .Stloc(MainLocal);
            }

            internal IILLocal MainLocal { get; private set; }
        }

        public bool IsCorruptingILStack(IGenerationContext context)
        {
            var buildCRegLocals = context.GetSpecific<BuildCRegLocals>();
            var localSingleton = buildCRegLocals.Get(this);
            if (localSingleton != null) return false;
            var obj = context.Container.Singletons[_singletonIndex];
            if (obj != null) return false;
            return true;
        }

        public IILLocal GenMain(IGenerationContext context)
        {
            var il = context.IL;
            var buildCRegLocals = context.GetSpecific<BuildCRegLocals>();
            var localSingleton = buildCRegLocals.Get(this);
            if (localSingleton != null)
            {
                return localSingleton;
            }
            var localSingletons = context.GetSpecific<SingletonsLocal>().MainLocal;
            localSingleton = il.DeclareLocal(_implementationType, "singleton");
            var obj = context.Container.Singletons[_singletonIndex];
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
                .Stloc(localLockTaken);
            context.PushToILStack(this, Need.ContainerNeed);
            il
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
            buildCRegLocals.Push();
            var nestedLocal = _wrapping.GenMain(context);
            if (nestedLocal != null)
            {
                il.Ldloc(nestedLocal);
            }
            buildCRegLocals.Pop();
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
            buildCRegLocals.Add(this, localSingleton);
            return localSingleton;
        }

        public IEnumerable<INeed> GetNeeds(IGenerationContext context)
        {
            yield return new Need
                {
                    Kind = NeedKind.CReg,
                    Key = _wrapping
                };
            yield return Need.ContainerNeed;
        }
    }
}
