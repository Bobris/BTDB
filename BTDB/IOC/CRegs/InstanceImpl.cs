using System;
using System.Collections.Generic;
using BTDB.IL;
using BTDB.ODBLayer;

namespace BTDB.IOC.CRegs
{
    internal class InstanceImpl : ICReg, ICRegILGen, ICRegFuncOptimized
    {
        readonly object _instance;
        readonly int _instanceIndex;

        public InstanceImpl(object instance, int instanceIndex)
        {
            _instance = instance;
            _instanceIndex = instanceIndex;
        }

        public string GenFuncName
        {
            get { return "Instance_" + _instance.GetType().ToSimpleName(); }
        }

        public void GenInitialization(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
            if (_instance == null) return;
            if (!context.ContainsKey("InstancesLocal"))
            {
                var localInstances = il.DeclareLocal(typeof(object[]), "instances");
                il
                    .Ldarg(0)
                    .Ldfld(() => default(ContainerImpl).Instances)
                    .Stloc(localInstances);
                context.Add("InstancesLocal", localInstances);
            }
            if (!context.ContainsKey("BuiltSingletons"))
            {
                context.Add("BuiltSingletons", new Dictionary<ICReg, IILLocal>(ReferenceEqualityComparer<ICReg>.Instance));
            }
        }

        public bool CorruptingILStack
        {
            get { return false; }
        }

        public IILLocal GenMain(ContainerImpl container, IILGen il, IDictionary<string, object> context)
        {
            if (_instance==null)
            {
                il.Ldnull();
                return null;
            }
            var builtSingletons = (Dictionary<ICReg, IILLocal>)context["BuiltSingletons"];
            IILLocal localInstance;
            if (builtSingletons.TryGetValue(this, out localInstance))
            {
                return localInstance;
            }
            var localInstances = (IILLocal)context["InstancesLocal"];
            localInstance = il.DeclareLocal(_instance.GetType(), "instance");
            il
                .Ldloc(localInstances)
                .LdcI4(_instanceIndex)
                .LdelemRef()
                .Castclass(_instance.GetType())
                .Stloc(localInstance);
            return localInstance;
        }

        public object BuildFuncOfT(ContainerImpl container, Type funcType)
        {
            var obj = container.Instances[_instanceIndex];
            return ClosureOfObjBuilder.Build(funcType, obj);
        }
    }
}