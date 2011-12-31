using System;
using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC.CRegs
{
    internal class FactoryImpl : ICReg, ICRegILGen
    {
        readonly Type _type;
        readonly int _instanceIndex;

        public FactoryImpl(ContainerImpl container, Func<ContainerImpl, object> buildFunc, Type type)
        {
            _type = type;
            _instanceIndex = container.AddInstance(buildFunc);
        }

        public FactoryImpl(int instanceIndex, Type type)
        {
            _instanceIndex = instanceIndex;
            _type = type;
        }

        public string GenFuncName
        {
            get { return "Factory_" + _type.ToSimpleName(); }
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

        public bool CorruptingILStack
        {
            get { return false; }
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