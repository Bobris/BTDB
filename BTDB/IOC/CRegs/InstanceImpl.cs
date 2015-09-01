using System;
using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC.CRegs
{
    class InstanceImpl : ICReg, ICRegILGen, ICRegFuncOptimized
    {
        readonly object _instance;
        readonly int _instanceIndex;

        public InstanceImpl(object instance, int instanceIndex)
        {
            _instance = instance;
            _instanceIndex = instanceIndex;
        }

        public object BuildFuncOfT(ContainerImpl container, Type funcType)
        {
            var obj = container.Instances[_instanceIndex];
            return ClosureOfObjBuilder.Build(funcType, obj);
        }

        string ICRegILGen.GenFuncName(IGenerationContext context)
        {
            return "Instance_" + _instance.GetType().ToSimpleName();
        }

        public void GenInitialization(IGenerationContext context)
        {
            if (_instance == null) return;
            context.GetSpecific<InstancesLocalGenCtxHelper>().Prepare();
        }

        public bool IsCorruptingILStack(IGenerationContext context)
        {
            return false;
        }

        public IILLocal GenMain(IGenerationContext context)
        {
            if (_instance == null)
            {
                context.IL.Ldnull();
                return null;
            }
            var buildCRegLocals = context.GetSpecific<SingletonImpl.BuildCRegLocals>();
            var localInstance = buildCRegLocals.Get(this);
            if (localInstance != null)
            {
                return localInstance;
            }
            var localInstances = context.GetSpecific<InstancesLocalGenCtxHelper>().MainLocal;
            localInstance = context.IL.DeclareLocal(_instance.GetType(), "instance");
            context.IL
                .Ldloc(localInstances)
                .LdcI4(_instanceIndex)
                .LdelemRef()
                .UnboxAny(_instance.GetType())
                .Stloc(localInstance);
            buildCRegLocals.Add(this, localInstance);
            return localInstance;
        }

        public IEnumerable<INeed> GetNeeds(IGenerationContext context)
        {
            yield return Need.ContainerNeed;
        }
    }
}