using System;
using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC.CRegs;

class FactoryWithContainerParamImpl : ICReg, ICRegILGen
{
    readonly Type _type;
    readonly int _instanceIndex;

    public FactoryWithContainerParamImpl(int instanceIndex, Type type)
    {
        _instanceIndex = instanceIndex;
        _type = type;
    }

    string ICRegILGen.GenFuncName(IGenerationContext context)
    {
        return "FactoryWithContainerParam_" + _type.ToSimpleName();
    }

    public void GenInitialization(IGenerationContext context)
    {
        context.GetSpecific<InstancesLocalGenCtxHelper>().Prepare();
    }

    public bool IsCorruptingILStack(IGenerationContext context)
    {
        return false;
    }

    public IILLocal GenMain(IGenerationContext context)
    {
        var localInstances = context.GetSpecific<InstancesLocalGenCtxHelper>().MainLocal;
        var localInstance = context.IL.DeclareLocal(_type, "instance");
        context.IL
            .Ldloc(localInstances)
            .LdcI4(_instanceIndex)
            .LdelemRef()
            .Castclass(typeof(Func<ContainerImpl, object>));
        context.PushToILStack(Need.ContainerNeed);
        context.IL
            .Castclass(typeof(ContainerImpl))
            .Call(() => default(Func<ContainerImpl, object>).Invoke(null))
            .Castclass(_type)
            .Stloc(localInstance);
        return localInstance;
    }

    public IEnumerable<INeed> GetNeeds(IGenerationContext context)
    {
        yield return Need.ContainerNeed;
    }

    public bool IsSingletonSafe()
    {
        return true;
    }

    public void Verify(ContainerVerification options, ContainerImpl container)
    {
    }
}
