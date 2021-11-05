using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC.CRegs;

class ContainerInjectImpl : ICReg, ICRegILGen
{
    public string GenFuncName(IGenerationContext context)
    {
        return "IContainer";
    }

    public void GenInitialization(IGenerationContext context)
    {
    }

    public bool IsCorruptingILStack(IGenerationContext context)
    {
        return false;
    }

    public IILLocal GenMain(IGenerationContext context)
    {
        context.PushToILStack(Need.ContainerNeed);
        return null;
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
