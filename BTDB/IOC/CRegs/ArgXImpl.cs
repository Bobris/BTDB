using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC.CRegs;

class ArgXImpl : ICRegILGen
{
    internal static ICRegILGen GetInstance(ushort x)
    {
        return new ArgXImpl(x);
    }

    readonly ushort _x;

    ArgXImpl(ushort x)
    {
        _x = x;
    }

    public string GenFuncName(IGenerationContext context)
    {
        return "Arg" + _x;
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
        context.IL.Ldarg(_x);
        return null;
    }

    public IEnumerable<INeed> GetNeeds(IGenerationContext context)
    {
        yield break;
    }

    public bool IsSingletonSafe()
    {
        return true;
    }
}
