using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.IL;

namespace BTDB.IOC.CRegs;

class EmptyEnumerableImpl : ICReg, ICRegILGen
{
    readonly Type _type;
    readonly Type _resultType;

    public EmptyEnumerableImpl(Type type, Type resultType)
    {
        _type = type;
        _resultType = resultType;
    }

    public string GenFuncName(IGenerationContext context)
    {
        return "EmptyEnumerable_" + _type.ToSimpleName();
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
        context.IL
            .Call(typeof(Enumerable).GetMethod("Empty").MakeGenericMethod(_resultType))
            .Castclass(_type);
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

    public void Verify(ContainerVerification options, ContainerImpl container)
    {
    }
}
