using System;
using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC.CRegs;

class DelegateImpl : ICReg, ICRegILGen
{
    readonly object? _key;
    readonly Type _type;
    readonly ICRegILGen _nestedRegistration;
    readonly Need _myNeed;

    public DelegateImpl(object? key, Type type, ICRegILGen nestedRegistration)
    {
        _key = key;
        _type = type;
        _nestedRegistration = nestedRegistration;
        _myNeed = new Need { Kind = NeedKind.Constant, ClrType = _type };
    }

    public string GenFuncName(IGenerationContext context)
    {
        return "Delegate_" + _type.ToSimpleName();
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
        context.PushToILStack(_myNeed);
        return null;
    }

    public IEnumerable<INeed> GetNeeds(IGenerationContext context)
    {
        if (_myNeed.Key == null)
        {
            var buildContext = context.BuildContext;
            var genCtx = new GenerationContext(context.Container, _nestedRegistration, buildContext, _type.GetMethod("Invoke").GetParameters());
            _myNeed.Key = genCtx.GenerateFunc(_type);
        }
        yield return _myNeed;
    }

    public bool IsSingletonSafe()
    {
        return true;
    }

    public void Verify(ContainerVerification options, ContainerImpl container)
    {
    }
}
