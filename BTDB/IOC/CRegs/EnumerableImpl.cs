using System;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.IL;

namespace BTDB.IOC.CRegs;

class EnumerableImpl : ICReg, ICRegILGen
{
    readonly object? _key;
    readonly Type _type;
    readonly Type _resultType;
    readonly List<KeyValuePair<IBuildContext, ICRegILGen>> _list = new List<KeyValuePair<IBuildContext, ICRegILGen>>();

    public EnumerableImpl(object? key, Type type, Type resultType, IBuildContext firstBuildCtx, ICRegILGen firstReg)
    {
        _key = key;
        _type = type;
        _resultType = resultType;
        _list.Add(new KeyValuePair<IBuildContext, ICRegILGen>(firstBuildCtx, firstReg));
    }

    public string GenFuncName(IGenerationContext context)
    {
        return "Enumerable_" + _type.ToSimpleName();
    }

    public void GenInitialization(IGenerationContext context)
    {
        var backupCtx = context.BuildContext;
        foreach (var pair in _list)
        {
            context.BuildContext = pair.Key;
            pair.Value.GenInitialization(context);
        }
        context.BuildContext = backupCtx;
    }

    public bool IsCorruptingILStack(IGenerationContext context)
    {
        var backupCtx = context.BuildContext;
        var result = false;
        foreach (var pair in _list)
        {
            context.BuildContext = pair.Key;
            result |= pair.Value.IsCorruptingILStack(context);
        }
        context.BuildContext = backupCtx;
        return result;
    }

    public IILLocal? GenMain(IGenerationContext context)
    {
        var il = context.IL;
        var resultLocal = il.DeclareLocal(_resultType.MakeArrayType());
        var itemLocal = il.DeclareLocal(_resultType);
        il
            .LdcI4(_list.Count)
            .Newarr(_resultType)
            .Stloc(resultLocal);
        var backupCtx = context.BuildContext;
        var idx = 0;
        foreach (var pair in _list)
        {
            context.BuildContext = pair.Key;
            var local = pair.Value.GenMain(context);
            if (local == null)
            {
                il.Stloc(itemLocal);
                local = itemLocal;
            }
            il.Ldloc(resultLocal).LdcI4(idx).Ldloc(local).StelemRef();
            idx++;
        }
        context.BuildContext = backupCtx;
        il
            .Ldloc(resultLocal)
            .Castclass(_type);
        return null;
    }

    public IEnumerable<INeed> GetNeeds(IGenerationContext context)
    {
        Trace.Assert(_list.Count == 1);
        var backupCtx = context.BuildContext;
        var nextCtx = _list[0].Key;
        context.BuildContext = nextCtx;
        yield return new Need { Kind = NeedKind.CReg, Key = _list[0].Value };
        nextCtx = nextCtx.IncrementEnumerable();
        while (nextCtx != null)
        {
            context.BuildContext = nextCtx;
            var reg = nextCtx.ResolveNeedBy(_resultType, _key);
            if (reg != null)
            {
                _list.Add(new KeyValuePair<IBuildContext, ICRegILGen>(nextCtx, reg));
                yield return new Need { Kind = NeedKind.CReg, Key = reg };
            }
            nextCtx = nextCtx.IncrementEnumerable();
        }
        context.BuildContext = backupCtx;
    }

    public bool IsSingletonSafe()
    {
        foreach (var (_, reg) in _list)
        {
            if (!reg.IsSingletonSafe()) return false;
        }

        return true;
    }

    public void Verify(ContainerVerification options, ContainerImpl container)
    {
    }
}
