using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.IL;

namespace BTDB.IOC.CRegs;

class AlwaysNewImpl : ICReg, ICRegILGen
{
    readonly Type _implementationType;
    readonly ConstructorInfo _constructorInfo;
    readonly bool _arePropertiesAutowired;

    internal AlwaysNewImpl(Type implementationType, ConstructorInfo constructorInfo, bool arePropertiesAutowired)
    {
        _implementationType = implementationType;
        _constructorInfo = constructorInfo;
        _arePropertiesAutowired = arePropertiesAutowired;
    }

    string ICRegILGen.GenFuncName(IGenerationContext context)
    {
        return "AlwaysNew_" + _implementationType.ToSimpleName();
    }

    public void GenInitialization(IGenerationContext context)
    {
        context.PushToCycleDetector(this, _implementationType.ToSimpleName());
        foreach (var regIlGen in GetNeeds(context).Where(context.IsResolvableNeed).Select(context.ResolveNeed))
        {
            regIlGen.GenInitialization(context);
        }

        context.PopFromCycleDetector();
    }

    public bool IsCorruptingILStack(IGenerationContext context)
    {
        return context.AnyCorruptingStack(GetNeeds(context));
    }

    public IILLocal? GenMain(IGenerationContext context)
    {
        var il = context.IL;
        var needsForProps = context.NeedsForProperties(_implementationType, _arePropertiesAutowired).ToList();
        if (needsForProps.Count > 0)
        {
            var result = il.DeclareLocal(_implementationType);
            context.PushToILStack(context.NeedsForConstructor(_constructorInfo));
            il.Newobj(_constructorInfo).Stloc(result);
            foreach (var need in needsForProps)
            {
                if (!context.IsResolvableNeed(need)) continue;
                var resolvedNeed = context.ResolveNeed(need);
                if (resolvedNeed.IsCorruptingILStack(context))
                {
                    var local = resolvedNeed.GenMain(context);
                    if (local == null)
                    {
                        local = il.DeclareLocal(need.ClrType);
                        il.Stloc(local);
                    }

                    il.Ldloc(result);
                    il.Ldloc(local);
                }
                else
                {
                    il.Ldloc(result);
                    var local = resolvedNeed.GenMain(context);
                    if (local != null)
                    {
                        il.Ldloc(local);
                    }
                }

                il.Call(need.PropertyInfo!.GetAnySetMethod()!);
            }

            return result;
        }

        context.PushToILStack(context.NeedsForConstructor(_constructorInfo));
        context.IL.Newobj(_constructorInfo);
        return null;
    }

    public IEnumerable<INeed> GetNeeds(IGenerationContext context)
    {
        return context.NeedsForConstructor(_constructorInfo).Concat(context.NeedsForProperties(_implementationType, _arePropertiesAutowired));
    }

    public bool IsSingletonSafe()
    {
        return false;
    }

    public void Verify(ContainerVerification options, ContainerImpl container)
    {
    }
}
