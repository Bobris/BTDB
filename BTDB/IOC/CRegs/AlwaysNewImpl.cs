using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.IL;

namespace BTDB.IOC.CRegs
{
    internal class AlwaysNewImpl : ICReg, ICRegILGen
    {
        readonly Type _implementationType;
        readonly ConstructorInfo _constructorInfo;

        internal AlwaysNewImpl(Type implementationType, ConstructorInfo constructorInfo)
        {
            _implementationType = implementationType;
            _constructorInfo = constructorInfo;
        }

        string ICRegILGen.GenFuncName(IGenerationContext context)
        {
            return "AlwaysNew_" + _implementationType.ToSimpleName();
        }

        public void GenInitialization(IGenerationContext context)
        {
            context.PushToCycleDetector(this,_implementationType.ToSimpleName());
            foreach (var regILGen in GetNeeds(context).Select(context.ResolveNeed))
            {
                regILGen.GenInitialization(context);
            }
            context.PopFromCycleDetector();
        }

        public bool IsCorruptingILStack(IGenerationContext context)
        {
            return context.AnyCorruptingStack(context.NeedsForConstructor(_constructorInfo));
        }

        public IILLocal GenMain(IGenerationContext context)
        {
            context.PushToILStack(context.NeedsForConstructor(_constructorInfo));
            context.IL.Newobj(_constructorInfo);
            return null;
        }

        public IEnumerable<INeed> GetNeeds(IGenerationContext context)
        {
            return context.NeedsForConstructor(_constructorInfo);
        }
    }
}