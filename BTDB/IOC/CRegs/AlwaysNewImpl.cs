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
            foreach (var regILGen in GetNeeds(context).Select(n => context.ResolveNeed(this, n)))
            {
                regILGen.GenInitialization(context);
            }
        }

        public bool IsCorruptingILStack(IGenerationContext context)
        {
            return context.AnyCorruptingStack(this, context.NeedsForConstructor(_constructorInfo));
        }

        public IILLocal GenMain(IGenerationContext context)
        {
            context.PushToILStack(this, context.NeedsForConstructor(_constructorInfo));
            context.IL.Newobj(_constructorInfo);
            return null;
        }

        public IEnumerable<INeed> GetNeeds(IGenerationContext context)
        {
            return context.NeedsForConstructor(_constructorInfo);
        }
    }
}