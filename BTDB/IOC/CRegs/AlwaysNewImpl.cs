using System;
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
            context.Container.CallInjectingInitializations(context, _constructorInfo);
        }

        public bool IsCorruptingILStack(IGenerationContext content)
        {
            return false;
        }

        public IILLocal GenMain(IGenerationContext context)
        {
            context.Container.CallInjectedConstructor(context, _constructorInfo);
            return null;
        }
    }
}