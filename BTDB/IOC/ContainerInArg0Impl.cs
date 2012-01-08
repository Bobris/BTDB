using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC
{
    internal class ContainerInArg0Impl : ICRegILGen
    {
        internal static readonly ICRegILGen Instance = new ContainerInArg0Impl();

        ContainerInArg0Impl()
        {
        }

        public string GenFuncName(IGenerationContext context)
        {
            return "ContainerInArg0";
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
            context.IL.Ldarg(0);
            return null;
        }

        public IEnumerable<INeed> GetNeeds(IGenerationContext context)
        {
            yield break;
        }
    }
}