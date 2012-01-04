using BTDB.IL;

namespace BTDB.IOC
{
    internal interface ICRegILGen
    {
        string GenFuncName(IGenerationContext context);
        void GenInitialization(IGenerationContext context);
        bool IsCorruptingILStack(IGenerationContext content);
        IILLocal GenMain(IGenerationContext context);
    }
}