using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC;

interface ICRegILGen
{
    string GenFuncName(IGenerationContext context);
    void GenInitialization(IGenerationContext context);
    bool IsCorruptingILStack(IGenerationContext context);
    IILLocal? GenMain(IGenerationContext context);
    IEnumerable<INeed> GetNeeds(IGenerationContext context);
    bool IsSingletonSafe();
}
