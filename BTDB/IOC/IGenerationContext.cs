using System.Collections.Generic;
using System.Reflection;
using BTDB.IL;

namespace BTDB.IOC
{
    internal interface IGenerationContext
    {
        IILGen IL { get; }
        ContainerImpl Container { get; }
        T GetSpecific<T>() where T : class, new();
        IEnumerable<INeed> NeedsForConstructor(ConstructorInfo constructor);
        void PushToILStack(ICRegILGen inCReg, INeed need);
        void PushToILStack(ICRegILGen inCReg, IEnumerable<INeed> needs);
        bool AnyCorruptingStack(ICRegILGen inCReg, IEnumerable<INeed> needs);
        ICRegILGen ResolveNeed(ICRegILGen inCReg, INeed need);
    }
}