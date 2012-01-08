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
        void PushToILStack(INeed need);
        void PushToILStack(IEnumerable<INeed> needs);
        bool AnyCorruptingStack(IEnumerable<INeed> needs);
        ICRegILGen ResolveNeed(INeed need);
    }
}