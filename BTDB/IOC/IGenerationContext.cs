using System;
using System.Collections.Generic;
using System.Reflection;
using BTDB.IL;

namespace BTDB.IOC;

interface IGenerationContext
{
    IILGen IL { get; }
    ContainerImpl Container { get; }
    IBuildContext BuildContext { get; set; }
    T GetSpecific<T>() where T : class, new();
    IEnumerable<INeed> NeedsForConstructor(ConstructorInfo constructor);
    IEnumerable<INeed> NeedsForProperties(Type type, bool autowired);
    void PushToILStack(INeed need);
    void PushToILStack(IEnumerable<INeed> needs);
    bool AnyCorruptingStack(IEnumerable<INeed> needs);
    bool IsResolvableNeed(INeed need);
    ICRegILGen ResolveNeed(INeed need);
    void PushToCycleDetector(ICReg reg, string name);
    void PopFromCycleDetector();
}
