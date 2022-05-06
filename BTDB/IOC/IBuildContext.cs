using System;

namespace BTDB.IOC;

interface IBuildContext
{
    ICRegILGen? ResolveNeedBy(Type type, object? key);
    IBuildContext? IncrementEnumerable();
    IBuildContext FreezeMulti();
}
