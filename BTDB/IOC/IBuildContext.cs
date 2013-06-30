using System;

namespace BTDB.IOC
{
    internal interface IBuildContext
    {
        ICRegILGen ResolveNeedBy(Type type, object key);
        IBuildContext IncrementEnumerable();
        IBuildContext FreezeMulti();
    }
}