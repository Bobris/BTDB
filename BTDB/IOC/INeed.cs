using System;

namespace BTDB.IOC
{
    internal interface INeed
    {
        NeedKind Kind { get; }
        Type ClrType { get; }
        bool Optional { get; }
        bool ForcedKey { get; }
        object Key { get; }
        Type ParentType { get; }
    }
}